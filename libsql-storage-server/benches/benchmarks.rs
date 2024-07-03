use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use libsql::{params, Builder, Connection};
use rand::Rng;

async fn setup_db(url: String, token: String, size: u32) -> Connection {
    let db = Builder::new_remote(url, token).build().await.unwrap();
    let conn = db.connect().unwrap();
    teardown_db(&conn).await;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS data (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
        (),
    )
    .await
    .unwrap();
    let mut stmt = conn
        .prepare("INSERT INTO data (value) VALUES (?)")
        .await
        .unwrap();
    for _ in 0..size {
        stmt.execute(params!["value".to_string()]).await.unwrap();
    }
    conn
}

async fn teardown_db(conn: &Connection) {
    conn.execute("DROP TABLE IF EXISTS data", ()).await.unwrap();
}

fn random_read_benchmark(c: &mut Criterion) {
    let durable_wal_url = std::env::var("LIBSQL_DURABLE_URL").unwrap();
    let durable_wal_token = std::env::var("LIBSQL_DURABLE_AUTH_TOKEN").unwrap();
    let legacy_wal_url = std::env::var("LIBSQL_LEGACY_URL").unwrap();
    let legacy_wal_token = std::env::var("LIBSQL_LEGACY_AUTH_TOKEN").unwrap();

    let sizes: Vec<u32> = vec![100, 200]; //,1000, 10_000];
    let configs = vec![
        ("legacy", legacy_wal_url.clone(), legacy_wal_token.clone()),
        ("durable", legacy_wal_url, legacy_wal_token),
        // ("durable", durable_wal_url, durable_wal_token),
    ];
    let mut group = c.benchmark_group("Random Read");
    group.sample_size(10);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for size in &sizes {
        for config in &configs {
            group.bench_with_input(
                BenchmarkId::new(format!("random_read_{}", config.0), size),
                size,
                |b, &size| {
                    println!("setting up db: {}", size);
                    let conn = rt.block_on(setup_db(config.1.clone(), config.2.clone(), size));
                    println!("set up db");
                    b.to_async(&rt).iter(|| async {
                        let id = rand::thread_rng().gen_range(1..=size);
                        println!("got {}", id);
                        let mut stmt = conn
                            .prepare("SELECT value FROM data WHERE id = ?")
                            .await
                            .unwrap();
                        println!("prep done");
                        let mut rows = stmt.query([id]).await.unwrap();
                        println!("query done");
                        let _row = rows.next().await.unwrap().unwrap();
                        println!("row: {:?}", _row);
                        println!("tear down done");
                    });
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, random_read_benchmark);
criterion_main!(benches);
