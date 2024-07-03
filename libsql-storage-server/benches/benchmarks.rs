use criterion::async_executor::FuturesExecutor;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use libsql::{params, Builder, Connection};
use rand::Rng;

async fn setup_db(url: String, token: String, size: usize) -> Connection {
    let db = Builder::new_remote(url, token).build().await.unwrap();
    let conn = db.connect().unwrap();
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

    let sizes = vec![100, 1000, 100_000, 1_000_000];
    let configs = vec![
        ("legacy", legacy_wal_url, legacy_wal_token),
        ("durable", durable_wal_url, durable_wal_token),
    ];

    let mut group = c.benchmark_group("Random Read");
    for size in &sizes {
        for config in &configs {
            group.bench_with_input(
                BenchmarkId::new(format!("random_read_{}", config.0), size),
                size,
                |b, &size| {
                    b.to_async(FuturesExecutor).iter_batched(
                        || async move {
                            let conn = setup_db(config.1.clone(), config.2.clone(), size).await;
                            let id = rand::thread_rng().gen_range(1..=size);
                            (conn, id)
                        },
                        |tuple| async move {
                            let (conn, id) = tuple;
                            conn.call(move |conn| {
                                conn.query_row(
                                    "SELECT value FROM data WHERE id = ?",
                                    params![id],
                                    |row| row.get::<_, String>(0),
                                )
                            })
                            .await
                            .unwrap();
                            teardown_db(&conn).await;
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, random_read_benchmark);
criterion_main!(benches);
