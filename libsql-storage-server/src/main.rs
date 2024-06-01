mod fdb_store;
mod memory_store;
mod redis_store;
mod service;
mod store;

use std::net::SocketAddr;

use crate::fdb_store::FDBFrameStore;
use crate::redis_store::RedisFrameStore;
use anyhow::Result;
use clap::Parser;
use libsql_storage::rpc::storage_server::StorageServer;
use libsql_storage_server::version::Version;
use redis::Client;
use service::Service;
use tonic::transport::Server;
use tracing::trace;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[derive(clap::ValueEnum, Clone, Debug)]
enum StorageType {
    InMemory,
    Redis,
    FoundationDB,
}

#[derive(Debug, Parser)]
#[command(name = "libsql-storage-server")]
#[command(about = "libSQL Storage Server", version = Version::default(), long_about = None)]
struct Cli {
    /// The address and port the storage RPC protocol listens to. Example: `127.0.0.1:5002`.
    #[clap(long, env = "LIBSQL_STORAGE_LISTEN_ADDR", default_value = "[::]:5002")]
    listen_addr: SocketAddr,

    /// The type of storage backend to use. Example: `redis`
    #[clap(value_enum, long, default_value = "in-memory")]
    storage_type: StorageType,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("libsql_storage_server=trace"));
    tracing::subscriber::set_global_default(
        FmtSubscriber::builder().with_env_filter(filter).finish(),
    )
    .expect("setting default subscriber failed");

    let args = Cli::parse();
    let service = match args.storage_type {
        StorageType::Redis => {
            // export REDIS_ADDR=http://libsql-storage-server.internal:5002
            let redis_addr =
                std::env::var("REDIS_ADDR").unwrap_or("redis://127.0.0.1/".to_string());
            let client = Client::open(redis_addr).unwrap();
            Service::with_store(Box::new(RedisFrameStore::new(client)))
        }
        StorageType::FoundationDB => Service::with_store(Box::new(FDBFrameStore::new())),
        _ => Service::new(),
    };

    trace!(
        "Starting libSQL storage server (with type {:?}) on {}",
        args.storage_type,
        args.listen_addr
    );
    Server::builder()
        .add_service(StorageServer::new(service))
        .serve(args.listen_addr)
        .await?;
    Ok(())
}
