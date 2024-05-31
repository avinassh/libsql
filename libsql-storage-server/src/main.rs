use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use libsql_storage::rpc::storage_server::{Storage, StorageServer};
use libsql_storage::rpc::{
    DbSizeRequest, DbSizeResponse, DestroyRequest, DestroyResponse, FindFrameRequest,
    FindFrameResponse, FramePageNumRequest, FramePageNumResponse, FramesInWalRequest,
    FramesInWalResponse, InsertFramesRequest, InsertFramesResponse, ReadFrameRequest,
    ReadFrameResponse,
};
use libsql_storage_server::version::Version;
use redis::{Client, Commands, RedisResult};
use serde;
use service::Service;
use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{error, trace};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

use crate::fdb_store::FDBFrameStore;
use crate::memory_store::InMemFrameStore;
use crate::redis_store::RedisFrameStore;
use crate::store::FrameStore;

mod fdb_store;
mod memory_store;
mod redis_store;
mod service;
mod store;

#[derive(clap::ValueEnum, Clone, Debug)]
enum StorageType {
    InMemory,
    Redis,
    FoundationDB,
}

#[derive(Debug, Parser)]
#[command(name = "libsql-storage-server")]
#[command(about = "libSQL storage server", version = Version::default(), long_about = None)]
struct Cli {
    /// The address and port the storage RPC protocol listens to. Example: `127.0.0.1:5002`.
    #[clap(long, env = "LIBSQL_STORAGE_LISTEN_ADDR", default_value = "[::]:5002")]
    listen_addr: SocketAddr,

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
    // export REDIS_ADDR=http://libsql-storage-server.internal:5002
    let redis_addr = std::env::var("REDIS_ADDR").unwrap_or("redis://127.0.0.1/".to_string());
    let client = Client::open(redis_addr).unwrap();
    let service = Service::with_store(Arc::new(Mutex::new(FDBFrameStore::new())));
    trace!(
        "(trace) Starting libSQL storage server on {}",
        args.listen_addr
    );
    Server::builder()
        .add_service(StorageServer::new(service))
        .serve(args.listen_addr)
        .await?;

    Ok(())
}
