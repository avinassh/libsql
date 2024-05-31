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

#[derive(Default)]
struct FrameData {
    page_no: u64,
    data: Bytes,
}

struct Service {
    store: Arc<Mutex<dyn FrameStore + Send + Sync>>,
    db_size: AtomicU32,
}

impl Service {
    pub fn new() -> Self {
        Self {
            store: Arc::new(Mutex::new(InMemFrameStore::new())),
            db_size: AtomicU32::new(0),
        }
    }
    pub fn with_store(store: Arc<Mutex<dyn FrameStore + Send + Sync>>) -> Self {
        Self {
            store,
            db_size: AtomicU32::new(0),
        }
    }
}

#[tonic::async_trait]
impl Storage for Service {
    async fn insert_frames(
        &self,
        request: tonic::Request<InsertFramesRequest>,
    ) -> Result<tonic::Response<InsertFramesResponse>, tonic::Status> {
        trace!("insert_frames()");
        let mut num_frames = 0;
        let mut store = self.store.lock().await;
        trace!("insert_frames() got lock");
        let request = request.into_inner();
        let namespace = request.namespace;
        let frames = request.frames.into_iter().map(|frame| FrameData {
            page_no: frame.page_no,
            data: frame.data.into(),
        });
        let all_data: Vec<u8> = frames
            .clone()
            .map(|f| f.data.clone().to_vec())
            .flatten()
            .collect();
        trace!("insert_frames() got frames (bytes): {:?}", all_data.len());
        trace!("insert_frames() got frames: {:?}", frames.len());
        for frame in frames {
            trace!(
                "inserted for page {} frame {}",
                frame.page_no,
                store
                    .insert_frame(&namespace, frame.page_no, frame.data.into())
                    .await
            );
            num_frames += 1;
            self.db_size
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(Response::new(InsertFramesResponse { num_frames }))
    }

    async fn find_frame(
        &self,
        request: tonic::Request<FindFrameRequest>,
    ) -> Result<tonic::Response<FindFrameResponse>, tonic::Status> {
        let request = request.into_inner();
        let page_no = request.page_no;
        let namespace = request.namespace;
        trace!("find_frame(page_no={})", page_no);
        if let Some(frame_no) = self
            .store
            .lock()
            .await
            .find_frame(&namespace, page_no)
            .await
        {
            Ok(Response::new(FindFrameResponse {
                frame_no: Some(frame_no),
            }))
        } else {
            error!("find_frame() failed for page_no={}", page_no);
            Ok(Response::new(FindFrameResponse { frame_no: None }))
        }
    }

    async fn read_frame(
        &self,
        request: tonic::Request<ReadFrameRequest>,
    ) -> Result<tonic::Response<ReadFrameResponse>, tonic::Status> {
        let request = request.into_inner();
        let frame_no = request.frame_no;
        let namespace = request.namespace;
        trace!("read_frame(frame_no={})", frame_no);
        if let Some(data) = self
            .store
            .lock()
            .await
            .read_frame(&namespace, frame_no)
            .await
        {
            Ok(Response::new(ReadFrameResponse {
                frame: Some(data.clone().into()),
            }))
        } else {
            error!("read_frame() failed for frame_no={}", frame_no);
            Ok(Response::new(ReadFrameResponse { frame: None }))
        }
    }

    async fn db_size(
        &self,
        request: tonic::Request<DbSizeRequest>,
    ) -> Result<tonic::Response<DbSizeResponse>, tonic::Status> {
        let size = self.db_size.load(std::sync::atomic::Ordering::SeqCst) as u64;
        Ok(Response::new(DbSizeResponse { size }))
    }

    async fn frames_in_wal(
        &self,
        request: Request<FramesInWalRequest>,
    ) -> std::result::Result<Response<FramesInWalResponse>, Status> {
        let namespace = request.into_inner().namespace;
        Ok(Response::new(FramesInWalResponse {
            count: self.store.lock().await.frames_in_wal(&namespace).await,
        }))
    }

    async fn frame_page_num(
        &self,
        request: Request<FramePageNumRequest>,
    ) -> std::result::Result<Response<FramePageNumResponse>, Status> {
        let request = request.into_inner();
        let frame_no = request.frame_no;
        let namespace = request.namespace;
        if let Some(page_no) = self
            .store
            .lock()
            .await
            .frame_page_no(&namespace, frame_no)
            .await
        {
            Ok(Response::new(FramePageNumResponse { page_no }))
        } else {
            error!("frame_page_num() failed for frame_no={}", frame_no);
            Ok(Response::new(FramePageNumResponse { page_no: 0 }))
        }
    }

    async fn destroy(
        &self,
        request: tonic::Request<DestroyRequest>,
    ) -> Result<tonic::Response<DestroyResponse>, tonic::Status> {
        trace!("destroy()");
        let namespace = request.into_inner().namespace;
        self.store.lock().await.destroy(&namespace).await;
        Ok(Response::new(DestroyResponse {}))
    }
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
    Arc::new(Mutex::new(RedisFrameStore::new(client)));
    let service = Service::with_store(Arc::new(Mutex::new(FDBFrameStore::new())));
    println!("Starting libSQL storage server on {}", args.listen_addr);
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

fn is_nil_response(e: &redis::RedisError) -> bool {
    e.to_string().contains("response was nil")
}
