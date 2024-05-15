mod memory_store;
mod redis_store;
mod store;

use crate::store::FrameStore;
use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use libsql_storage::rpc::storage_server::{Storage, StorageServer};
use libsql_storage::rpc::{
    DbSizeReq, DbSizeResp, DestroyReq, DestroyResp, FindFrameReq, FindFrameResp, FramePageNumReq,
    FramePageNumResp, FramesInWalReq, FramesInWalResp, InsertFramesReq, InsertFramesResp,
    ReadFrameReq, ReadFrameResp,
};
use libsql_storage_server::version::Version;
use redis::{Client, Commands, RedisResult};
use redis_store::RedisFrameStore;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{error, trace};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

/// libSQL storage server
#[derive(Debug, Parser)]
#[command(name = "libsql-storage-server")]
#[command(about = "libSQL storage server", version = Version::default(), long_about = None)]
struct Cli {
    /// The address and port the storage RPC protocol listens to. Example: `127.0.0.1:5002`.
    #[clap(long, env = "LIBSQL_STORAGE_LISTEN_ADDR", default_value = "[::]:5002")]
    listen_addr: SocketAddr,
}

#[derive(Default)]
struct FrameData {
    page_no: u64,
    data: bytes::Bytes,
}

struct Service {
    store: Arc<Mutex<RedisFrameStore>>,
    db_size: AtomicU32,
}

impl Service {
    pub fn new(client: Client) -> Self {
        Self {
            store: Arc::new(Mutex::new(RedisFrameStore::new(client))),
            db_size: AtomicU32::new(0),
        }
    }
}

#[tonic::async_trait]
impl Storage for Service {
    async fn insert_frames(
        &self,
        request: tonic::Request<InsertFramesReq>,
    ) -> Result<tonic::Response<InsertFramesResp>, tonic::Status> {
        trace!("insert_frames()");
        let mut num_frames = 0;
        let mut store = self.store.lock().unwrap();
        trace!("insert_frames() got lock");
        let frames = request
            .into_inner()
            .frames
            .into_iter()
            .map(|frame| FrameData {
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
                store.insert_frame(frame.page_no, frame.data.into())
            );
            num_frames += 1;
            self.db_size
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(Response::new(InsertFramesResp { num_frames }))
    }

    async fn find_frame(
        &self,
        request: tonic::Request<FindFrameReq>,
    ) -> Result<tonic::Response<FindFrameResp>, tonic::Status> {
        let page_no = request.into_inner().page_no;
        trace!("find_frame(page_no={})", page_no);
        if let Some(frame_no) = self.store.lock().unwrap().find_frame(page_no) {
            Ok(Response::new(FindFrameResp {
                frame_no: Some(frame_no),
            }))
        } else {
            error!("find_frame() failed for page_no={}", page_no);
            Ok(Response::new(FindFrameResp { frame_no: None }))
        }
    }

    async fn read_frame(
        &self,
        request: tonic::Request<ReadFrameReq>,
    ) -> Result<tonic::Response<ReadFrameResp>, tonic::Status> {
        let frame_no = request.into_inner().frame_no;
        trace!("read_frame(frame_no={})", frame_no);
        if let Some(data) = self.store.lock().unwrap().read_frame(frame_no) {
            Ok(Response::new(ReadFrameResp {
                frame: Some(data.clone().into()),
            }))
        } else {
            error!("read_frame() failed for frame_no={}", frame_no);
            Ok(Response::new(ReadFrameResp { frame: None }))
        }
    }

    async fn destroy(
        &self,
        request: tonic::Request<DestroyReq>,
    ) -> Result<tonic::Response<DestroyResp>, tonic::Status> {
        trace!("destroy()");
        self.store.lock().unwrap().destroy();
        Ok(Response::new(DestroyResp {}))
    }

    async fn db_size(
        &self,
        request: tonic::Request<DbSizeReq>,
    ) -> Result<tonic::Response<DbSizeResp>, tonic::Status> {
        let size = self.db_size.load(std::sync::atomic::Ordering::SeqCst) as u64;
        Ok(Response::new(DbSizeResp { size }))
    }

    async fn frames_in_wal(
        &self,
        request: Request<FramesInWalReq>,
    ) -> std::result::Result<Response<FramesInWalResp>, Status> {
        Ok(Response::new(FramesInWalResp {
            count: self.store.lock().unwrap().frames_in_wal() as u32,
        }))
    }

    async fn frame_page_num(
        &self,
        request: Request<FramePageNumReq>,
    ) -> std::result::Result<Response<FramePageNumResp>, Status> {
        let frame_no = request.into_inner().frame_no;
        if let Some(page_no) = self.store.lock().unwrap().frame_page_no(frame_no) {
            Ok(Response::new(FramePageNumResp { page_no }))
        } else {
            error!("frame_page_num() failed for frame_no={}", frame_no);
            Ok(Response::new(FramePageNumResp { page_no: 0 }))
        }
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
    let service = Service::new(client);

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
