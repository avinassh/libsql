use crate::errors::Error::WriteConflict;
use crate::memory_store::InMemFrameStore;
use crate::store::FrameStore;
use futures::stream;
use libsql_storage::rpc;
use libsql_storage::rpc::storage_server::Storage;
use std::pin::Pin;
use std::sync::atomic::AtomicU32;
use tonic::codegen::tokio_stream::Stream;
use tonic::{Request, Response, Status};
use tracing::{error, trace};

pub struct Service {
    store: Box<dyn FrameStore + Send + Sync>,
    db_size: AtomicU32,
}

impl Service {
    pub fn new() -> Self {
        Self {
            store: Box::new(InMemFrameStore::new()),
            db_size: AtomicU32::new(0),
        }
    }

    pub fn with_store(store: Box<dyn FrameStore + Send + Sync>) -> Self {
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
        request: Request<rpc::InsertFramesRequest>,
    ) -> Result<Response<rpc::InsertFramesResponse>, Status> {
        let request = request.into_inner();
        let ns = request.namespace;
        let frames = request.frames;
        let max_frame_no = request.max_frame_no;
        let num_frames = self.store.insert_frames(&ns, max_frame_no, frames).await? as u32;
        Ok(Response::new(rpc::InsertFramesResponse { num_frames }))
    }

    async fn find_frame(
        &self,
        request: Request<rpc::FindFrameRequest>,
    ) -> Result<Response<rpc::FindFrameResponse>, Status> {
        let request = request.into_inner();
        let page_no = request.page_no;
        let namespace = request.namespace;
        trace!("find_frame(page_no={})", page_no);
        if let Some(frame_no) = self
            .store
            .find_frame(&namespace, page_no, request.max_frame_no)
            .await
        {
            Ok(Response::new(rpc::FindFrameResponse {
                frame_no: Some(frame_no),
            }))
        } else {
            error!("find_frame() failed for page_no={}", page_no);
            Ok(Response::new(rpc::FindFrameResponse { frame_no: None }))
        }
    }

    async fn read_frame(
        &self,
        request: Request<rpc::ReadFrameRequest>,
    ) -> Result<Response<rpc::ReadFrameResponse>, Status> {
        let request = request.into_inner();
        let frame_no = request.frame_no;
        let namespace = request.namespace;
        trace!("read_frame(frame_no={})", frame_no);
        if let Some(data) = self.store.read_frame(&namespace, frame_no).await {
            Ok(Response::new(rpc::ReadFrameResponse {
                frame: Some(data.clone().into()),
            }))
        } else {
            error!("read_frame() failed for frame_no={}", frame_no);
            Ok(Response::new(rpc::ReadFrameResponse { frame: None }))
        }
    }

    async fn get_frame_by_page(
        &self,
        request: Request<rpc::GetFrameByPageRequest>,
    ) -> Result<Response<rpc::GetFrameByPageResponse>, Status> {
        let request = request.into_inner();
        let page_no = request.page_no;
        let namespace = request.namespace;
        trace!("get_frame_by_page(page_no={})", page_no);
        if let Some((frame_no, data)) = self
            .store
            .get_frame_by_page(&namespace, page_no, request.max_frame_no)
            .await
        {
            Ok(Response::new(rpc::GetFrameByPageResponse {
                frame: Some(data.clone().into()),
                frame_no: Some(frame_no),
            }))
        } else {
            Ok(Response::new(rpc::GetFrameByPageResponse {
                frame: None,
                frame_no: None,
            }))
        }
    }

    async fn db_size(
        &self,
        _request: Request<rpc::DbSizeRequest>,
    ) -> Result<Response<rpc::DbSizeResponse>, Status> {
        let size = self.db_size.load(std::sync::atomic::Ordering::SeqCst) as u64;
        Ok(Response::new(rpc::DbSizeResponse { size }))
    }

    async fn frames_in_wal(
        &self,
        request: Request<rpc::FramesInWalRequest>,
    ) -> Result<Response<rpc::FramesInWalResponse>, Status> {
        let namespace = request.into_inner().namespace;
        Ok(Response::new(rpc::FramesInWalResponse {
            count: self.store.frames_in_wal(&namespace).await,
        }))
    }

    async fn destroy(
        &self,
        request: Request<rpc::DestroyRequest>,
    ) -> Result<Response<rpc::DestroyResponse>, Status> {
        trace!("destroy()");
        let namespace = request.into_inner().namespace;
        self.store.destroy(&namespace).await;
        Ok(Response::new(rpc::DestroyResponse {}))
    }

    type StreamVersionMapStream =
        Pin<Box<dyn Stream<Item = Result<rpc::StreamVersionMapResponse, Status>> + Send>>;

    async fn stream_version_map(
        &self,
        request: Request<rpc::StreamVersionMapRequest>,
    ) -> Result<Response<Self::StreamVersionMapStream>, Status> {
        let max_batch = 10_000;
        let request = request.into_inner();
        let namespace = request.namespace;
        trace!("stream_version_map(namespace={})", namespace);
        let max_frame_no = self.store.frames_in_wal(&namespace).await;
        if request.frame_no > 0 && request.frame_no + max_batch < max_frame_no {
            return Err(WriteConflict.into());
        }

        let receiver = self
            .store
            .streaming_query(&namespace, request.frame_no)
            .await;
        let stream = stream::unfold(
            (receiver, max_frame_no),
            |(mut rx, max_frame_no)| async move {
                match rx.recv().await {
                    Some(Some(vec)) => {
                        let versions: Vec<rpc::Version> = vec
                            .into_iter()
                            .map(|(page_no, frame_no)| rpc::Version { frame_no, page_no })
                            .collect();
                        let response = rpc::StreamVersionMapResponse {
                            max_frame_no,
                            version: versions,
                        };
                        Some((Ok(response), (rx, max_frame_no)))
                    }
                    Some(None) | None => None,
                }
            },
        );

        Ok(Response::new(
            Box::pin(stream) as Self::StreamVersionMapStream
        ))
    }
}
