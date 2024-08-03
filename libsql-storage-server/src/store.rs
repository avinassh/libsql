use crate::errors::Error;
use async_trait::async_trait;
use libsql_storage::rpc::Frame;
use tokio::sync::mpsc;

#[async_trait]
pub trait FrameStore: Send + Sync {
    async fn insert_frames(
        &self,
        namespace: &str,
        max_frame_no: u64,
        frames: Vec<Frame>,
    ) -> Result<u64, Error>;
    async fn read_frame(&self, namespace: &str, frame_no: u64) -> Option<bytes::Bytes>;
    async fn find_frame(&self, namespace: &str, page_no: u32, max_frame_no: u64) -> Option<u64>;
    async fn get_frame_by_page(
        &self,
        namespace: &str,
        page_no: u32,
        max_frame_no: u64,
    ) -> Option<(u64, bytes::Bytes)>;
    async fn frames_in_wal(&self, namespace: &str) -> u64;
    async fn destroy(&self, namespace: &str);
    async fn streaming_query(
        &self,
        namespace: &str,
        start_page: u32,
    ) -> mpsc::Receiver<Option<Vec<(u32, u64)>>>;
}
