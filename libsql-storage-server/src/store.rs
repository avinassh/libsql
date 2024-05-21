use crate::FrameData;

pub trait FrameStore {
    async fn insert_frame(&mut self, namespace: &str, page_no: u64, frame: bytes::Bytes) -> u64;
    async fn insert_frames(&mut self, namespace: &str, frames: Vec<FrameData>) -> u64;
    async fn read_frame(&self, namespace: &str, frame_no: u64) -> Option<bytes::Bytes>;
    async fn find_frame(&self, namespace: &str, page_no: u64) -> Option<u64>;
    async fn frame_page_no(&self, namespace: &str, frame_no: u64) -> Option<u64>;
    async fn frames_in_wal(&self, namespace: &str) -> u64;
    async fn destroy(&mut self, namespace: &str);
}
