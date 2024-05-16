pub trait FrameStore {
    async fn insert_frame(&mut self, page_no: u64, frame: bytes::Bytes) -> u64;
    async fn read_frame(&self, frame_no: u64) -> Option<bytes::Bytes>;
    async fn find_frame(&self, page_no: u64) -> Option<u64>;
    async fn frame_page_no(&self, frame_no: u64) -> Option<u64>;
    async fn frames_in_wal(&self) -> u64;
    async fn destroy(&mut self);
}
