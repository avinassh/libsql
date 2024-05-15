use crate::store::FrameStore;
use crate::FrameData;
use bytes::Bytes;
use std::collections::BTreeMap;

#[derive(Default)]
struct InMemFrameStore {
    // contains a frame data, key is the frame number
    frames: BTreeMap<u64, FrameData>,
    // pages map contains the page number as a key and the list of frames for the page as a value
    pages: BTreeMap<u64, Vec<u64>>,
    max_frame_no: u64,
}

impl InMemFrameStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl FrameStore for InMemFrameStore {
    // inserts a new frame for the page number and returns the new frame value
    fn insert_frame(&mut self, page_no: u64, frame: Bytes) -> u64 {
        let frame_no = self.max_frame_no + 1;
        self.max_frame_no = frame_no;
        self.frames.insert(
            frame_no,
            FrameData {
                page_no,
                data: frame,
            },
        );
        self.pages
            .entry(page_no)
            .or_insert_with(Vec::new)
            .push(frame_no);
        frame_no
    }

    fn read_frame(&self, frame_no: u64) -> Option<bytes::Bytes> {
        self.frames.get(&frame_no).map(|frame| frame.data.clone())
    }

    // given a page number, return the maximum frame for the page
    fn find_frame(&self, page_no: u64) -> Option<u64> {
        self.pages
            .get(&page_no)
            .map(|frames| *frames.last().unwrap())
    }

    // given a frame num, return the page number
    fn frame_page_no(&self, frame_no: u64) -> Option<u64> {
        self.frames.get(&frame_no).map(|frame| frame.page_no)
    }

    fn frames_in_wal(&self) -> u64 {
        self.max_frame_no
    }

    fn destroy(&mut self) {
        self.frames.clear();
        self.pages.clear();
        self.max_frame_no = 0;
    }
}
