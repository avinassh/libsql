use crate::store::FrameStore;
use bytes::Bytes;
use redis::{Client, Commands, RedisResult};
use tracing::error;

pub struct RedisFrameStore {
    client: Client,
}

impl RedisFrameStore {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

impl FrameStore for RedisFrameStore {
    fn insert_frame(&mut self, page_no: u64, frame: Bytes) -> u64 {
        let namespace = "default";
        let max_frame_key = format!("{}/max_frame_no", namespace);

        let mut con = self.client.get_connection().unwrap();
        // max_frame_key might change if another client inserts a frame, so do
        // all this in a transaction!
        let (max_frame_no,): (u64,) =
            redis::transaction(&mut con, &[&max_frame_key], |con, pipe| {
                let result: RedisResult<u64> = con.get(max_frame_key.clone());
                if result.is_err() && !crate::is_nil_response(result.as_ref().err().unwrap()) {
                    return Err(result.err().unwrap());
                }
                let max_frame_no = result.unwrap_or(0) + 1;
                let frame_key = format!("f/{}/{}", namespace, max_frame_no);
                let page_key = format!("p/{}/{}", namespace, page_no);

                pipe.hset::<String, &str, Vec<u8>>(frame_key.clone(), "f", frame.to_vec())
                    .ignore()
                    .hset::<String, &str, u64>(frame_key.clone(), "p", page_no)
                    .ignore()
                    .set::<String, u64>(page_key, max_frame_no)
                    .ignore()
                    .set::<String, u64>(max_frame_key.clone(), max_frame_no)
                    .ignore()
                    .get(max_frame_key.clone())
                    .query(con)
            })
            .unwrap();
        max_frame_no
    }

    fn read_frame(&self, frame_no: u64) -> Option<Bytes> {
        let namespace = "default";
        let frame_key = format!("f/{}/{}", namespace, frame_no);
        let mut con = self.client.get_connection().unwrap();
        let result = con.hget::<String, &str, Vec<u8>>(frame_key.clone(), "f");
        match result {
            Ok(frame) => Some(Bytes::from(frame)),
            Err(e) => {
                if !crate::is_nil_response(&e) {
                    error!(
                        "read_frame() failed for frame_no={} with err={}",
                        frame_no, e
                    );
                }
                None
            }
        }
    }

    fn find_frame(&self, page_no: u64) -> Option<u64> {
        let page_key = format!("p/{}/{}", "default", page_no);
        let mut con = self.client.get_connection().unwrap();
        let frame_no = con.get::<String, u64>(page_key.clone());
        match frame_no {
            Ok(frame_no) => Some(frame_no),
            Err(e) => {
                if !crate::is_nil_response(&e) {
                    error!("find_frame() failed for page_no={} with err={}", page_no, e);
                }
                None
            }
        }
    }

    fn frame_page_no(&self, frame_no: u64) -> Option<u64> {
        let namespace = "default";
        let frame_key = format!("f/{}/{}", namespace, frame_no);
        let mut con = self.client.get_connection().unwrap();
        let result = con.hget::<String, &str, u64>(frame_key.clone(), "p");
        match result {
            Ok(page_no) => Some(page_no),
            Err(e) => {
                if !crate::is_nil_response(&e) {
                    error!(
                        "frame_page_no() failed for frame_no={} with err={}",
                        frame_no, e
                    );
                }
                None
            }
        }
    }

    fn frames_in_wal(&self) -> u64 {
        let namespace = "default";
        let max_frame_key = format!("{}/max_frame_no", namespace);
        let mut con = self.client.get_connection().unwrap();
        let result = con.get::<String, u64>(max_frame_key.clone());
        result.unwrap_or_else(|e| {
            if !crate::is_nil_response(&e) {
                error!("frames_in_wal() failed with err={}", e);
            }
            0
        })
    }

    fn destroy(&mut self) {
        // remove all the keys in redis
        let mut con = self.client.get_connection().unwrap();
        // send a FLUSHALL request
        let _: () = redis::cmd("FLUSHALL").query(&mut con).unwrap();
    }
}
