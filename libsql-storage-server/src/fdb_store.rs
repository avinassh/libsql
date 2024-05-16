use crate::store::FrameStore;
use bytes::Bytes;
use foundationdb::api::NetworkAutoStop;
use foundationdb::tuple::pack;
use foundationdb::tuple::unpack;
use foundationdb::Transaction;
use tracing::error;

pub struct FDBFrameStore {
    network: NetworkAutoStop,
}

impl FDBFrameStore {
    pub fn new() -> Self {
        let network = unsafe { foundationdb::boot() };
        Self { network }
    }

    async fn get_max_frame_no(&self, txn: &Transaction, namespace: &str) -> u64 {
        let namespace = "default";
        let max_frame_key = format!("{}/max_frame_no", namespace);
        let result = txn.get(&max_frame_key.as_bytes(), false).await;
        if let Err(e) = result {
            error!("get failed: {:?}", e);
            return 0;
        }
        if let Ok(None) = result {
            error!("page not found");
            return 0;
        }
        let frame_no: u64 = unpack(&result.unwrap().unwrap()).expect("failed to decode u64");
        tracing::info!("max_frame_no={}", frame_no);
        frame_no
    }
}

impl FrameStore for FDBFrameStore {
    async fn insert_frame(&mut self, page_no: u64, frame: Bytes) -> u64 {
        let namespace = "default";
        let max_frame_key = format!("{}/max_frame_no", namespace);
        let db = foundationdb::Database::default().unwrap();
        let txn = db.create_trx().expect("unable to create transaction");
        let frame_no = self.get_max_frame_no(&txn, namespace).await;

        let frame_key = format!("{}/f/{}/f", namespace, frame_no);
        let frame_page_key = format!("{}/f/{}/p", namespace, frame_no);
        let page_key = format!("{}/p/{}", namespace, page_no);

        txn.set(&frame_key.as_bytes(), &frame);
        txn.set(&frame_page_key.as_bytes(), &pack(&page_no));
        txn.set(&page_key.as_bytes(), &pack(&frame_no));
        txn.set(&max_frame_key.as_bytes(), &pack(&(frame_no + 1)));
        txn.commit().await.expect("commit failed");
        frame_no + 1
    }

    async fn read_frame(&self, frame_no: u64) -> Option<Bytes> {
        let namespace = "default";
        let frame_key = format!("{}/f/{}/f", namespace, frame_no);

        let db = foundationdb::Database::default().unwrap();
        let txn = db.create_trx().expect("unable to create transaction");
        let frame = txn.get(frame_key.as_bytes(), false).await;
        if let Ok(Some(data)) = frame {
            return Some(data.to_vec().into());
        }
        None
    }

    async fn find_frame(&self, page_no: u64) -> Option<u64> {
        let namespace = "default";
        let page_key = format!("{}/p/{}", namespace, page_no);

        let db = foundationdb::Database::default().unwrap();
        let txn = db.create_trx().expect("unable to create transaction");

        let result = txn.get(&page_key.as_bytes(), false).await;
        if let Err(e) = result {
            error!("get failed: {:?}", e);
            return None;
        }
        if let Ok(None) = result {
            error!("page not found");
            return None;
        }
        let frame_no: u64 = unpack(&result.unwrap().unwrap()).expect("failed to decode u64");
        // let frame_no: u64 = unpack(
        //     &txn.get(&page_key.as_bytes(), true)
        //         .await
        //         .expect("get failed")
        //         .expect("frame not found"),
        // )
        // .expect("failed to decode u64");
        Some(frame_no)
    }

    async fn frame_page_no(&self, frame_no: u64) -> Option<u64> {
        let namespace = "default";
        let frame_key = format!("{}/f/{}/p", namespace, frame_no);

        let db = foundationdb::Database::default().unwrap();
        let txn = db.create_trx().expect("unable to create transaction");
        let page_no: u64 = unpack(
            &txn.get(&frame_key.as_bytes(), true)
                .await
                .expect("get failed")
                .expect("frame not found"),
        )
        .expect("failed to decode u64");

        Some(page_no)
    }

    async fn frames_in_wal(&self) -> u64 {
        let namespace = "default";
        let db = foundationdb::Database::default().unwrap();
        let txn = db.create_trx().expect("unable to create transaction");
        self.get_max_frame_no(&txn, namespace).await
    }

    async fn destroy(&mut self) {}
}
