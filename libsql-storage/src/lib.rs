use libsql_sys::ffi::SQLITE_BUSY;
use libsql_sys::rusqlite;
use libsql_sys::wal::{Result, Vfs, Wal, WalManager};
use sieve_cache::SieveCache;
use std::collections::BTreeMap;
use std::mem::size_of;
use std::sync::{Arc, Mutex};
use tonic::transport::Channel;
use tracing::trace;
use uuid::uuid;

pub mod rpc {
    #![allow(clippy::all)]
    include!("generated/storage.rs");
}

use rpc::storage_client::StorageClient;

#[derive(Clone)]
pub struct DurableWalManager {
    lock_manager: Arc<Mutex<LockManager>>,
}

impl DurableWalManager {
    pub fn new(lock_manager: Arc<Mutex<LockManager>>) -> Self {
        Self { lock_manager }
    }
}

impl WalManager for DurableWalManager {
    type Wal = DurableWal;

    fn use_shared_memory(&self) -> bool {
        trace!("DurableWalManager::use_shared_memory()");
        false
    }

    fn open(
        &self,
        vfs: &mut Vfs,
        file: &mut libsql_sys::wal::Sqlite3File,
        no_shm_mode: std::ffi::c_int,
        max_log_size: i64,
        db_path: &std::ffi::CStr,
    ) -> Result<Self::Wal> {
        let db_path = db_path.to_str().unwrap();
        trace!("DurableWalManager::open(db_path: {})", db_path);
        Ok(DurableWal::new(self.lock_manager.clone()))
    }

    fn close(
        &self,
        wal: &mut Self::Wal,
        db: &mut libsql_sys::wal::Sqlite3Db,
        sync_flags: std::ffi::c_int,
        scratch: Option<&mut [u8]>,
    ) -> Result<()> {
        trace!("DurableWalManager::close()");
        Ok(())
    }

    fn destroy_log(&self, vfs: &mut Vfs, db_path: &std::ffi::CStr) -> Result<()> {
        trace!("DurableWalManager::destroy_log()");
        // let address = std::env::var("LIBSQL_STORAGE_SERVER_ADDR")
        //     .unwrap_or("http://127.0.0.1:5002".to_string());
        // let client = StorageClient::connect(address);
        // let rt = tokio::runtime::Runtime::new().unwrap();
        // let mut client = tokio::task::block_in_place(|| rt.block_on(client)).unwrap();
        // let req = rpc::DestroyReq {};
        // let resp = client.destroy(req);
        // let resp = tokio::task::block_in_place(|| rt.block_on(resp)).unwrap();
        Ok(())
    }

    fn log_exists(&self, vfs: &mut Vfs, db_path: &std::ffi::CStr) -> Result<bool> {
        trace!("DurableWalManager::log_exists()");
        // TODO: implement
        Ok(true)
    }

    fn destroy(self)
    where
        Self: Sized,
    {
        trace!("DurableWalManager::destroy()");
    }
}

pub struct DurableWal {
    client: parking_lot::Mutex<StorageClient<Channel>>,
    page_frames: SieveCache<std::num::NonZeroU32, Vec<u8>>,
    db_size: u32,
    name: String,
    lock_manager: Arc<Mutex<LockManager>>,
    runtime: Option<tokio::runtime::Runtime>,
    rt: tokio::runtime::Handle,
    write_cache: BTreeMap<u32, rpc::Frame>,
}

impl DurableWal {
    fn new(lock_manager: Arc<Mutex<LockManager>>) -> Self {
        let (runtime, rt) = match tokio::runtime::Handle::try_current() {
            Ok(h) => (None, h),
            Err(_) => {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let handle = rt.handle().clone();
                (Some(rt), handle)
            }
        };
        // connect to external storage server
        // export LIBSQL_STORAGE_SERVER_ADDR=http://libsql-storage-server.internal:5002
        let address = std::env::var("LIBSQL_STORAGE_SERVER_ADDR")
            .unwrap_or("http://127.0.0.1:5002".to_string());
        trace!("DurableWal::new() address = {}", address);

        // let channel = Channel::from_static(address).connect_lazy().unwrap();
        // let channel = Channel::builder("127.0.0.1:3000".parse().unwrap()).connect_lazy();

        let client = StorageClient::connect(address);
        // let client = client.max_encoding_message_size(100);
        // let client = StorageClient::new(channel)
        //     .max_encoding_message_size(100)
        //     .unwrap();

        let mut client = tokio::task::block_in_place(|| rt.block_on(client)).unwrap();

        let req = rpc::DbSizeReq {};
        let resp = client.db_size(req);
        let resp = tokio::task::block_in_place(|| rt.block_on(resp)).unwrap();
        let db_size = resp.into_inner().size as u32;

        let page_frames = SieveCache::new(1000).unwrap();

        Self {
            client: parking_lot::Mutex::new(client),
            page_frames,
            db_size,
            name: uuid::Uuid::new_v4().to_string(),
            lock_manager,
            runtime,
            rt,
            write_cache: BTreeMap::new(),
        }
    }

    fn find_frame_by_page_no(
        &mut self,
        page_no: std::num::NonZeroU32,
    ) -> Result<Option<std::num::NonZeroU32>> {
        trace!("DurableWal::find_frame_by_page_no(page_no: {:?})", page_no);
        // TODO: send max frame number in the request
        let req = rpc::FindFrameReq {
            page_no: page_no.get() as u64,
        };
        let mut binding = self.client.lock();
        let resp = binding.find_frame(req);
        let resp = tokio::task::block_in_place(|| self.rt.block_on(resp)).unwrap();
        let frame_no = resp
            .into_inner()
            .frame_no
            .map(|page_no| std::num::NonZeroU32::new(page_no as u32))
            .flatten();
        Ok(frame_no)
    }

    fn frames_count(&self) -> u32 {
        let req = rpc::FramesInWalReq {};
        let mut binding = self.client.lock();
        let resp = binding.frames_in_wal(req);
        let resp = tokio::task::block_in_place(|| self.rt.block_on(resp)).unwrap();
        let count = resp.into_inner().count;
        trace!("DurableWal::frames_in_wal() = {}", count);
        count
    }
}

impl Wal for DurableWal {
    fn limit(&mut self, size: i64) {
        // no op, we go bottomless baby!
    }

    fn begin_read_txn(&mut self) -> Result<bool> {
        trace!("DurableWal::begin_read_txn()");
        // TODO: give a read lock for this conn
        // note down max frame number
        Ok(true)
    }

    fn end_read_txn(&mut self) {
        trace!("DurableWal::end_read_txn()");
        // TODO: drop both read or write lock
    }

    fn find_frame(
        &mut self,
        page_no: std::num::NonZeroU32,
    ) -> Result<Option<std::num::NonZeroU32>> {
        trace!("DurableWal::find_frame(page_no: {:?})", page_no);
        let frame_no = self.find_frame_by_page_no(page_no).unwrap();
        if frame_no.is_none() {
            return Ok(None);
        }
        return Ok(Some(page_no));
    }

    // read_frame reads the page, not the frame
    fn read_frame(&mut self, page_no: std::num::NonZeroU32, buffer: &mut [u8]) -> Result<()> {
        trace!("DurableWal::read_frame(page_no: {:?})", page_no);
        if let Some(frame) = self.write_cache.get(&(u32::from(page_no))) {
            trace!(
                "DurableWal::read_frame(page_no: {:?}) -- write cache hit",
                page_no
            );
            buffer.copy_from_slice(&frame.data);
            return Ok(());
        }
        let frame_no = self.find_frame_by_page_no(page_no).unwrap().unwrap();
        // check if the frame exists in the local cache
        if let Some(frame) = self.page_frames.get(&frame_no) {
            trace!(
                "DurableWal::read_frame(page_no: {:?}) -- read cache hit",
                page_no
            );
            buffer.copy_from_slice(&frame);
            return Ok(());
        }
        let frame_no = frame_no.get() as u64;
        let req = rpc::ReadFrameReq { frame_no };
        let mut binding = self.client.lock();
        let resp = binding.read_frame(req);
        let resp = tokio::task::block_in_place(|| self.rt.block_on(resp)).unwrap();
        let frame = resp.into_inner().frame.unwrap();
        buffer.copy_from_slice(&frame);
        self.page_frames
            .insert(std::num::NonZeroU32::new(frame_no as u32).unwrap(), frame);
        Ok(())
    }

    fn db_size(&self) -> u32 {
        trace!("DurableWal::db_size() => {}", self.db_size);
        self.frames_count()
    }

    fn begin_write_txn(&mut self) -> Result<()> {
        // todo: check if the connection holds a read lock
        // then try to acquire a write lock
        let mut lock_manager = self.lock_manager.lock().unwrap();
        if !lock_manager.lock("default".to_string(), self.name.clone()) {
            trace!(
                "DurableWal::begin_write_txn() lock = false, id = {}",
                self.name
            );
            return Err(rusqlite::ffi::Error::new(SQLITE_BUSY));
        };
        trace!(
            "DurableWal::begin_write_txn() lock = true, id = {}",
            self.name
        );
        Ok(())
    }

    fn end_write_txn(&mut self) -> Result<()> {
        // release only if lock is write lock
        let mut lock_manager = self.lock_manager.lock().unwrap();
        trace!(
            "DurableWal::end_write_txn() id = {}, unlocked = {}",
            self.name,
            lock_manager.unlock("default".to_string(), self.name.clone())
        );
        Ok(())
    }

    fn undo<U: libsql_sys::wal::UndoHandler>(&mut self, handler: Option<&mut U>) -> Result<()> {
        // TODO: no op
        Ok(())
    }

    fn savepoint(&mut self, rollback_data: &mut [u32]) {}

    fn savepoint_undo(&mut self, rollback_data: &mut [u32]) -> Result<()> {
        Ok(())
    }

    fn insert_frames(
        &mut self,
        page_size: std::ffi::c_int,
        page_headers: &mut libsql_sys::wal::PageHeaders,
        size_after: u32,
        is_commit: bool,
        sync_flags: std::ffi::c_int,
    ) -> Result<usize> {
        // TODO: check if it has a write lock
        // check if the size_after is > 0, if so then mark txn as committed
        trace!("name = {}", self.name);
        trace!("DurableWal::insert_frames(page_size: {}, size_after: {}, is_commit: {}, sync_flags: {})", page_size, size_after, is_commit, sync_flags);
        // add data from frame_headers to writeCache
        for (page_no, frame) in page_headers.iter() {
            self.write_cache.insert(
                page_no,
                rpc::Frame {
                    page_no: page_no as u64,
                    data: frame.to_vec(),
                },
            );
            // todo: update size after
        }

        if size_after <= 0 {
            // todo: update new size
            return Ok(0);
        }

        let req = rpc::InsertFramesReq {
            frames: self.write_cache.values().cloned().collect(),
        };
        self.write_cache.clear();
        let mut binding = self.client.lock();
        trace!("sending DurableWal::insert_frames() {:?}", req.frames.len());
        let mut c = binding
            .clone()
            .max_encoding_message_size(256 * 1024 * 1024)
            .max_decoding_message_size(256 * 1024 * 1024);
        let resp = c.insert_frames(req);
        let resp = tokio::task::block_in_place(|| self.rt.block_on(resp)).unwrap();
        self.db_size = size_after;
        Ok(resp.into_inner().num_frames as usize)
    }

    fn checkpoint(
        &mut self,
        db: &mut libsql_sys::wal::Sqlite3Db,
        mode: libsql_sys::wal::CheckpointMode,
        busy_handler: Option<&mut dyn libsql_sys::wal::BusyHandler>,
        sync_flags: u32,
        // temporary scratch buffer
        buf: &mut [u8],
        checkpoint_cb: Option<&mut dyn libsql_sys::wal::CheckpointCallback>,
        in_wal: Option<&mut i32>,
        backfilled: Option<&mut i32>,
    ) -> Result<()> {
        // checkpoint is a no op
        Ok(())
    }

    fn exclusive_mode(&mut self, op: std::ffi::c_int) -> Result<()> {
        trace!("DurableWal::exclusive_mode(op: {})", op);
        Ok(())
    }

    fn uses_heap_memory(&self) -> bool {
        trace!("DurableWal::uses_heap_memory()");
        true
    }

    fn set_db(&mut self, db: &mut libsql_sys::wal::Sqlite3Db) {}

    fn callback(&self) -> i32 {
        trace!("DurableWal::callback()");
        0
    }

    fn frames_in_wal(&self) -> u32 {
        // let req = rpc::FramesInWalReq {};
        // let mut binding = self.client.lock();
        // let resp = binding.frames_in_wal(req);
        // let resp = tokio::task::block_in_place(|| self.rt.block_on(resp)).unwrap();
        // let count = resp.into_inner().count;
        // trace!("DurableWal::frames_in_wal() = {}", count);
        // count
        0
    }
}

pub struct LockManager {
    locks: std::collections::HashMap<String, String>,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            locks: std::collections::HashMap::new(),
        }
    }

    pub fn lock(&mut self, namespace: String, wal_id: String) -> bool {
        if let Some(lock) = self.locks.get(&namespace) {
            if lock == &wal_id {
                return true;
            }
            return false;
        }
        self.locks.insert(namespace, wal_id);
        true
    }

    pub fn unlock(&mut self, namespace: String, wal_id: String) -> bool {
        if let Some(lock) = self.locks.get(&namespace) {
            if lock == &wal_id {
                self.locks.remove(&namespace);
                return true;
            }
            return false;
        }
        true
    }
}
