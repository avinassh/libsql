use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::rpc::Frame;
use libsql_sys::rusqlite::{ffi, params, Connection, Error, Result};

/// We use LocalCache to cache frames and transaction state. Each namespace gets its own cache
/// which is currently stored in a SQLite DB file, along with the main database file.
///
/// Frames Cache:
///     Frames are immutable. So we can cache all the frames locally, and it does not require them
///     to be fetched from the storage server. We cache the frame data with frame_no being the key.
///
/// Transaction State:
///     Whenever a transaction reads any pages from storage server, we cache them in the transaction
///     state. Since we want to provide a consistent view of the database, for the next reads we can
///     serve the pages from the cache. Any writes a transaction makes are cached too. At the time of
///     commit they are removed from the cache and sent to the storage server.

pub struct LocalCache {
    conn: Connection,
    path: Arc<Path>,
}

impl LocalCache {
    pub fn new(db_path: &Path) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        let path: Arc<Path> = Arc::from(PathBuf::from(db_path));
        let local_cache = LocalCache { conn, path };
        local_cache.create_table()?;
        Ok(local_cache)
    }

    fn create_table(&self) -> Result<()> {
        self.conn.pragma_update(None, "journal_mode", &"WAL")?;
        self.conn.pragma_update(None, "synchronous", &"NORMAL")?;
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS frames (
                frame_no INTEGER PRIMARY KEY NOT NULL,
                data BLOB NOT NULL
            )",
            // page_no INTEGER NOT NULL,
            // cretae index (page_no, frame)
            [],
        )?;

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS transactions (
                txn_id TEXT NOT NULL,
                page_no INTEGER NOT NULL,
                data BLOB NOT NULL,
                PRIMARY KEY (txn_id, page_no)
            )",
            [],
        )?;
        Ok(())
    }

    pub fn insert_frame(&self, frame_no: u64, frame_data: &[u8]) -> Result<()> {
        match self.conn.execute(
            "INSERT INTO frames (frame_no, data) VALUES (?1, ?2)",
            params![frame_no, frame_data],
        ) {
            Ok(_) => Ok(()),
            Err(Error::SqliteFailure(e, _)) if e.code == ffi::ErrorCode::ConstraintViolation => {
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn insert_frames(&self, frames: Vec<Frame>) -> Result<()> {
        Ok(())
    }

    pub fn get_frame(&self, frame_no: u64) -> Result<Option<Vec<u8>>> {
        let mut stmt = self
            .conn
            .prepare("SELECT data FROM frames WHERE frame_no = ?1")?;
        match stmt.query_row(params![frame_no], |row| row.get(0)) {
            Ok(frame_data) => Ok(Some(frame_data)),
            Err(Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn insert_page(&self, txn_id: &str, page_no: u32, frame_data: &[u8]) -> Result<()> {
        self.conn.execute(
            "INSERT INTO transactions (txn_id, page_no, data) VALUES (?1, ?2, ?3)
                 ON CONFLICT(txn_id, page_no) DO UPDATE SET data = ?3",
            params![txn_id, page_no, frame_data],
        )?;
        Ok(())
    }

    pub fn get_page(&self, txn_id: &str, page_no: u32) -> Result<Option<Vec<u8>>> {
        let mut stmt = self
            .conn
            .prepare("SELECT data FROM transactions WHERE txn_id = ?1 AND page_no = ?2")?;
        match stmt.query_row(params![txn_id, page_no], |row| row.get(0)) {
            Ok(frame_data) => Ok(Some(frame_data)),
            Err(Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn get_all_pages(&self, txn_id: &str) -> Result<BTreeMap<u32, Vec<u8>>> {
        let mut stmt = self
            .conn
            .prepare("SELECT page_no, data FROM transactions WHERE txn_id = ?1")?;
        let pages: BTreeMap<u32, Vec<u8>> = stmt
            .query_map(params![txn_id], |row| Ok((row.get(0)?, row.get(1)?)))?
            .filter_map(|result| result.ok())
            .collect();
        self.conn.execute(
            "DELETE FROM transactions WHERE txn_id = ?1",
            params![txn_id],
        )?;
        Ok(pages)
    }
}

impl Clone for LocalCache {
    fn clone(&self) -> Self {
        let conn = Connection::open(&*self.path).expect("failed to open database");
        LocalCache {
            conn,
            path: Arc::clone(&self.path),
        }
    }
}
