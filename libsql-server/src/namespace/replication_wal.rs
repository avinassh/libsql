use std::sync::{Arc, Mutex};

use bottomless::bottomless_wal::BottomlessWalWrapper;
use bottomless::replicator::Replicator;
use libsql_sys::wal::wrapper::{Then, WrapWal};

use crate::connection::connection_manager::ManagedConnectionWal;
use crate::replication::primary::replication_logger_wal::ReplicationLoggerWalWrapper;
use crate::replication::ReplicationLogger;

// pub type ReplicationWalManager =
//     WalWrapper<Option<BottomlessWalWrapper>, ReplicationLoggerWalManager>;
// pub type ReplicationWal = WrappedWal<Option<BottomlessWalWrapper>, ReplicationLoggerWal>;

pub type ReplicationWalWrapper =
    Then<ReplicationLoggerWalWrapper, Option<BottomlessWalWrapper>, ManagedConnectionWal>;

// pub fn make_replication_wal(
//     bottomless: Option<Replicator>,
//     logger: Arc<ReplicationLogger>,
//     lock_manager: Arc<Mutex<LockManager>>,
// ) -> ReplicationWalManager {
//     let wal_manager = libsql_storage::DurableWalManager::new(lock_manager);
//     WalWrapper::new(
//         bottomless.map(|b| BottomlessWalWrapper::new(Arc::new(std::sync::Mutex::new(Some(b))))),
//         ReplicationLoggerWalManager::new(wal_manager, logger),
//     )
// }

pub fn make_replication_wal_wrapper(
    bottomless: Option<Replicator>,
    logger: Arc<ReplicationLogger>,
    lock_manager: Arc<Mutex<libsql_storage::LockManager>>,
) -> ReplicationWalWrapper {
    let wal_manager = libsql_storage::DurableWalManager::new(lock_manager);
    let btm =
        bottomless.map(|b| BottomlessWalWrapper::new(Arc::new(std::sync::Mutex::new(Some(b)))));
    ReplicationLoggerWalWrapper::new(logger).then(btm)
}
