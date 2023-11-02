use crate::connection::libsql::LibSqlConnection;
use crate::connection::write_proxy::WriteProxyConnection;
use crate::connection::{Connection, MakeConnection, TrackedConnection};
use crate::replication::{ReplicationLogger, ReplicationLoggerHook};
use async_trait::async_trait;
use std::sync::Arc;

pub type Result<T> = anyhow::Result<T>;

#[async_trait]
pub trait Database: Sync + Send + 'static {
    /// The connection type of the database
    type Connection: Connection;

    fn connection_maker(&self) -> Arc<dyn MakeConnection<Connection = Self::Connection>>;
    async fn shutdown(&self) -> Result<()>;
}

pub struct ReplicaDatabase {
    pub connection_maker:
        Arc<dyn MakeConnection<Connection = TrackedConnection<WriteProxyConnection>>>,
}

#[async_trait]
impl Database for ReplicaDatabase {
    type Connection = TrackedConnection<WriteProxyConnection>;

    fn connection_maker(&self) -> Arc<dyn MakeConnection<Connection = Self::Connection>> {
        self.connection_maker.clone()
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

pub type PrimaryConnection = TrackedConnection<LibSqlConnection<ReplicationLoggerHook>>;

pub struct PrimaryDatabase {
    pub logger: Arc<ReplicationLogger>,
    pub connection_maker: Arc<dyn MakeConnection<Connection = PrimaryConnection>>,
}

#[async_trait]
impl Database for PrimaryDatabase {
    type Connection = PrimaryConnection;

    fn connection_maker(&self) -> Arc<dyn MakeConnection<Connection = Self::Connection>> {
        self.connection_maker.clone()
    }

    async fn shutdown(&self) -> Result<()> {
        self.logger.closed_signal.send_replace(true);
        if let Some(replicator) = &self.logger.bottomless_replicator {
            let t = replicator.lock().unwrap().take();
            if let Some(mut replicator) = t {
                replicator.wait_until_snapshotted().await?;
            }
        }
        Ok(())
    }
}
