//! A host of the database, as the tests use it: a session over a state of
//! its own, every SQL commit durable when its call returns, as the Node.js
//! addon's.
use crate::sql::{Database, Limits};
pub(crate) use crate::state::simulation::Setup as StateSetup;
use crate::state::{kv::KvStore, FlushReport, Result, StateOptions};
use bytes::Bytes;
use std::{ops::Deref, sync::Arc};

impl StateSetup {
    /// These options, changed by `change`.
    pub(crate) fn with(self, change: impl FnOnce(StateOptions) -> StateOptions) -> Self {
        Self {
            options: change(self.options),
            ..self
        }
    }
}

/// A session of a database on a state of its own.
pub(super) struct Db(Database);

impl Deref for Db {
    type Target = Database;
    fn deref(&self) -> &Database {
        &self.0
    }
}

impl Db {
    pub(super) async fn open(
        store: Arc<dyn KvStore>,
        setup: impl Into<StateSetup>,
    ) -> Result<Self> {
        Self::open_with(store, setup, Limits::default()).await
    }

    /// [`Self::open`] with explicit SQL limits.
    pub(super) async fn open_with(
        store: Arc<dyn KvStore>,
        setup: impl Into<StateSetup>,
        limits: Limits,
    ) -> Result<Self> {
        let state = setup.into().open(store).await?;
        Ok(Self(Database::with_limits(state, limits).await?))
    }

    /// See [`transaction`].
    pub(super) async fn transaction<T>(
        &self,
        body: impl AsyncFnOnce(&Database) -> Result<T>,
    ) -> Result<T> {
        transaction(&self.0, body).await
    }

    /// End the session, leaving its engine.
    pub(super) async fn save(self) -> Result<()> {
        self.0.save().await
    }

    /// Checkpoint ([`Database::checkpoint`]), then end the session: the
    /// database's pages are read from the checkpoint's pack after it. The
    /// checkpoint's report.
    pub(super) async fn save_checkpointed(self) -> Result<FlushReport> {
        let report = self.0.checkpoint().await?;
        self.0.save().await?;
        Ok(report)
    }

    /// `COMMIT` the open transaction with the record `outbox`, in one
    /// flush: what the addon's `commit(records)` does.
    pub(super) async fn commit_with(&self, outbox: Bytes) -> Result<()> {
        self.0.commit([("outbox".into(), outbox)]).await
    }

    /// The record `outbox`, as last flushed.
    pub(super) fn outbox(&self) -> Bytes {
        self.0.state().record("outbox")
    }

    /// The state's version: the flushes ever made, as this session holds it.
    pub(super) fn version(&self) -> u64 {
        self.0.state().version()
    }
}

/// Run `body` as one transaction, as the host's `transaction()` does:
/// `begin`, then `commit` when it returns `Ok`, or `rollback` when it or the
/// `COMMIT` fails.
pub(super) async fn transaction<T>(
    db: &Database,
    body: impl AsyncFnOnce(&Database) -> Result<T>,
) -> Result<T> {
    db.begin().await?;
    let result = match body(db).await {
        Ok(value) => db.commit([]).await.map(|()| value),
        Err(error) => Err(error),
    };
    if result.is_err() {
        // The error says why: a failing rollback only ends the session.
        let _ = db.rollback().await;
    }
    result
}
