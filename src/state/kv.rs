//! Backend contract: a Restate object's state, as a handler without a journal
//! sees it.
//!
//! - **Writes are queued, in order.** [`KvStore::put`] and [`KvStore::delete`]
//!   return at once. The store keeps them in issue order: a crash loses a
//!   suffix of the writes not yet synced, never a write without the ones
//!   before it.
//! - **A sync makes them durable.** [`KvStore::sync`] completes once every
//!   write issued before it is stored. Its failure, or a crash, leaves an
//!   unknown prefix of them stored.
//! - **Reads see the writes.** [`KvStore::get`] returns the latest value,
//!   stored or written by this owner since.
//!
//! So a write needs no acknowledgement for a later one to rely on it: if the
//! later one survives, so does it. A value is atomic: no partial value is ever
//! visible.
use bytes::Bytes;
use futures::future::BoxFuture;
use std::io;

/// A single-namespace key/value store; see the module documentation.
pub trait KvStore: Send + Sync {
    /// The latest value of `key`: stored, or written since; `None` if absent.
    /// `Bytes` lets a network client hand over its receive buffer without copying.
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<Bytes>>>;
    /// Queue the replacement of a whole value, after every write issued before.
    fn put(&self, key: &str, value: Bytes) -> io::Result<()>;
    /// Queue the removal of a key, after every write issued before. Deleting
    /// a missing key succeeds.
    fn delete(&self, key: &str) -> io::Result<()>;
    /// Wait until every write issued so far is durable.
    fn sync(&self) -> BoxFuture<'_, io::Result<()>>;
    /// [`KvStore::put`], then wait until it, and so every write before it, is durable: a put and a
    /// [`KvStore::sync`], which a store may do in one step (Restate acknowledges the put). The put
    /// is queued when this is called, as [`KvStore::put`]'s is.
    fn put_and_sync<'a>(&'a self, key: &'a str, value: Bytes) -> BoxFuture<'a, io::Result<()>> {
        match self.put(key, value) {
            Ok(()) => self.sync(),
            Err(error) => Box::pin(futures::future::ready(Err(error))),
        }
    }
    /// Every key, stored or written since, as Restate's `getKeys`: what
    /// checkpoints kept here need, to delete the packs no checkpoint needs. A store that cannot fails with
    /// [`io::ErrorKind::Unsupported`]; its garbage stays.
    fn keys(&self) -> BoxFuture<'_, io::Result<Vec<String>>> {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "this store cannot list its keys",
            ))
        })
    }
}
