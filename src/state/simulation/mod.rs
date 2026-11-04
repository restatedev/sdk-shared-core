//! Opt-in in-memory simulation, fault injection and measurement support.
//! In tests, and with the `__bench` Cargo feature.
mod blobs;
mod fault;
mod latency;
mod memory;
mod monitor;
#[cfg(test)]
mod setup;
pub use blobs::{BlobFaults, MemoryBlobs};
pub use fault::{Effect, Event, Failpoint, FaultKv, Operation};
pub use latency::LatencyKv;
pub use memory::{object_kind, MemoryKv};
#[cfg(feature = "__bench")]
pub use memory::{Stats, Traffic};
pub use monitor::MonitorKv;
#[cfg(test)]
pub(crate) use setup::Setup;

use crate::state::{PageCache, PageCacheConfig};
use std::sync::{
    atomic::{AtomicU64, Ordering::Relaxed},
    OnceLock,
};

/// Cache settings for tests: a temporary file, and 1 MiB disk blocks.
pub fn cache_config(memory: usize, disk: usize) -> PageCacheConfig {
    PageCacheConfig {
        disk_block: 1 << 20,
        ..PageCacheConfig::new(memory, disk)
    }
}

/// Open a [`PageCache`] from any context, even inside an executor.
pub fn open_cache(config: PageCacheConfig) -> PageCache {
    std::thread::spawn(move || futures::executor::block_on(PageCache::open(config)))
        .join()
        .unwrap()
        .expect("open the page cache")
}

/// The process-wide cache that tests share: 64 MiB of memory, 256 MiB of disk.
pub fn shared_cache() -> &'static PageCache {
    static CACHE: OnceLock<PageCache> = OnceLock::new();
    CACHE.get_or_init(|| open_cache(cache_config(64 << 20, 256 << 20)))
}

/// A namespace of a page cache: what [`crate::sql::Open::cached`] takes, as one value.
#[derive(Clone)]
pub struct Namespace {
    pub cache: PageCache,
    pub key: String,
}

impl Namespace {
    pub fn new(cache: &PageCache, key: &str) -> Self {
        Self {
            cache: cache.clone(),
            key: key.to_owned(),
        }
    }
}

/// A namespace of [`shared_cache`] that nobody used before: a fresh owner.
pub fn fresh_namespace() -> Namespace {
    static KEYS: AtomicU64 = AtomicU64::new(0);
    let key = format!("namespace-{}", KEYS.fetch_add(1, Relaxed));
    Namespace::new(shared_cache(), &key)
}

/// The handle of a [`fresh_namespace`].
#[cfg(test)]
pub(crate) fn fresh_namespace_cache() -> crate::state::cache::NamespaceCache {
    let namespace = fresh_namespace();
    namespace.cache.namespace(&namespace.key)
}
