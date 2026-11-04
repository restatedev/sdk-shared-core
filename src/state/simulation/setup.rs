//! How the tests open a state.
use super::{MemoryBlobs, Namespace};
use crate::state::{kv::KvStore, Open, State, StateOptions};
use std::sync::Arc;

/// How a test opens a state: its options, the namespace it keeps warm in,
/// if any, and where its checkpoints are, if not in the store.
#[derive(Clone, Default)]
pub(crate) struct Setup {
    pub(crate) options: StateOptions,
    pub(crate) cache: Option<Namespace>,
    pub(crate) packs: Option<MemoryBlobs>,
}

impl Setup {
    /// Open a state over `store` this way.
    pub(crate) fn open(&self, store: Arc<dyn KvStore>) -> Open {
        let mut open = State::open(store).options(self.options.clone());
        if let Some(namespace) = &self.cache {
            open = open.cached(&namespace.cache, &namespace.key);
        }
        if let Some(packs) = &self.packs {
            open = open.checkpoints(Arc::new(packs.clone()));
        }
        open
    }
}

impl From<StateOptions> for Setup {
    fn from(options: StateOptions) -> Self {
        Self {
            options,
            ..Self::default()
        }
    }
}
