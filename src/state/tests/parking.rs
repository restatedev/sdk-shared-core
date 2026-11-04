//! Parking: a value left for the namespace's next state, kept only while
//! nothing changed since.
use crate::state::{kv::KvStore, simulation::*, Result, State};
use std::sync::Arc;

#[test]
fn a_parked_value_goes_to_the_next_state_if_nothing_changed() -> Result<()> {
    futures::executor::block_on(async {
        let store: Arc<dyn KvStore> = Arc::new(MemoryKv::default());
        let open = || State::open(store.clone()).cached(shared_cache(), "parking");
        let state = open().await?;
        let mut mutation = state.mutation();
        mutation.set_record("outbox", "1".into())?;
        mutation.flush().await?;
        state.park(7u32, 1).unwrap();
        drop(state);

        let state = open().await?;
        assert_eq!(state.take_parked::<u64>(), None, "another type");
        assert_eq!(state.take_parked::<u32>(), Some(7), "still parked");
        assert_eq!(state.take_parked::<u32>(), None, "taken");
        // A mutation changes nothing until it flushes: the state is still at the
        // last durable flush, before and after.
        let mut mutation = state.mutation();
        mutation.set_record("outbox", "2".into())?;
        assert!(state.can_park());
        mutation.flush().await?;
        state.park(9u32, 1).unwrap();
        drop(state);

        // Another state of the owner flushes: what was parked is stale.
        let state = open().await?;
        let other = open().await?;
        let mut mutation = other.mutation();
        mutation.set_record("outbox", "3".into())?;
        mutation.flush().await?;
        drop((state, other));
        let state = open().await?;
        assert_eq!(state.take_parked::<u32>(), None);
        Ok(())
    })
}

/// Without a cache nothing is kept: `park` gives the value back.
#[test]
fn a_value_is_given_back_without_a_cache() -> Result<()> {
    futures::executor::block_on(async {
        let state = State::open(Arc::new(MemoryKv::default())).await?;
        assert!(!state.can_park());
        assert_eq!(state.park(8u32, 1), Err(8));
        Ok(())
    })
}

/// Two handles of the file are equal when they are of the same state.
#[test]
fn files_are_equal_by_state() -> Result<()> {
    futures::executor::block_on(async {
        let store: Arc<dyn KvStore> = Arc::new(MemoryKv::default());
        let state = State::open(store.clone()).await?;
        assert!(state.file() == state.file());
        let reader = State::open(store).reader().await?;
        assert!(state.file() != reader.file());
        Ok(())
    })
}
