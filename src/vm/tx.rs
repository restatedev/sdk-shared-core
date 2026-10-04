//! Transactional handlers: an actor-style execution mode where the whole handler body is a single
//! commit boundary.
//!
//! In this mode the handler body runs without writing anything to the journal. State reads are
//! served from the eager state snapshot shipped with the `StartMessage`, state writes and outgoing
//! one-way calls are buffered in memory. When the handler returns, everything it did, plus its
//! output, is serialized into a single commit record, which is made durable as the result of one
//! `RunCommand` named [`TX_COMMIT_RUN_NAME`].
//!
//! Once the commit record is durable, the VM deterministically re-emits the record as regular
//! `SetState`/`ClearState`/`OneWayCall`/`Output` commands. If the attempt dies half-way through
//! that phase, the next attempt replays the record and finishes applying it, without running
//! the handler body again. If the attempt dies before the commit record is durable, nothing
//! the handler did is visible, and the next attempt runs the handler body from scratch.

use crate::service_protocol::messages::{Failure, OneWayCallCommandMessage};
use crate::vm::context::{EagerGetState, EagerGetStateKeys, EagerState};
use crate::{NonEmptyValue, NotificationHandle};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};

/// Name of the `RunCommand` carrying the commit record.
pub(crate) const TX_COMMIT_RUN_NAME: &str = "restate.tx.commit";

/// Version of the commit record encoding.
const TX_COMMIT_RECORD_VERSION: u32 = 1;

// --- Commit record encoding.
//
// The record is stored in the journal, so its encoding must be stable across SDK versions.

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct TxCommitRecord {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    /// If true, all the state is cleared before applying `state_mutations`.
    #[prost(bool, tag = "2")]
    pub clear_all_state: bool,
    /// State mutations, ordered by key.
    #[prost(message, repeated, tag = "3")]
    pub state_mutations: Vec<TxStateMutation>,
    /// Outgoing one-way calls, in the order they were issued.
    /// `invocation_id_notification_idx` is not set here, it's assigned when applying the record.
    #[prost(message, repeated, tag = "4")]
    pub sends: Vec<OneWayCallCommandMessage>,
    #[prost(oneof = "tx_commit_record::Output", tags = "14, 15")]
    pub output: Option<tx_commit_record::Output>,
}

pub(crate) mod tx_commit_record {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Output {
        #[prost(bytes = "bytes", tag = "14")]
        Value(::prost::bytes::Bytes),
        #[prost(message, tag = "15")]
        Failure(super::Failure),
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct TxStateMutation {
    #[prost(string, tag = "1")]
    pub key: String,
    /// `None` means clear.
    #[prost(bytes = "bytes", optional, tag = "2")]
    pub value: Option<Bytes>,
}

impl TxCommitRecord {
    pub(crate) fn output(&self) -> Option<NonEmptyValue> {
        match self.output.as_ref()? {
            tx_commit_record::Output::Value(v) => Some(NonEmptyValue::Success(v.clone())),
            tx_commit_record::Output::Failure(f) => Some(NonEmptyValue::Failure(f.clone().into())),
        }
    }
}

// --- Write set

/// Buffered state writes of the handler body, layered on top of the eager state snapshot.
#[derive(Debug, Default)]
pub(crate) struct WriteSet {
    clear_all: bool,
    /// `None` means cleared.
    entries: BTreeMap<String, Option<Bytes>>,
}

/// The state snapshot doesn't contain the requested information, because the runtime sent a partial state.
pub(crate) struct PartialStateError;

impl WriteSet {
    pub(crate) fn get(
        &self,
        key: &str,
        snapshot: &EagerState,
    ) -> Result<Option<Bytes>, PartialStateError> {
        if let Some(v) = self.entries.get(key) {
            return Ok(v.clone());
        }
        if self.clear_all {
            return Ok(None);
        }
        snapshot_get(key, snapshot)
    }

    pub(crate) fn keys(&self, snapshot: &EagerState) -> Result<Vec<String>, PartialStateError> {
        let mut keys: BTreeSet<String> = if self.clear_all {
            BTreeSet::new()
        } else {
            snapshot_keys(snapshot)?.into_iter().collect()
        };
        for (k, v) in &self.entries {
            if v.is_some() {
                keys.insert(k.clone());
            } else {
                keys.remove(k);
            }
        }
        Ok(keys.into_iter().collect())
    }

    pub(crate) fn set(&mut self, key: String, value: Bytes) {
        self.entries.insert(key, Some(value));
    }

    pub(crate) fn clear(&mut self, key: String) {
        self.entries.insert(key, None);
    }

    pub(crate) fn clear_all(&mut self) {
        self.clear_all = true;
        self.entries.clear();
    }

    /// Computes the minimal set of mutations to apply on top of `snapshot`,
    /// dropping writes that would not change the stored state.
    pub(crate) fn into_mutations(self, snapshot: &EagerState) -> (bool, Vec<TxStateMutation>) {
        let clear_all = self.clear_all;
        let mutations = self
            .entries
            .into_iter()
            .filter(|(key, value)| {
                if clear_all {
                    // Everything was cleared already, only the sets matter
                    return value.is_some();
                }
                match (snapshot.get(key), value) {
                    (EagerGetState::Value(old), Some(new)) => old != new,
                    (EagerGetState::Empty, None) => false,
                    _ => true,
                }
            })
            .map(|(key, value)| TxStateMutation { key, value })
            .collect();
        (clear_all, mutations)
    }
}

pub(crate) fn snapshot_get(
    key: &str,
    snapshot: &EagerState,
) -> Result<Option<Bytes>, PartialStateError> {
    match snapshot.get(key) {
        EagerGetState::Unknown => Err(PartialStateError),
        EagerGetState::Empty => Ok(None),
        EagerGetState::Value(v) => Ok(Some(v)),
    }
}

pub(crate) fn snapshot_keys(snapshot: &EagerState) -> Result<Vec<String>, PartialStateError> {
    match snapshot.get_keys() {
        EagerGetStateKeys::Unknown => Err(PartialStateError),
        EagerGetStateKeys::Keys(keys) => Ok(keys),
    }
}

// --- Transaction state machine

#[derive(Debug, Default)]
pub(crate) enum TxState {
    /// No transactional handler body was started.
    #[default]
    Inactive,
    /// The handler body is executing.
    Executing {
        /// Set if the commit `RunCommand` was already in the replayed journal, without its result.
        commit_run_handle: Option<NotificationHandle>,
        write_set: WriteSet,
        sends: Vec<OneWayCallCommandMessage>,
    },
    /// The commit record was proposed in this attempt, or was found in the replayed journal.
    Committed {
        handle: NotificationHandle,
        /// Set if the commit record was proposed in this attempt.
        proposed_record: Option<TxCommitRecord>,
    },
    /// The commit record was applied.
    Ended,
}

impl TxState {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            TxState::Inactive => "Inactive",
            TxState::Executing { .. } => "Executing",
            TxState::Committed { .. } => "Committed",
            TxState::Ended => "Ended",
        }
    }
}

pub(crate) fn new_commit_record(
    clear_all_state: bool,
    state_mutations: Vec<TxStateMutation>,
    sends: Vec<OneWayCallCommandMessage>,
    output: NonEmptyValue,
) -> TxCommitRecord {
    TxCommitRecord {
        version: TX_COMMIT_RECORD_VERSION,
        clear_all_state,
        state_mutations,
        sends,
        output: Some(match output {
            NonEmptyValue::Success(v) => tx_commit_record::Output::Value(v),
            NonEmptyValue::Failure(f) => tx_commit_record::Output::Failure(f.into()),
        }),
    }
}

pub(crate) fn check_record_version(record: &TxCommitRecord) -> bool {
    record.version == TX_COMMIT_RECORD_VERSION
}
