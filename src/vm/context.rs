use crate::service_protocol::messages::{EntryMessage, RestateMessage, WriteableRestateMessage};
use crate::service_protocol::{Encoder, MessageType, Version};
use crate::Value;
use bytes::Bytes;
use bytes_utils::SegmentedBuf;
use std::collections::{HashMap, VecDeque};

#[derive(Clone, Debug)]
pub(crate) struct StartInfo {
    pub(crate) id: Bytes,
    pub(crate) debug_id: String,
    pub(crate) key: String,
    pub(crate) entries_to_replay: u32,
}

pub(crate) struct Journal {
    index: Option<u32>,
    pub(crate) current_entry_ty: MessageType,
    pub(crate) current_entry_name: String,
}

impl Journal {
    pub(crate) fn transition<M: EntryMessage + RestateMessage>(&mut self, expected: &M) {
        self.index = Some(self.index.take().map(|i| i + 1).unwrap_or(0));
        self.current_entry_name = expected.name();
        self.current_entry_ty = M::ty();
    }

    pub(crate) fn index(&self) -> i64 {
        self.index.map(|u| u as i64).unwrap_or(-1)
    }

    pub(crate) fn expect_index(&self) -> u32 {
        self.index.expect("index was initialized")
    }
}

impl Default for Journal {
    fn default() -> Self {
        Journal {
            index: None,
            current_entry_ty: MessageType::Start,
            current_entry_name: "".to_string(),
        }
    }
}

pub struct Output {
    encoder: Encoder,
    pub(crate) buffer: SegmentedBuf<Bytes>,
    is_closed: bool,
}

impl Output {
    pub(crate) fn new(version: Version) -> Self {
        Self {
            encoder: Encoder::new(version),
            buffer: Default::default(),
            is_closed: false,
        }
    }

    pub(crate) fn send<M: WriteableRestateMessage>(&mut self, msg: &M) {
        if !self.is_closed {
            self.buffer.push(self.encoder.encode(msg))
        }
    }

    pub(crate) fn send_eof(&mut self) {
        self.is_closed = true;
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.is_closed
    }
}

#[derive(Debug, Default)]
pub(crate) struct AsyncResultsState {
    ready_results: HashMap<u32, Value>,
    last_acked_entry: u32,
    waiting_ack_results: VecDeque<(u32, Value)>,
}

impl AsyncResultsState {
    pub(crate) fn has_ready_result(&self, index: u32) -> bool {
        self.ready_results.contains_key(&index)
    }

    pub(crate) fn take_ready_result(&mut self, index: u32) -> Option<Value> {
        self.ready_results.remove(&index)
    }

    pub(crate) fn insert_ready_result(&mut self, index: u32, value: Value) {
        self.ready_results.insert(index, value);
    }

    pub(crate) fn insert_waiting_ack_result(&mut self, index: u32, value: Value) {
        if index <= self.last_acked_entry {
            self.ready_results.insert(index, value);
        } else {
            self.waiting_ack_results.push_back((index, value));
        }
    }

    pub(crate) fn notify_ack(&mut self, ack: u32) {
        if ack <= self.last_acked_entry {
            return;
        }
        self.last_acked_entry = ack;

        while let Some((idx, _)) = self.waiting_ack_results.front() {
            if *idx > self.last_acked_entry {
                return;
            }
            let (idx, value) = self.waiting_ack_results.pop_front().unwrap();
            self.ready_results.insert(idx, value);
        }
    }
}

#[derive(Debug)]
pub(crate) enum RunState {
    Running(String),
    NotRunning,
}

impl RunState {
    pub(crate) fn is_running(&self) -> bool {
        matches!(self, RunState::Running(_))
    }
}

pub(crate) enum EagerGetState {
    /// Means we don't have sufficient information to establish whether state is there or not, so the VM should interact with the runtime to deal with it.
    Unknown,
    Empty,
    Value(Bytes),
}

#[allow(dead_code)]
pub(crate) enum EagerGetStateKeys {
    /// Means we don't have sufficient information to establish whether state is there or not, so the VM should interact with the runtime to deal with it.
    Unknown,
    Keys(Vec<String>),
}

pub(crate) struct EagerState {
    is_partial: bool,
    // None means Void, Value means value
    values: HashMap<String, Option<Bytes>>,
}

impl Default for EagerState {
    fn default() -> Self {
        Self {
            is_partial: true,
            values: Default::default(),
        }
    }
}

impl EagerState {
    pub(crate) fn new(is_partial: bool, values: Vec<(String, Bytes)>) -> Self {
        Self {
            is_partial,
            values: values
                .into_iter()
                .map(|(key, val)| (key, Some(val)))
                .collect(),
        }
    }

    pub(crate) fn get(&self, k: &str) -> EagerGetState {
        self.values
            .get(k)
            .map(|opt| match opt {
                None => EagerGetState::Empty,
                Some(s) => EagerGetState::Value(s.clone()),
            })
            .unwrap_or(if self.is_partial {
                EagerGetState::Unknown
            } else {
                EagerGetState::Empty
            })
    }

    #[allow(dead_code)]
    pub(crate) fn get_keys(&self) -> EagerGetStateKeys {
        if self.is_partial {
            EagerGetStateKeys::Unknown
        } else {
            EagerGetStateKeys::Keys(self.values.keys().cloned().collect())
        }
    }

    pub(crate) fn set(&mut self, k: String, v: Bytes) {
        self.values.insert(k, Some(v));
    }

    pub(crate) fn clear(&mut self, k: String) {
        self.values.insert(k, None);
    }

    pub(crate) fn clear_all(&mut self) {
        self.values.clear();
        self.is_partial = false;
    }
}

/// Context of the current invocation. Holds some state across all the different FSM transitions.
pub(crate) struct Context {
    // We keep those here to persist them in case of logging after transitioning to a failure state
    // It's not very Rusty I know, but it makes much more reasonable handling failure cases.
    pub(crate) start_info: Option<StartInfo>,
    pub(crate) journal: Journal,

    pub(crate) input_is_closed: bool,
    pub(crate) output: Output,
    pub(crate) eager_state: EagerState,
}

impl Context {
    pub(crate) fn start_info(&self) -> Option<&StartInfo> {
        self.start_info.as_ref()
    }

    pub(crate) fn expect_start_info(&self) -> &StartInfo {
        self.start_info().expect("state is not WaitingStart")
    }
}
