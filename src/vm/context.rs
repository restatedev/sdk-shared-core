use crate::error::CommandMetadata;
use crate::service_protocol::messages::{
    NamedCommandMessage, RestateEncodableMessage, RestateMessage,
};
use crate::service_protocol::{Encoder, MessageType, Version};
use crate::{CommandRelationship, EntryRetryInfo};
use bytes::Bytes;
use bytes_utils::SegmentedBuf;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Clone, Debug)]
pub(crate) struct StartInfo {
    pub(crate) id: Bytes,
    pub(crate) debug_id: String,
    pub(crate) key: String,
    pub(crate) entries_to_replay: u32,
    pub(crate) retry_count_since_last_stored_entry: u32,
    pub(crate) duration_since_last_stored_entry: u64,
    pub(crate) random_seed: Option<u64>,
}

pub(crate) struct Journal {
    command_index: Option<u32>,
    notification_index: Option<u32>,
    completion_index: u32,
    signal_index: u32,
    pub(crate) current_entry_ty: MessageType,
    pub(crate) current_entry_name: String,
}

impl Journal {
    pub(crate) fn transition<M: NamedCommandMessage + RestateMessage>(&mut self, expected: &M) {
        if M::ty().is_notification() {
            self.notification_index =
                Some(self.notification_index.take().map(|i| i + 1).unwrap_or(0));
        } else if M::ty().is_command() {
            self.command_index = Some(self.command_index.take().map(|i| i + 1).unwrap_or(0));
        }
        self.current_entry_name = expected.name();
        self.current_entry_ty = M::ty();
    }

    pub(crate) fn command_index(&self) -> i64 {
        self.command_index.map(|u| u as i64).unwrap_or(-1)
    }

    pub(crate) fn notification_index(&self) -> i64 {
        self.notification_index.map(|u| u as i64).unwrap_or(-1)
    }

    pub(crate) fn next_completion_notification_id(&mut self) -> u32 {
        let next = self.completion_index;
        self.completion_index += 1;
        next
    }

    pub(crate) fn next_signal_notification_id(&mut self) -> u32 {
        let next = self.signal_index;
        self.signal_index += 1;
        next
    }

    pub(crate) fn resolve_related_command(
        &self,
        related_command: CommandRelationship,
    ) -> CommandMetadata {
        match related_command {
            CommandRelationship::Last => CommandMetadata {
                index: self.command_index.unwrap_or_default(),
                ty: self.current_entry_ty,
                name: if self.current_entry_name.is_empty() {
                    None
                } else {
                    Some(self.current_entry_name.clone().into())
                },
            },
            CommandRelationship::Next { ty, name } => CommandMetadata {
                index: self.command_index.unwrap_or_default() + 1,
                ty: ty.into(),
                name,
            },
            CommandRelationship::Specific {
                command_index,
                name,
                ty,
            } => CommandMetadata {
                index: command_index,
                ty: ty.into(),
                name,
            },
        }
    }

    pub(crate) fn last_command_metadata(&self) -> CommandMetadata {
        self.resolve_related_command(CommandRelationship::Last)
    }
}

impl Default for Journal {
    fn default() -> Self {
        Journal {
            command_index: None,
            notification_index: None,
            // Clever trick for protobuf here
            completion_index: 1,
            // 1 to 16 are reserved!
            signal_index: 17,
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

    pub(crate) fn send<M: RestateEncodableMessage>(&mut self, msg: &M) {
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
            let mut keys: Vec<_> = self.values.keys().cloned().collect();
            keys.sort();
            EagerGetStateKeys::Keys(keys)
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
    pub(crate) negotiated_protocol_version: Version,

    pub(crate) input_is_closed: bool,
    pub(crate) output: Output,
    pub(crate) eager_state: EagerState,
    pub(crate) non_deterministic_checks_ignore_payload_equality: bool,
}

impl Context {
    pub(crate) fn start_info(&self) -> Option<&StartInfo> {
        self.start_info.as_ref()
    }

    pub(crate) fn expect_start_info(&self) -> &StartInfo {
        self.start_info().expect("state is not WaitingStart")
    }

    pub(crate) fn infer_entry_retry_info(&self) -> EntryRetryInfo {
        let start_info = self.expect_start_info();
        // This is the first entry we try to commit after replay.
        //  ONLY in this case we re-use the StartInfo!
        let retry_count = start_info.retry_count_since_last_stored_entry;
        let retry_loop_duration = if start_info.retry_count_since_last_stored_entry == 0 {
            // When the retry count is == 0, the duration_since_last_stored_entry might not be zero.
            //
            // In fact, in that case the duration is the interval between the previously stored entry and the time to start/resume the invocation.
            // For the sake of entry retries though, we're not interested in that time elapsed, so we 0 it here for simplicity of the downstream consumer (the retry policy).
            Duration::ZERO
        } else {
            Duration::from_millis(start_info.duration_since_last_stored_entry)
        };
        EntryRetryInfo {
            retry_count,
            retry_loop_duration,
        }
    }
}
