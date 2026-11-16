use crate::service_protocol::messages::RestateEncodableMessage;
use crate::service_protocol::{CompletionId, Encoder, Version};
use bytes::Bytes;
use bytes_utils::SegmentedBuf;
use std::collections::HashMap;
use std::num::NonZero;

#[derive(Clone, Debug)]
pub(crate) struct StartInfo {
    pub(crate) debug_id: String,
    pub(crate) key: String,
    /// The entries the earlier attempts stored, which the runtime sends after the start message.
    pub(crate) known_entries: u32,
    pub(crate) random_seed: u64,
    pub(crate) scope: Option<String>,
    pub(crate) limit_key: Option<String>,
    pub(crate) idempotency_key: Option<String>,
}

/// The numbering of the attempt's commands and completion ids.
///
/// Nothing is replayed, but the runtime still numbers the commands of the earlier attempts: this
/// attempt's continue after them, see [`Journal::skip_to`].
pub(crate) struct Journal {
    command_index: u32,
    completion_index: NonZero<CompletionId>,
    /// The completion ids below are those of the earlier attempts.
    first_completion_id: NonZero<CompletionId>,
    ephemeral_completion_index: NonZero<u32>,
}

impl Default for Journal {
    fn default() -> Self {
        Journal {
            command_index: 0,
            // Clever trick for protobuf here
            completion_index: unsafe { NonZero::new_unchecked(1) },
            first_completion_id: unsafe { NonZero::new_unchecked(1) },
            ephemeral_completion_index: unsafe { NonZero::new_unchecked(1) },
        }
    }
}

impl Journal {
    /// Number this attempt's commands after the `earlier_commands` of the earlier attempts (the
    /// input included), and its completion ids after theirs: as CoreVM's journal skip does,
    /// completion ids start at 1, and a command after the input takes at most two.
    pub(crate) fn skip_to(&mut self, earlier_commands: u32) {
        self.command_index = earlier_commands;
        self.first_completion_id = earlier_commands
            .saturating_sub(1)
            .saturating_mul(2)
            .saturating_add(1)
            .try_into()
            .unwrap();
        self.completion_index = self.first_completion_id;
    }

    pub(crate) fn next_command_index(&mut self) -> u32 {
        let next = self.command_index;
        self.command_index += 1;
        next
    }

    pub(crate) fn next_command_index_nonzero(&mut self) -> NonZero<u32> {
        NonZero::new(self.next_command_index())
            .expect("this attempt's commands come after the input, which is command 0")
    }

    pub(crate) fn next_completion_notification_id(&mut self) -> NonZero<CompletionId> {
        let next = self.completion_index;
        self.completion_index = self.completion_index.saturating_add(1);
        next
    }

    pub(crate) fn next_ephemeral_completion_id(&mut self) -> NonZero<u32> {
        let next = self.ephemeral_completion_index;
        self.ephemeral_completion_index = self.ephemeral_completion_index.saturating_add(1);
        next
    }

    /// Whether `completion_id` is one of an earlier attempt's commands.
    pub(crate) fn is_earlier_completion_id(&self, completion_id: CompletionId) -> bool {
        completion_id < u32::from(self.first_completion_id)
    }
}

pub(crate) struct Output {
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

    /// Ignored once closed: nothing follows the message that ends the attempt.
    pub(crate) fn send<M: RestateEncodableMessage>(&mut self, msg: &M) {
        if !self.is_closed {
            self.buffer.push(self.encoder.encode(msg))
        }
    }

    pub(crate) fn send_eof(&mut self) {
        self.is_closed = true;
    }
}

pub(crate) enum EagerGetState {
    /// Means we don't have sufficient information to establish whether state is there or not, so the VM should interact with the runtime to deal with it.
    Unknown,
    Empty,
    Value(Bytes),
}

pub(crate) enum EagerGetStateKeys {
    /// Means we don't have sufficient information to establish whether state is there or not, so the VM should interact with the runtime to deal with it.
    Unknown,
    Keys(Vec<String>),
}

/// The state values known without asking the runtime: those it sent at the start, and those this
/// attempt wrote since.
#[derive(Debug)]
pub(crate) struct EagerState {
    is_partial: bool,
    // None means Void, Value means value
    values: HashMap<String, Option<Bytes>>,
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

    /// Unlike CoreVM's, the cleared keys are left out.
    pub(crate) fn get_keys(&self) -> EagerGetStateKeys {
        if self.is_partial {
            EagerGetStateKeys::Unknown
        } else {
            let mut keys: Vec<_> = self
                .values
                .iter()
                .filter(|(_, value)| value.is_some())
                .map(|(key, _)| key.clone())
                .collect();
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

/// Context of the current attempt. Holds some state across all the different FSM transitions.
pub(crate) struct Context {
    pub(crate) start_info: Option<StartInfo>,
    pub(crate) journal: Journal,
    pub(crate) negotiated_protocol_version: Version,

    pub(crate) input_is_closed: bool,
    pub(crate) output: Output,

    /// Whether a cancellation signal was received, by this attempt or an earlier one.
    pub(crate) cancelled: bool,
}

impl Context {
    pub(crate) fn start_info(&self) -> Option<&StartInfo> {
        self.start_info.as_ref()
    }

    pub(crate) fn expect_start_info(&self) -> &StartInfo {
        self.start_info().expect("state is not WaitingStart")
    }
}
