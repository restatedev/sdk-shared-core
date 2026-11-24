use crate::explicit_recovery::vm::errors::result_ids_exhausted;
use crate::explicit_recovery::Options;
use crate::service_protocol::explicit_recovery::messages::{self, RestateMessage};
use crate::service_protocol::explicit_recovery::ProtocolVersion;
use crate::Error;
use bytes::Bytes;
use bytes_utils::SegmentedBuf;
use std::num::NonZero;

/// What `StartMessage` says of the invocation and of this attempt.
#[derive(Clone, Debug)]
pub(crate) struct StartInfo {
    pub(crate) debug_id: String,
    pub(crate) key: String,
    pub(crate) headers: Vec<messages::Header>,
    pub(crate) input: Bytes,
    pub(crate) scope: Option<String>,
    pub(crate) limit_key: Option<String>,
    pub(crate) idempotency_key: Option<String>,
    pub(crate) attempt: u32,
    /// The invocation received the cancel signal before this attempt.
    pub(crate) cancelled: bool,
    /// The features the runtime supports in this attempt, beyond the version's baseline.
    pub(crate) features: Vec<u32>,
}

/// The ids the attempt picks: result ids, from `StartMessage.next_result_id` and strictly
/// increasing, and span ids.
#[derive(Debug)]
pub(crate) struct Ids {
    /// `None` once `u32::MAX` was handed out: ids never wrap.
    next_result_id: Option<NonZero<u32>>,
    /// The ids below are those of the earlier attempts.
    first_result_id: u32,
    next_span_id: NonZero<u32>,
}

impl Default for Ids {
    fn default() -> Self {
        Ids {
            next_result_id: NonZero::new(1),
            first_result_id: 1,
            next_span_id: NonZero::new(1).unwrap(),
        }
    }
}

impl Ids {
    pub(crate) fn start_at(&mut self, next_result_id: NonZero<u32>) {
        self.next_result_id = Some(next_result_id);
        self.first_result_id = next_result_id.get();
    }

    pub(crate) fn next_result_id(&mut self) -> Result<NonZero<u32>, Error> {
        let next = self.next_result_id.ok_or_else(result_ids_exhausted)?;
        self.next_result_id = next.checked_add(1);
        Ok(next)
    }

    /// Whether `result_id` is below the ids of this attempt: one of an earlier attempt's actions.
    pub(crate) fn is_earlier_result_id(&self, result_id: u32) -> bool {
        result_id < self.first_result_id
    }

    pub(crate) fn next_span_id(&mut self) -> NonZero<u32> {
        let next = self.next_span_id;
        self.next_span_id = self.next_span_id.saturating_add(1);
        next
    }
}

pub(crate) struct Output {
    pub(crate) buffer: SegmentedBuf<Bytes>,
    is_closed: bool,
}

impl Output {
    pub(crate) fn new() -> Self {
        Self {
            buffer: Default::default(),
            is_closed: false,
        }
    }

    /// Ignored once closed: nothing follows the message that ends the attempt.
    pub(crate) fn send<M: RestateMessage>(&mut self, msg: &M) {
        if !self.is_closed {
            self.buffer.push(messages::encode(msg))
        }
    }

    pub(crate) fn send_eof(&mut self) {
        self.is_closed = true;
    }

    /// The last message is written: nothing follows it.
    pub(crate) fn is_closed(&self) -> bool {
        self.is_closed
    }
}

/// Context of the current attempt. Holds some state across all the different FSM transitions.
pub(crate) struct Context {
    pub(crate) start_info: Option<StartInfo>,
    pub(crate) negotiated_protocol_version: ProtocolVersion,
    pub(crate) options: Options,
    pub(crate) ids: Ids,

    pub(crate) input_is_closed: bool,
    pub(crate) output: Output,

    /// The runtime sent the cancel signal in this attempt.
    pub(crate) cancel_received: bool,
    /// The cancel signal was handed out by `take_next_notification`.
    pub(crate) cancel_taken: bool,
}

impl Context {
    pub(crate) fn start_info(&self) -> Option<&StartInfo> {
        self.start_info.as_ref()
    }

    pub(crate) fn expect_start_info(&self) -> &StartInfo {
        self.start_info().expect("state is not WaitingStart")
    }
}
