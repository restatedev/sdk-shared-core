use crate::explicit_recovery::TraceId;
use crate::service_protocol::MessageType;
use crate::vm::errors::codes;
use crate::Error;
use std::time::Duration;

/// Returned by every call once the runtime shut the attempt down.
pub(crate) fn attempt_shut_down() -> Error {
    Error::new(
        codes::SUSPENDED,
        "The attempt was shut down: the input stream was closed, so no response can arrive anymore",
    )
}

/// Written when the attempt was shut down with no response pending that Restate could start a new
/// attempt on. With a retry delay, the runtime retries right away without counting the retry.
pub(crate) fn restart_attempt() -> Error {
    Error::new(
        codes::INTERNAL,
        "The attempt was shut down with no call pending, starting a new attempt",
    )
    .with_next_retry_delay_override(Duration::ZERO)
}

/// The error that fails an invocation served over a request/response transport (e.g. AWS Lambda,
/// HTTP/1.1), pausing it: see [`crate::explicit_recovery::ExplicitRecoveryVM::notify_error`].
pub fn request_response_unsupported() -> Error {
    Error::new(
        codes::UNSUPPORTED_FEATURE,
        "Handlers without journal replay need a bidirectional stream between Restate and the service: the request/response protocol (e.g. AWS Lambda, HTTP/1.1) can't deliver responses to them",
    )
    .with_should_pause(true)
}

pub(crate) fn input_closed_while_waiting_entries() -> Error {
    Error::new(
        codes::PROTOCOL_VIOLATION,
        "The input was closed while still waiting to receive the start of the invocation",
    )
}

pub(crate) fn missing_input_command() -> Error {
    Error::new(
        codes::PROTOCOL_VIOLATION,
        "The entries of the earlier attempts contain no input command",
    )
}

pub(crate) fn unexpected_message(ty: MessageType, state: &'static str) -> Error {
    Error::new(
        codes::PROTOCOL_VIOLATION,
        format!("Unexpected message '{ty:?}' while {state}"),
    )
}

pub(crate) fn unknown_trace(trace: TraceId) -> Error {
    Error::new(
        codes::INTERNAL,
        format!(
            "Unknown trace {}: it was never started by this attempt, or it was already ended",
            u32::from(trace)
        ),
    )
}

pub(crate) fn empty_ephemeral_notification() -> Error {
    Error::new(
        codes::PROTOCOL_VIOLATION,
        "Unexpected empty result variant for get state ephemeral notification",
    )
}
