use crate::explicit_recovery::SpanId;
use crate::service_protocol::explicit_recovery::MessageType;
use crate::vm::errors::codes;
use crate::Error;

/// Returned by every call once the runtime shut the attempt down.
pub(crate) fn attempt_shut_down() -> Error {
    Error::new(
        codes::SUSPENDED,
        "The attempt was shut down: Restate closed its side of the stream, so no result can arrive anymore",
    )
}

/// The error of a handler that needs features the attempt does not support (code 573): they are
/// named in its metadata, under `restate.missing_features`, as their numbers separated by commas.
pub fn missing_features(features: &[u32]) -> Error {
    let numbers = features
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(",");
    Error::new(
        codes::UNSUPPORTED_FEATURE,
        format!("The handler needs features this attempt does not support: {numbers}"),
    )
    .with_metadata([("restate.missing_features", numbers)])
}

// --- The runtime violated the protocol: 571

pub(crate) fn protocol_violation(what: impl Into<String>) -> Error {
    Error::new(codes::PROTOCOL_VIOLATION, what.into())
}

pub(crate) fn unexpected_message(ty: MessageType, state: &'static str) -> Error {
    protocol_violation(format!("Unexpected message '{ty:?}' while {state}"))
}

pub(crate) fn input_closed_before_start() -> Error {
    protocol_violation("The input was closed before the start message")
}

// --- The SDK misused the VM: the attempt fails

pub(crate) fn not_allowed_in_atomic_group(event: String) -> Error {
    Error::new(
        codes::INTERNAL,
        format!("'{event}' cannot be made while an atomic group is open: a group holds state changes, calls, cancels, signals sent and spans, and is closed before anything else is done"),
    )
}

pub(crate) fn no_open_atomic_group(event: &'static str) -> Error {
    Error::new(
        codes::INTERNAL,
        format!("'{event}' needs an open atomic group"),
    )
}

pub(crate) fn unknown_span(span: SpanId) -> Error {
    Error::new(
        codes::INTERNAL,
        format!(
            "Unknown span {}: it was never started by this attempt, or it was already ended",
            u32::from(span)
        ),
    )
}

pub(crate) fn object_state_without_key() -> Error {
    Error::new(
        codes::INTERNAL,
        "The target of this invocation has no key: there is no object state to read or write",
    )
}

pub(crate) fn limit_key_without_scope() -> Error {
    Error::new(
        codes::INTERNAL,
        "Trying to execute a request with a limit key and no scope",
    )
}

pub(crate) fn result_ids_exhausted() -> Error {
    Error::new(
        codes::INTERNAL,
        "The invocation used every result id: no further action can have a result",
    )
}
