use crate::explicit_recovery::vm::async_results_state::Notified;
use crate::explicit_recovery::vm::context::{Context, StartInfo};
use crate::explicit_recovery::vm::errors::{
    input_closed_before_start, protocol_violation, unexpected_message,
};
use crate::explicit_recovery::vm::local_state::LocalState;
use crate::explicit_recovery::vm::transitions::{HitShutdown, Transition};
use crate::explicit_recovery::vm::{Attempt, State};
use crate::explicit_recovery::StateScope;
use crate::service_protocol::explicit_recovery::messages::{
    self, result_notification_message, BuiltInSignal, RestateMessage, ResultNotificationMessage,
    SignalNotificationMessage, StartMessage, Void,
};
use crate::service_protocol::explicit_recovery::{MessageType, RawMessage};
use crate::vm::errors::BadEagerStateKeyError;
use crate::Error;
use std::num::NonZero;
use tracing::debug;

/// The payload of `msg` as an `M`. The runtime sent it: what cannot be decoded violates the protocol.
fn decode<M: RestateMessage>(msg: RawMessage) -> Result<M, Error> {
    M::decode(msg.into_payload())
        .map_err(|e| protocol_violation(format!("Cannot decode '{:?}': {e}", M::ty())))
}

pub(crate) struct NewMessage(pub(crate) RawMessage);

impl Transition<Context, NewMessage> for State {
    fn transition(self, context: &mut Context, NewMessage(msg): NewMessage) -> Result<Self, Error> {
        if matches!(self, State::Closed) {
            // The handler ended: what still comes is ignored.
            return Ok(self);
        }
        let header = msg.raw_header();
        if header.flags() != 0 {
            return Err(protocol_violation(format!(
                "The reserved bits of the header of '{:?}' are set: {:#06x}",
                header.ty(),
                header.flags()
            )));
        }
        match (&self, header.ty()) {
            (State::WaitingStart, MessageType::Start) => {
                self.transition(context, NewStartMessage(decode(msg)?))
            }
            (State::Running(_) | State::InAtomicGroup(..), MessageType::ResultNotification) => {
                self.transition(context, NewResultNotification(decode(msg)?))
            }
            (State::Running(_) | State::InAtomicGroup(..), MessageType::SignalNotification) => {
                self.transition(context, NewSignalNotification(decode(msg)?))
            }
            // A second start, a notification before the start, or a message only the SDK sends.
            (state, ty) => Err(unexpected_message(ty, state.into())),
        }
    }
}

struct NewStartMessage(StartMessage);

impl Transition<Context, NewStartMessage> for State {
    fn transition(
        self,
        context: &mut Context,
        NewStartMessage(msg): NewStartMessage,
    ) -> Result<Self, Error> {
        let next_result_id = NonZero::new(msg.next_result_id)
            .ok_or_else(|| protocol_violation("StartMessage.next_result_id is 0"))?;
        context.ids.start_at(next_result_id);

        let preloaded = msg
            .state_map
            .into_iter()
            .map(|entry| {
                let scope = match messages::StateScope::try_from(entry.scope) {
                    Ok(messages::StateScope::Object) => StateScope::Object,
                    Ok(messages::StateScope::Invocation) => StateScope::Invocation,
                    Ok(messages::StateScope::Unspecified) | Err(_) => {
                        return Err(protocol_violation(format!(
                            "StartMessage.state_map names the state scope {}, which is not a scope of this version",
                            entry.scope
                        )))
                    }
                };
                let key = String::from_utf8(entry.key.to_vec()).map_err(BadEagerStateKeyError)?;
                // Unset: the key is known to have no value.
                Ok((scope, key, entry.value.map(|value| value.content)))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let local_state = LocalState::new(
            preloaded,
            msg.is_object_scope_partial_state,
            msg.is_invocation_scope_partial_state,
        );

        context.start_info = Some(StartInfo {
            debug_id: msg.debug_id,
            key: msg.key,
            headers: msg.headers,
            input: msg.input.map(|value| value.content).unwrap_or_default(),
            scope: msg.scope,
            limit_key: msg.limit_key,
            idempotency_key: msg.idempotency_key,
            attempt: msg.attempt,
            cancelled: msg.cancelled,
            features: msg.features,
        });

        debug!("Start of attempt {}", msg.attempt);

        // There is nothing else to wait for: the handler starts.
        Ok(State::Running(Attempt {
            async_results: Default::default(),
            local_state,
            spans: Default::default(),
        }))
    }
}

struct NewResultNotification(ResultNotificationMessage);

impl Transition<Context, NewResultNotification> for State {
    fn transition(
        mut self,
        context: &mut Context,
        NewResultNotification(msg): NewResultNotification,
    ) -> Result<Self, Error> {
        let (State::Running(attempt) | State::InAtomicGroup(attempt, _)) = &mut self else {
            return Err(self.as_unexpected_state("NewResultNotification"));
        };
        // A oneof that carries a payload with nothing set reads as void.
        let result = msg
            .result
            .unwrap_or(result_notification_message::Result::Void(Void {}));
        let result_id = NonZero::new(msg.result_id)
            .ok_or_else(|| protocol_violation("ResultNotificationMessage.result_id is 0"))?;
        match attempt.async_results.notify_result(result_id, result) {
            Notified::Ok => {}
            Notified::UnknownResultId if context.ids.is_earlier_result_id(result_id.get()) => {
                // The runtime drops the results of earlier attempts' actions (G7).
                return Err(protocol_violation(format!(
                    "Result {result_id}: the id is below this attempt's next_result_id, so it belongs to an earlier attempt"
                )));
            }
            Notified::UnknownResultId => {
                return Err(protocol_violation(format!(
                    "Result {result_id}: this attempt sent no action with that result id, or the result came twice"
                )))
            }
            Notified::UnexpectedVariant(expect) => {
                return Err(protocol_violation(format!(
                    "Result {result_id} is of a variant its action does not allow, expected {expect:?}"
                )))
            }
        }
        Ok(self)
    }
}

struct NewSignalNotification(SignalNotificationMessage);

impl Transition<Context, NewSignalNotification> for State {
    fn transition(
        mut self,
        context: &mut Context,
        NewSignalNotification(msg): NewSignalNotification,
    ) -> Result<Self, Error> {
        let (State::Running(attempt) | State::InAtomicGroup(attempt, _)) = &mut self else {
            return Err(self.as_unexpected_state("NewSignalNotification"));
        };
        match BuiltInSignal::try_from(msg.signal_id) {
            // An attempt learns of the cancellation once: from the start, or from this.
            Ok(BuiltInSignal::Cancel)
                if context.expect_start_info().cancelled || context.cancel_received =>
            {
                return Err(protocol_violation(
                    "A second cancel signal: this attempt already knew of the cancellation",
                ));
            }
            // Handed out as a notification, so that it counts as processed only once the SDK
            // took it.
            Ok(BuiltInSignal::Cancel) => {
                context.cancel_received = true;
                attempt.async_results.notify_cancel()
            }
            _ => {
                return Err(protocol_violation(format!(
                    "The built-in signal {} is not one of this version",
                    msg.signal_id
                )))
            }
        }
        Ok(self)
    }
}

pub(crate) struct NotifyInputClosed;

impl Transition<Context, NotifyInputClosed> for State {
    fn transition(self, context: &mut Context, _: NotifyInputClosed) -> Result<Self, Error> {
        match self {
            State::WaitingStart => Err(input_closed_before_start()),
            // The runtime shut the attempt down.
            State::Running(_) | State::InAtomicGroup(..) => self.transition(context, HitShutdown),
            State::Closed => Ok(self),
        }
    }
}
