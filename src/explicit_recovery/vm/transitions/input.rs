use crate::explicit_recovery::vm::async_results_state;
use crate::explicit_recovery::vm::context::{Context, EagerState, StartInfo};
use crate::explicit_recovery::vm::errors::{
    empty_ephemeral_notification, input_closed_while_waiting_entries, missing_input_command,
    unexpected_message,
};
use crate::explicit_recovery::vm::transitions::{HitShutdown, Transition};
use crate::explicit_recovery::vm::State;
use crate::service_protocol::messages::{
    get_state_ephemeral_notification_message, CommandAckMessage,
    GetStateEphemeralNotificationMessage, InputCommandMessage, RunCommandMessage, StartMessage,
    WithFlags,
};
use crate::service_protocol::{MessageFlags, MessageType, NotificationId, RawMessage};
use crate::vm::errors::{
    known_entries_is_zero, unexpected_entry_message, unexpected_input_message,
    BadEagerStateKeyError,
};
use crate::{Error, Value};
use bytes::Bytes;
use std::num::NonZero;
use tracing::{debug, trace, warn};

pub(crate) struct NewMessage(pub(crate) RawMessage);

impl Transition<Context, NewMessage> for State {
    fn transition(self, context: &mut Context, NewMessage(msg): NewMessage) -> Result<Self, Error> {
        match (&self, msg.ty()) {
            (State::Closed, _) => {
                // Can ignore
                Ok(self)
            }
            (_, MessageType::Start) => {
                self.transition(context, NewStartMessage(msg.decode_to::<StartMessage>(0)?))
            }
            (State::WaitingEarlierEntries { .. }, _) => {
                self.transition(context, NewEarlierEntry(msg))
            }
            (_, ty) if ty.is_notification() => {
                self.transition(context, NewNotificationMessage(msg))
            }
            (_, MessageType::CommandAck) => self.transition(context, NewCommandAckMessage(msg)),
            (_, MessageType::ProposeRunCompletionAck) => {
                self.transition(context, NewProposeRunCompletionAckMessage)
            }
            (_, MessageType::GetStateEphemeralNotification) => {
                self.transition(context, NewGetStateEphemeralNotificationMessage(msg))
            }
            // The runtime sends commands only as the earlier attempts' entries.
            (_, ty) if ty.is_command() => Err(unexpected_entry_message()),
            _ => Err(unexpected_input_message()),
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
        if !matches!(self, State::WaitingStart) {
            return Err(self.as_unexpected_state("NewStartMessage"));
        }
        context.start_info = Some(StartInfo {
            debug_id: msg.debug_id,
            key: msg.key,
            known_entries: msg.known_entries,
            random_seed: msg.random_seed,
            scope: msg.scope,
            limit_key: msg.limit_key,
            idempotency_key: msg.idempotency_key,
        });
        let eager_state = EagerState::new(
            msg.partial_state,
            msg.state_map
                .into_iter()
                .map(|e| {
                    Ok::<(String, Bytes), BadEagerStateKeyError>((
                        String::from_utf8(e.key.to_vec()).map_err(BadEagerStateKeyError)?,
                        e.value,
                    ))
                })
                .collect::<Result<Vec<(String, Bytes)>, _>>()?,
        );

        debug!("Start invocation");

        if msg.known_entries == 0 {
            return Err(known_entries_is_zero());
        }

        Ok(State::WaitingEarlierEntries {
            received_entries: 0,
            earlier_entries: Default::default(),
            async_results: Default::default(),
            eager_state,
        })
    }
}

/// An entry the earlier attempts stored: the VM takes what `EarlierEntries` keeps, and a
/// cancellation.
struct NewEarlierEntry(RawMessage);

impl Transition<Context, NewEarlierEntry> for State {
    fn transition(
        mut self,
        context: &mut Context,
        NewEarlierEntry(msg): NewEarlierEntry,
    ) -> Result<Self, Error> {
        let State::WaitingEarlierEntries {
            ref mut received_entries,
            ref mut earlier_entries,
            ..
        } = self
        else {
            return Err(self.as_unexpected_state("NewEarlierEntry"));
        };

        let ty = msg.ty();
        if ty.is_command() {
            match ty {
                MessageType::InputCommand => {
                    earlier_entries.input = Some(
                        msg.decode_to::<InputCommandMessage>(earlier_entries.commands as i64)?,
                    );
                }
                MessageType::RunCommand => {
                    let run =
                        msg.decode_to::<RunCommandMessage>(earlier_entries.commands as i64)?;
                    earlier_entries
                        .open_runs
                        .insert(run.result_completion_id, run.name);
                }
                _ => {
                    // We just ignore in the other cases
                }
            }
            earlier_entries.commands += 1;
        } else if ty.is_notification() {
            // Either a run that completed, or a completion no one needs anymore. Signals,
            // the cancellation included, are dropped for now.
            if let NotificationId::CompletionId(completion_id) = msg.decode_as_notification()?.id {
                earlier_entries.open_runs.remove(&completion_id);
            }
        } else {
            return Err(unexpected_message(
                ty,
                "receiving the entries of the earlier attempts",
            ));
        }

        *received_entries += 1;
        if *received_entries < context.expect_start_info().known_entries {
            return Ok(self);
        }

        // That was the last one: the attempt is ready to execute.
        let State::WaitingEarlierEntries {
            earlier_entries,
            async_results,
            eager_state,
            ..
        } = self
        else {
            unreachable!("matched above");
        };

        context.journal.skip_to(earlier_entries.commands);
        let (Some(input), resolved_dangling_traces) =
            earlier_entries.get_input_and_close_dangling_traces()
        else {
            return Err(missing_input_command());
        };
        for msg in resolved_dangling_traces {
            // Asking for the ack, as the other proposals do, rather than the whole completion back.
            context
                .output
                .send(&WithFlags(&msg, &[MessageFlags::RequestedAck]));
        }

        Ok(State::Processing {
            input,
            async_results,
            eager_state,
            trace_state: Default::default(),
        })
    }
}

struct NewNotificationMessage(RawMessage);

impl Transition<Context, NewNotificationMessage> for State {
    fn transition(
        mut self,
        context: &mut Context,
        NewNotificationMessage(msg): NewNotificationMessage,
    ) -> Result<Self, Error> {
        let async_results = match &mut self {
            State::Processing { async_results, .. } => async_results,
            s => return Err(s.as_unexpected_state("NewNotificationMessage")),
        };
        let notification = msg.decode_as_notification()?;
        match notification.id {
            NotificationId::CompletionId(completion_id) => {
                let notified = NonZero::new(completion_id).is_some_and(|completion_id| {
                    async_results.notify_result(
                        async_results_state::NotificationId::CompletionId(completion_id),
                        notification.result,
                    )
                });
                if !notified {
                    if context.journal.is_earlier_completion_id(completion_id) {
                        trace!("Dropping the completion {completion_id} of an earlier attempt");
                    } else {
                        // E.g. the completion of a proposed run, which the runtime may send
                        // instead of the ack.
                        warn!("Dropping the completion {completion_id}, which no action waits for");
                    }
                }
            }
            // Signals, the cancellation included, are dropped for now.
            id => trace!("Dropping the signal {id:?}"),
        }
        Ok(self)
    }
}

struct NewCommandAckMessage(RawMessage);

impl Transition<Context, NewCommandAckMessage> for State {
    fn transition(
        mut self,
        _: &mut Context,
        NewCommandAckMessage(msg): NewCommandAckMessage,
    ) -> Result<Self, Error> {
        match &mut self {
            State::Processing { async_results, .. } => {
                let ack = msg.decode_to::<CommandAckMessage>(0)?;
                let notified = NonZero::new(ack.command_index).is_some_and(|command_index| {
                    async_results.notify_result(
                        async_results_state::NotificationId::CommandAck(command_index),
                        Value::Void,
                    )
                });
                if !notified {
                    trace!("Ignoring the ack of command {}", ack.command_index);
                }
            }
            s => return Err(s.as_unexpected_state("NewCommandAckMessage")),
        }
        Ok(self)
    }
}

struct NewProposeRunCompletionAckMessage;

impl Transition<Context, NewProposeRunCompletionAckMessage> for State {
    fn transition(
        self,
        _: &mut Context,
        _: NewProposeRunCompletionAckMessage,
    ) -> Result<Self, Error> {
        match &self {
            // The ack of a proposed run (a trace's end): nothing waits for it.
            State::Processing { .. } => {
                trace!("Ignoring the ack of a proposed run completion");
            }
            s => return Err(s.as_unexpected_state("NewProposeRunCompletionAckMessage")),
        }
        Ok(self)
    }
}

struct NewGetStateEphemeralNotificationMessage(RawMessage);

impl Transition<Context, NewGetStateEphemeralNotificationMessage> for State {
    fn transition(
        mut self,
        _: &mut Context,
        NewGetStateEphemeralNotificationMessage(msg): NewGetStateEphemeralNotificationMessage,
    ) -> Result<Self, Error> {
        match &mut self {
            State::Processing { async_results, .. } => {
                let msg = msg.decode_to::<GetStateEphemeralNotificationMessage>(0)?;
                let value = match msg.result.ok_or_else(empty_ephemeral_notification)? {
                    get_state_ephemeral_notification_message::Result::Void(_) => Value::Void,
                    get_state_ephemeral_notification_message::Result::Value(v) => {
                        Value::Success(v.content)
                    }
                };
                let notified =
                    NonZero::new(msg.ephemeral_completion_id).is_some_and(|completion_id| {
                        async_results.notify_result(
                            async_results_state::NotificationId::EphemeralCompletionId(
                                completion_id,
                            ),
                            value,
                        )
                    });
                if !notified {
                    warn!(
                        "Ignoring the ephemeral notification {}, which no action waits for",
                        msg.ephemeral_completion_id
                    );
                }
            }
            s => return Err(s.as_unexpected_state("NewGetStateEphemeralNotificationMessage")),
        }
        Ok(self)
    }
}

pub(crate) struct NotifyInputClosed;

impl Transition<Context, NotifyInputClosed> for State {
    fn transition(self, context: &mut Context, _: NotifyInputClosed) -> Result<Self, Error> {
        match self {
            State::WaitingStart | State::WaitingEarlierEntries { .. } => {
                Err(input_closed_while_waiting_entries())
            }
            // The runtime shut the attempt down.
            State::Processing { .. } => self.transition(context, HitShutdown),
            State::Closed => Ok(self),
        }
    }
}
