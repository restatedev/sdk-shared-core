use crate::service_protocol::messages::{
    get_state_ephemeral_notification_message, GetStateEphemeralCommandMessage,
    GetStateEphemeralNotificationMessage,
};
use crate::service_protocol::RawMessage;
use crate::vm::context::{Context, EagerGetState};
use crate::vm::errors::{
    EphemeralStateGetWithClosedInput, EMPTY_GET_STATE_EPHEMERAL_NOTIFICATION,
    EPHEMERAL_COMMAND_DURING_REPLAY,
};
use crate::vm::transitions::{Transition, TransitionAndReturn};
use crate::vm::State;
use crate::{EphemeralCompletionId, Error, Value};
use bytes::Bytes;

pub(crate) struct EphemeralStateGet(pub(crate) String);

impl TransitionAndReturn<Context, EphemeralStateGet> for State {
    type Output = EphemeralCompletionId;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        EphemeralStateGet(key): EphemeralStateGet,
    ) -> Result<(Self, Self::Output), Error> {
        match &mut self {
            State::Processing {
                eager_state,
                ephemeral_commands,
                ..
            } => {
                match eager_state.get(&key) {
                    EagerGetState::Value(v) => {
                        let completion_id = ephemeral_commands.next_completion_id();
                        ephemeral_commands.register_ready(completion_id, Value::Success(v));
                        Ok((self, completion_id.into()))
                    }
                    EagerGetState::Empty => {
                        let completion_id = ephemeral_commands.next_completion_id();
                        ephemeral_commands.register_ready(completion_id, Value::Void);
                        Ok((self, completion_id.into()))
                    }
                    EagerGetState::Unknown => {
                        if context.input_is_closed {
                            // The runtime can't answer anymore.
                            return Err(EphemeralStateGetWithClosedInput::new(key).into());
                        }
                        let completion_id = ephemeral_commands.next_completion_id();
                        ephemeral_commands.register_in_flight(completion_id);
                        context.output.send(&GetStateEphemeralCommandMessage {
                            ephemeral_completion_id: completion_id,
                            key: Bytes::from(key),
                        });
                        Ok((self, completion_id.into()))
                    }
                }
            }
            State::Replaying { .. } => Err(EPHEMERAL_COMMAND_DURING_REPLAY),
            s => Err(s.as_unexpected_state("SysEphemeralStateGet")),
        }
    }
}

pub(crate) struct NewGetStateEphemeralNotificationMessage(pub(crate) RawMessage);

impl Transition<Context, NewGetStateEphemeralNotificationMessage> for State {
    fn transition(
        mut self,
        _: &mut Context,
        NewGetStateEphemeralNotificationMessage(msg): NewGetStateEphemeralNotificationMessage,
    ) -> Result<Self, Error> {
        match &mut self {
            State::Processing {
                ephemeral_commands, ..
            } => {
                let msg = msg.decode_to::<GetStateEphemeralNotificationMessage>(0)?;
                let value = match msg.result.ok_or(EMPTY_GET_STATE_EPHEMERAL_NOTIFICATION)? {
                    get_state_ephemeral_notification_message::Result::Void(_) => Value::Void,
                    get_state_ephemeral_notification_message::Result::Value(v) => {
                        Value::Success(v.content)
                    }
                };
                ephemeral_commands.complete(msg.ephemeral_completion_id, value);
            }
            State::Closed => {
                // Can ignore
            }
            s => return Err(s.as_unexpected_state("NewGetStateEphemeralNotificationMessage")),
        };
        Ok(self)
    }
}

pub(crate) struct TakeEphemeralNotification(pub(crate) EphemeralCompletionId);

impl TransitionAndReturn<Context, TakeEphemeralNotification> for State {
    type Output = Option<Value>;

    fn transition_and_return(
        mut self,
        _: &mut Context,
        TakeEphemeralNotification(completion_id): TakeEphemeralNotification,
    ) -> Result<(Self, Self::Output), Error> {
        match &mut self {
            State::Processing {
                ephemeral_commands, ..
            } => {
                let value = ephemeral_commands.take_notification(completion_id);
                Ok((self, value))
            }
            s => Err(s.as_unexpected_state("TakeEphemeralNotification")),
        }
    }
}
