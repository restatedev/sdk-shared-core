use crate::service_protocol::messages::StartMessage;
use crate::service_protocol::{MessageType, RawMessage};
use crate::vm::context::{Context, EagerState, StartInfo};
use crate::vm::errors::{
    BadEagerStateKeyError, INPUT_CLOSED_WHILE_WAITING_ENTRIES, KNOWN_ENTRIES_IS_ZERO,
    UNEXPECTED_INPUT_MESSAGE,
};
use crate::vm::transitions::Transition;
use crate::vm::{errors, State};
use crate::Error;
use bytes::Bytes;
use tracing::debug;

pub(crate) struct NewMessage(pub(crate) RawMessage);

impl Transition<Context, NewMessage> for State {
    fn transition(self, context: &mut Context, NewMessage(msg): NewMessage) -> Result<Self, Error> {
        match msg.ty() {
            MessageType::Start => {
                self.transition(context, NewStartMessage(msg.decode_to::<StartMessage>(0)?))
            }
            ty if ty.is_command() => self.transition(context, NewCommandMessage(msg)),
            ty if ty.is_notification() => self.transition(context, NewNotificationMessage(msg)),
            _ => Err(UNEXPECTED_INPUT_MESSAGE)?,
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
        context.start_info = Some(StartInfo {
            id: msg.id,
            debug_id: msg.debug_id,
            key: msg.key,
            entries_to_replay: msg.known_entries,
            retry_count_since_last_stored_entry: msg.retry_count_since_last_stored_entry,
            duration_since_last_stored_entry: msg.duration_since_last_stored_entry,
        });
        context.eager_state = EagerState::new(
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
            return Err(KNOWN_ENTRIES_IS_ZERO);
        }

        Ok(State::WaitingReplayEntries {
            received_entries: 0,
            commands: Default::default(),
            async_results: Default::default(),
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
        match &mut self {
            State::WaitingReplayEntries { async_results, .. }
            | State::Replaying { async_results, .. }
            | State::Processing { async_results, .. } => {
                async_results.enqueue(msg.decode_as_notification()?);
            }
            State::Closed => {
                // Can ignore
            }
            s => return Err(s.as_unexpected_state("NewNotificationMessage")),
        };

        self.transition(context, PostReceiveEntry)
    }
}

struct NewCommandMessage(RawMessage);

impl Transition<Context, NewCommandMessage> for State {
    fn transition(
        mut self,
        context: &mut Context,
        NewCommandMessage(msg): NewCommandMessage,
    ) -> Result<Self, Error> {
        match &mut self {
            State::WaitingReplayEntries { commands, .. } => {
                commands.push_back(msg);
            }
            _ => return Err(errors::UNEXPECTED_ENTRY_MESSAGE),
        };

        self.transition(context, PostReceiveEntry)
    }
}

struct PostReceiveEntry;

impl Transition<Context, PostReceiveEntry> for State {
    fn transition(self, context: &mut Context, _: PostReceiveEntry) -> Result<Self, Error> {
        match self {
            State::WaitingReplayEntries {
                mut received_entries,
                commands,
                async_results,
            } => {
                received_entries += 1;
                if context.expect_start_info().entries_to_replay == received_entries {
                    Ok(State::Replaying {
                        commands,
                        run_state: Default::default(),
                        async_results,
                    })
                } else {
                    Ok(State::WaitingReplayEntries {
                        received_entries,
                        commands,
                        async_results,
                    })
                }
            }
            s => Ok(s),
        }
    }
}

pub(crate) struct NotifyInputClosed;

impl Transition<Context, NotifyInputClosed> for State {
    fn transition(self, _: &mut Context, _: NotifyInputClosed) -> Result<Self, Error> {
        match self {
            State::WaitingStart | State::WaitingReplayEntries { .. } => {
                Err(INPUT_CLOSED_WHILE_WAITING_ENTRIES)
            }
            _ => Ok(self),
        }
    }
}
