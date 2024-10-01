use crate::service_protocol::messages::{CompletionMessage, EntryAckMessage, StartMessage};
use crate::service_protocol::{MessageType, RawMessage};
use crate::vm::context::{Context, EagerState, StartInfo};
use crate::vm::errors::{BadEagerStateKeyError, KNOWN_ENTRIES_IS_ZERO, UNEXPECTED_INPUT_MESSAGE};
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
                self.transition(context, NewStartMessage(msg.decode_to::<StartMessage>()?))
            }
            MessageType::Completion => self.transition(
                context,
                NewCompletionMessage(msg.decode_to::<CompletionMessage>()?),
            ),
            MessageType::EntryAck => self.transition(
                context,
                NewEntryAckMessage(msg.decode_to::<EntryAckMessage>()?),
            ),
            ty if ty.is_entry() => self.transition(context, NewEntryMessage(msg)),
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
            entries: Default::default(),
            async_results: Default::default(),
        })
    }
}

struct NewCompletionMessage(CompletionMessage);

impl Transition<Context, NewCompletionMessage> for State {
    fn transition(
        mut self,
        _: &mut Context,
        NewCompletionMessage(msg): NewCompletionMessage,
    ) -> Result<Self, Error> {
        // Add completion to completions buffer
        let CompletionMessage {
            entry_index,
            result,
        } = msg;
        match &mut self {
            State::WaitingReplayEntries {
                ref mut async_results,
                ..
            }
            | State::Replaying {
                ref mut async_results,
                ..
            }
            | State::Processing {
                ref mut async_results,
                ..
            } => {
                async_results.insert_unparsed_completion(
                    entry_index,
                    result.ok_or(errors::EXPECTED_COMPLETION_RESULT)?,
                )?;
            }
            State::Ended | State::Suspended => {
                // Can ignore
            }
            s => return Err(s.as_unexpected_state("NewCompletionMessage")),
        }

        Ok(self)
    }
}

struct NewEntryAckMessage(EntryAckMessage);

impl Transition<Context, NewEntryAckMessage> for State {
    fn transition(
        mut self,
        _: &mut Context,
        NewEntryAckMessage(msg): NewEntryAckMessage,
    ) -> Result<Self, Error> {
        match self {
            State::WaitingReplayEntries {
                ref mut async_results,
                ..
            }
            | State::Replaying {
                ref mut async_results,
                ..
            }
            | State::Processing {
                ref mut async_results,
                ..
            } => {
                async_results.notify_ack(msg.entry_index);
            }
            State::Ended | State::Suspended => {
                // Can ignore
            }
            s => return Err(s.as_unexpected_state("NewEntryAck")),
        }
        Ok(self)
    }
}

struct NewEntryMessage(RawMessage);

impl Transition<Context, NewEntryMessage> for State {
    fn transition(
        self,
        context: &mut Context,
        NewEntryMessage(msg): NewEntryMessage,
    ) -> Result<Self, Error> {
        match self {
            State::WaitingReplayEntries {
                mut entries,
                async_results,
            } => {
                entries.push_back(msg);

                if context.expect_start_info().entries_to_replay == entries.len() as u32 {
                    Ok(State::Replaying {
                        current_await_point: None,
                        entries,
                        async_results,
                    })
                } else {
                    Ok(State::WaitingReplayEntries {
                        entries,
                        async_results,
                    })
                }
            }
            _ => Err(errors::UNEXPECTED_ENTRY_MESSAGE),
        }
    }
}
