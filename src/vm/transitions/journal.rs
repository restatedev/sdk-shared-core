use crate::retries::NextRetry;
use crate::service_protocol::messages::{
    get_eager_state_command_message, propose_run_completion_message, CommandMessageHeaderEq,
    GetEagerStateCommandMessage, GetEagerStateKeysCommandMessage, GetLazyStateCommandMessage,
    GetLazyStateKeysCommandMessage, InputCommandMessage, NamedCommandMessage,
    ProposeRunCompletionMessage, RestateMessage, RunCommandMessage, StateKeys, Void,
};
use crate::service_protocol::{
    messages, CompletionId, MessageType, Notification, NotificationId, NotificationResult,
};
use crate::vm::context::{Context, EagerGetState, EagerGetStateKeys, RunState};
use crate::vm::errors::{
    EmptyGetEagerState, EmptyGetEagerStateKeys, EntryMismatchError, UnavailableEntryError,
    UnexpectedGetState, UnexpectedGetStateKeys, UnexpectedStateError,
};
use crate::vm::transitions::{Transition, TransitionAndReturn};
use crate::vm::State;
use crate::{EntryRetryInfo, Error, Header, Input, NotificationHandle, RetryPolicy, RunExitResult};
use bytes::Bytes;
use std::fmt;
use tracing::trace;

pub(crate) struct PopJournalEntry<M>(pub(crate) &'static str, pub(crate) M);

impl<M: RestateMessage + CommandMessageHeaderEq + Clone>
    TransitionAndReturn<Context, PopJournalEntry<M>> for State
{
    type Output = M;

    fn transition_and_return(
        self,
        _: &mut Context,
        PopJournalEntry(sys_name, expected): PopJournalEntry<M>,
    ) -> Result<(Self, Self::Output), Error> {
        match self {
            State::Replaying {
                mut entries,
                run_state,
                async_results,
            } => {
                let actual = entries
                    .pop_front()
                    .ok_or(UnavailableEntryError::new(M::ty()))?
                    .decode_to::<M>()?;
                let new_state = if entries.is_empty() {
                    State::Processing {
                        processing_first_entry: true,
                        run_state: RunState::default(),
                        async_results,
                    }
                } else {
                    State::Replaying {
                        entries,
                        run_state,
                        async_results,
                    }
                };

                check_entry_header_match(&actual, &expected)?;

                Ok((new_state, actual))
            }
            s => Err(UnexpectedStateError::new(s.into(), sys_name).into()),
        }
    }
}

struct PopOrWriteJournalEntry<M>(&'static str, M);

impl<M: RestateMessage + CommandMessageHeaderEq + Clone>
    TransitionAndReturn<Context, PopOrWriteJournalEntry<M>> for State
{
    type Output = M;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        PopOrWriteJournalEntry(sys_name, expected): PopOrWriteJournalEntry<M>,
    ) -> Result<(Self, Self::Output), Error> {
        match self {
            State::Processing {
                ref mut processing_first_entry,
                ..
            } => {
                *processing_first_entry = false;
                context.output.send(&expected);
                Ok((self, expected))
            }
            s => s.transition_and_return(context, PopJournalEntry(sys_name, expected)),
        }
    }
}

pub(crate) struct SysInput;

impl TransitionAndReturn<Context, SysInput> for State {
    type Output = Input;

    fn transition_and_return(
        self,
        context: &mut Context,
        _: SysInput,
    ) -> Result<(Self, Self::Output), Error> {
        context.journal.transition(&InputCommandMessage::default());
        let (s, msg) = TransitionAndReturn::transition_and_return(
            self,
            context,
            PopJournalEntry("SysInput", InputCommandMessage::default()),
        )?;
        let start_info = context.expect_start_info();

        Ok((
            s,
            Input {
                invocation_id: start_info.debug_id.clone(),
                random_seed: compute_random_seed(&start_info.id),
                key: start_info.key.clone(),
                headers: msg.headers.into_iter().map(Header::from).collect(),
                input: msg.value.map(|v| v.content).unwrap_or_default(),
            },
        ))
    }
}

#[cfg(feature = "sha2_random_seed")]
fn compute_random_seed(id: &[u8]) -> u64 {
    use bytes::Buf;
    use sha2::{Digest, Sha256};

    let id_hash = Sha256::digest(id);
    let mut b = id_hash.as_slice();
    b.get_u64()
}

#[cfg(not(feature = "sha2_random_seed"))]
fn compute_random_seed(id: &[u8]) -> u64 {
    use std::hash::{DefaultHasher, Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    id.hash(&mut hasher);
    hasher.finish()
}

pub(crate) struct SysNonCompletableEntry<M>(pub(crate) &'static str, pub(crate) M);

impl<M: RestateMessage + CommandMessageHeaderEq + NamedCommandMessage + Clone>
    Transition<Context, SysNonCompletableEntry<M>> for State
{
    fn transition(
        self,
        context: &mut Context,
        SysNonCompletableEntry(sys_name, expected): SysNonCompletableEntry<M>,
    ) -> Result<Self, Error> {
        context.journal.transition(&expected);
        let (s, _) =
            self.transition_and_return(context, PopOrWriteJournalEntry(sys_name, expected))?;
        Ok(s)
    }
}

pub(crate) struct SysNonCompletableEntryWithCompletion<M>(
    pub(crate) &'static str,
    pub(crate) M,
    pub(crate) Notification,
);

impl<M: RestateMessage + CommandMessageHeaderEq + NamedCommandMessage + Clone>
    TransitionAndReturn<Context, SysNonCompletableEntryWithCompletion<M>> for State
{
    type Output = NotificationHandle;

    fn transition_and_return(
        self,
        context: &mut Context,
        SysNonCompletableEntryWithCompletion(sys_name, expected, notification): SysNonCompletableEntryWithCompletion<M>,
    ) -> Result<(Self, Self::Output), Error> {
        context.journal.transition(&expected);
        let (mut s, _) =
            self.transition_and_return(context, PopOrWriteJournalEntry(sys_name, expected))?;
        match s {
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
                let handle = async_results.create_handle_mapping(notification.id.clone());
                async_results.insert_ready(notification);
                Ok((s, handle))
            }
            s => Err(UnexpectedStateError::new(s.into(), sys_name).into()),
        }
    }
}

pub(crate) struct SysSimpleCompletableEntry<M>(
    pub(crate) &'static str,
    pub(crate) M,
    pub(crate) CompletionId,
);

impl<M: RestateMessage + CommandMessageHeaderEq + NamedCommandMessage + Clone>
    TransitionAndReturn<Context, SysSimpleCompletableEntry<M>> for State
{
    type Output = NotificationHandle;

    fn transition_and_return(
        self,
        context: &mut Context,
        SysSimpleCompletableEntry(sys_name, expected, completion_id): SysSimpleCompletableEntry<M>,
    ) -> Result<(Self, Self::Output), Error> {
        let (s, handles) = TransitionAndReturn::transition_and_return(
            self,
            context,
            SysCompletableEntryWithMultipleCompletions(sys_name, expected, vec![completion_id]),
        )?;
        Ok((s, handles[0]))
    }
}

pub(crate) struct SysCompletableEntryWithMultipleCompletions<M>(
    pub(crate) &'static str,
    pub(crate) M,
    pub(crate) Vec<CompletionId>,
);

impl<M: RestateMessage + CommandMessageHeaderEq + NamedCommandMessage + Clone>
    TransitionAndReturn<Context, SysCompletableEntryWithMultipleCompletions<M>> for State
{
    type Output = Vec<NotificationHandle>;

    fn transition_and_return(
        self,
        context: &mut Context,
        SysCompletableEntryWithMultipleCompletions(sys_name, expected, completion_ids): SysCompletableEntryWithMultipleCompletions<M>,
    ) -> Result<(Self, Self::Output), Error> {
        context.journal.transition(&expected);
        let (mut s, _) = TransitionAndReturn::transition_and_return(
            self,
            context,
            PopOrWriteJournalEntry(sys_name, expected),
        )?;

        match s {
            State::Replaying {
                ref mut async_results,
                ..
            }
            | State::Processing {
                ref mut async_results,
                ..
            } => {
                // Create mapping for all the necessary notification ids
                let mut notification_handles = Vec::with_capacity(completion_ids.len());
                for completion_id in completion_ids {
                    notification_handles.push(
                        async_results
                            .create_handle_mapping(NotificationId::CompletionId(completion_id)),
                    )
                }

                Ok((s, notification_handles))
            }
            s => Err(UnexpectedStateError::new(s.into(), sys_name).into()),
        }
    }
}

pub(crate) struct CreateSignalHandle(pub(crate) &'static str, pub(crate) NotificationId);

impl TransitionAndReturn<Context, CreateSignalHandle> for State {
    type Output = NotificationHandle;

    fn transition_and_return(
        mut self,
        _: &mut Context,
        CreateSignalHandle(sys_name, notification_id): CreateSignalHandle,
    ) -> Result<(Self, Self::Output), Error> {
        match self {
            State::Replaying {
                ref mut async_results,
                ..
            }
            | State::Processing {
                ref mut async_results,
                ..
            } => {
                // Create mapping for the notification id
                let handle = async_results.create_handle_mapping(notification_id);

                Ok((self, handle))
            }
            s => Err(UnexpectedStateError::new(s.into(), sys_name).into()),
        }
    }
}

pub(crate) struct SysStateGet(pub(crate) String);

impl TransitionAndReturn<Context, SysStateGet> for State {
    type Output = NotificationHandle;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        SysStateGet(key): SysStateGet,
    ) -> Result<(Self, Self::Output), Error> {
        let completion_id = context.journal.next_completion_notification_id();

        match self {
            State::Processing {
                ref mut processing_first_entry,
                ref mut async_results,
                ..
            } => {
                *processing_first_entry = false;

                // Let's look into the eager_state
                let result = match context.eager_state.get(&key) {
                    EagerGetState::Unknown => None,
                    EagerGetState::Empty => Some((
                        get_eager_state_command_message::Result::Void(Void::default()),
                        NotificationResult::Void(Void::default()),
                    )),
                    EagerGetState::Value(v) => Some((
                        get_eager_state_command_message::Result::Value(v.clone().into()),
                        NotificationResult::Value(v.clone().into()),
                    )),
                };

                if let Some((get_state_result, notification_result)) = result {
                    // Eager state case, we're good let's prepare the ready notification and send the get eager state entry
                    let new_entry = GetEagerStateCommandMessage {
                        key: Bytes::from(key),
                        result: Some(get_state_result),
                        ..Default::default()
                    };
                    let new_notification = Notification {
                        id: NotificationId::CompletionId(completion_id),
                        result: notification_result,
                    };
                    let handle = async_results.create_handle_mapping(new_notification.id.clone());

                    async_results.insert_ready(new_notification);
                    context.journal.transition(&new_entry);
                    context.output.send(&new_entry);

                    Ok((self, handle))
                } else {
                    let new_entry = GetLazyStateCommandMessage {
                        key: Bytes::from(key),
                        result_completion_id: completion_id,
                        ..Default::default()
                    };
                    let handle = async_results
                        .create_handle_mapping(NotificationId::CompletionId(completion_id));

                    context.journal.transition(&new_entry);
                    context.output.send(&new_entry);

                    Ok((self, handle))
                }
            }
            State::Replaying {
                mut entries,
                mut async_results,
                run_state,
            } => {
                context
                    .journal
                    .transition(&GetEagerStateCommandMessage::default());
                let handle = async_results
                    .create_handle_mapping(NotificationId::CompletionId(completion_id));

                let actual = entries
                    .pop_front()
                    .ok_or(UnavailableEntryError::new(GetLazyStateCommandMessage::ty()))?;

                match actual.ty() {
                    MessageType::GetEagerStateCommand => {
                        let get_eager_state_command =
                            actual.decode_to::<GetEagerStateCommandMessage>()?;
                        check_entry_header_match(
                            &get_eager_state_command,
                            &GetEagerStateCommandMessage {
                                key: key.into_bytes().into(),
                                result: get_eager_state_command.result.clone(),
                                name: "".to_string(),
                            },
                        )?;

                        let notification_result =
                            match get_eager_state_command.result.ok_or(EmptyGetEagerState)? {
                                get_eager_state_command_message::Result::Void(v) => {
                                    NotificationResult::Void(v)
                                }
                                get_eager_state_command_message::Result::Value(v) => {
                                    NotificationResult::Value(v)
                                }
                            };

                        async_results.insert_ready(Notification {
                            id: NotificationId::CompletionId(completion_id),
                            result: notification_result,
                        });
                    }
                    MessageType::GetLazyStateCommand => {
                        let get_lazy_state_command =
                            actual.decode_to::<GetLazyStateCommandMessage>()?;
                        check_entry_header_match(
                            &get_lazy_state_command,
                            &GetLazyStateCommandMessage {
                                key: key.into_bytes().into(),
                                result_completion_id: completion_id,
                                name: "".to_string(),
                            },
                        )?;
                    }
                    message_type => {
                        return Err(UnexpectedGetState {
                            actual: message_type,
                        }
                        .into())
                    }
                }

                let new_state = if entries.is_empty() {
                    State::Processing {
                        processing_first_entry: true,
                        run_state: RunState::default(),
                        async_results,
                    }
                } else {
                    State::Replaying {
                        entries,
                        run_state,
                        async_results,
                    }
                };

                Ok((new_state, handle))
            }
            s => Err(UnexpectedStateError::new(s.into(), "SysStateGet").into()),
        }
    }
}

pub(crate) struct SysStateGetKeys;

impl TransitionAndReturn<Context, SysStateGetKeys> for State {
    type Output = NotificationHandle;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        _: SysStateGetKeys,
    ) -> Result<(Self, Self::Output), Error> {
        let completion_id = context.journal.next_completion_notification_id();

        match self {
            State::Processing {
                ref mut processing_first_entry,
                ref mut async_results,
                ..
            } => {
                *processing_first_entry = false;

                // Let's look into the eager_state
                let result = match context.eager_state.get_keys() {
                    EagerGetStateKeys::Unknown => None,
                    EagerGetStateKeys::Keys(keys) => {
                        let state_keys = StateKeys {
                            keys: keys.into_iter().map(Bytes::from).collect(),
                        };
                        Some((
                            state_keys.clone(),
                            NotificationResult::StateKeys(state_keys),
                        ))
                    }
                };

                if let Some((get_state_result, notification_result)) = result {
                    // Eager state case, we're good let's prepare the ready notification and send the get eager state entry
                    let new_entry = GetEagerStateKeysCommandMessage {
                        value: Some(get_state_result),
                        ..Default::default()
                    };
                    let new_notification = Notification {
                        id: NotificationId::CompletionId(completion_id),
                        result: notification_result,
                    };
                    let handle = async_results.create_handle_mapping(new_notification.id.clone());

                    async_results.insert_ready(new_notification);
                    context.journal.transition(&new_entry);
                    context.output.send(&new_entry);

                    Ok((self, handle))
                } else {
                    let new_entry = GetLazyStateKeysCommandMessage {
                        result_completion_id: completion_id,
                        ..Default::default()
                    };
                    let handle = async_results
                        .create_handle_mapping(NotificationId::CompletionId(completion_id));

                    context.journal.transition(&new_entry);
                    context.output.send(&new_entry);

                    Ok((self, handle))
                }
            }
            State::Replaying {
                mut entries,
                mut async_results,
                run_state,
            } => {
                context
                    .journal
                    .transition(&GetEagerStateKeysCommandMessage::default());
                let handle = async_results
                    .create_handle_mapping(NotificationId::CompletionId(completion_id));

                let actual = entries.pop_front().ok_or(UnavailableEntryError::new(
                    GetLazyStateKeysCommandMessage::ty(),
                ))?;

                match actual.ty() {
                    MessageType::GetEagerStateKeysCommand => {
                        let get_eager_state_command =
                            actual.decode_to::<GetEagerStateKeysCommandMessage>()?;
                        check_entry_header_match(
                            &get_eager_state_command,
                            &GetEagerStateKeysCommandMessage {
                                value: get_eager_state_command.value.clone(),
                                name: "".to_string(),
                            },
                        )?;

                        let notification_result = NotificationResult::StateKeys(
                            get_eager_state_command
                                .value
                                .ok_or(EmptyGetEagerStateKeys)?,
                        );

                        async_results.insert_ready(Notification {
                            id: NotificationId::CompletionId(completion_id),
                            result: notification_result,
                        });
                    }
                    MessageType::GetLazyStateKeysCommand => {
                        let get_lazy_state_command =
                            actual.decode_to::<GetLazyStateKeysCommandMessage>()?;
                        check_entry_header_match(
                            &get_lazy_state_command,
                            &GetLazyStateKeysCommandMessage {
                                result_completion_id: completion_id,
                                name: "".to_string(),
                            },
                        )?;
                    }
                    message_type => {
                        return Err(UnexpectedGetStateKeys {
                            actual: message_type,
                        }
                        .into())
                    }
                }

                let new_state = if entries.is_empty() {
                    State::Processing {
                        processing_first_entry: true,
                        run_state: RunState::default(),
                        async_results,
                    }
                } else {
                    State::Replaying {
                        entries,
                        run_state,
                        async_results,
                    }
                };

                Ok((new_state, handle))
            }
            s => Err(UnexpectedStateError::new(s.into(), "SysStateGet").into()),
        }
    }
}

pub(crate) struct SysRun(pub(crate) String);

impl TransitionAndReturn<Context, SysRun> for State {
    type Output = NotificationHandle;

    fn transition_and_return(
        self,
        context: &mut Context,
        SysRun(name): SysRun,
    ) -> Result<(Self, Self::Output), Error> {
        let result_completion_id = context.journal.next_completion_notification_id();
        let expected = RunCommandMessage {
            name: name.clone(),
            result_completion_id,
        };

        let (mut s, handle) = TransitionAndReturn::transition_and_return(
            self,
            context,
            SysSimpleCompletableEntry("SysRun", expected, result_completion_id),
        )?;

        let notification_id = NotificationId::CompletionId(result_completion_id);
        let mut needs_execution = true;
        if let State::Replaying { async_results, .. } = &mut s {
            // If we're replying,
            // we need to check whether there is a completion already,
            // otherwise enqueue it to execute it.
            if async_results.non_deterministic_find_id(&notification_id) {
                trace!(
                    "Found notification for {handle:?} with id {notification_id:?} while replaying, the run closure won't be executed."
                );
                needs_execution = false;
            }
        }
        if needs_execution {
            trace!(
                "Run notification for {handle:?} with id {notification_id:?} not found while replaying, \
                 so we enqueue the run to be executed later"
            );
            match &mut s {
                State::Replaying { run_state, .. } | State::Processing { run_state, .. } => {
                    run_state.insert_run_to_execute(handle)
                }
                _ => {}
            };
        }

        Ok((s, handle))
    }
}

pub(crate) struct ProposeRunCompletion(
    pub(crate) NotificationHandle,
    pub(crate) RunExitResult,
    pub(crate) RetryPolicy,
);

impl Transition<Context, ProposeRunCompletion> for State {
    fn transition(
        mut self,
        context: &mut Context,
        ProposeRunCompletion(notification_handle, run_exit_result, retry_policy): ProposeRunCompletion,
    ) -> Result<Self, Error> {
        match self {
            State::Processing {
                ref mut async_results,
                ref mut run_state,
                processing_first_entry,
            } => {
                let notification_id =
                    async_results.must_resolve_notification_handle(&notification_handle);
                run_state.notify_executed(notification_handle);

                let value = match run_exit_result {
                    RunExitResult::Success(s) => propose_run_completion_message::Result::Value(s),
                    RunExitResult::TerminalFailure(f) => {
                        propose_run_completion_message::Result::Failure(f.into())
                    }
                    RunExitResult::RetryableFailure {
                        error,
                        attempt_duration,
                    } => {
                        let mut retry_info = if processing_first_entry {
                            context.infer_entry_retry_info()
                        } else {
                            EntryRetryInfo::default()
                        };
                        retry_info.retry_count += 1;
                        retry_info.retry_loop_duration += attempt_duration;

                        match retry_policy.next_retry(retry_info) {
                            NextRetry::Retry(next_retry_interval) => {
                                // We need to retry!
                                context.next_retry_delay = next_retry_interval;
                                return Err(Error::new(error.code, error.message));
                            }
                            NextRetry::DoNotRetry => {
                                // We don't retry, but convert the retryable error to actual error
                                propose_run_completion_message::Result::Failure(messages::Failure {
                                    code: error.code.into(),
                                    message: error.message.to_string(),
                                })
                            }
                        }
                    }
                };

                let result_completion_id = match notification_id {
                    NotificationId::CompletionId(cid) => cid,
                    nid => {
                        panic!("NotificationId for run should be a completion id, but was {nid:?}")
                    }
                };
                let expected = ProposeRunCompletionMessage {
                    result_completion_id,
                    result: Some(value),
                };
                context.output.send(&expected);

                Ok(self)
            }
            s => {
                trace!("Going to ignore proposed completion for run with handle {notification_handle:?}, because state is {}", <&'static str>::from(&s));
                Ok(s)
            }
        }
    }
}

fn check_entry_header_match<M: CommandMessageHeaderEq + Clone + fmt::Debug>(
    actual: &M,
    expected: &M,
) -> Result<(), Error> {
    if !actual.header_eq(expected) {
        return Err(EntryMismatchError::new(actual.clone(), expected.clone()).into());
    }

    Ok(())
}
