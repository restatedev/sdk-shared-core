use crate::service_protocol::messages;
use crate::service_protocol::messages::{
    run_entry_message, CompletableEntryMessage, EntryMessage, EntryMessageHeaderEq,
    InputEntryMessage, RestateMessage, RunEntryMessage, WriteableRestateMessage,
};
use crate::vm::context::{Context, RunState};
use crate::vm::errors::{
    EntryMismatchError, UnavailableEntryError, UnexpectedStateError, INSIDE_RUN,
    INVOKED_RUN_EXIT_WITHOUT_ENTER, UNEXPECTED_NONE_RUN_RESULT,
};
use crate::vm::transitions::{Transition, TransitionAndReturn};
use crate::vm::State;
use crate::{AsyncResultHandle, Header, Input, NonEmptyValue, RunEnterResult, VMError};
use bytes::Buf;
use sha2::{Digest, Sha256};
use std::{fmt, mem};

impl State {
    fn check_side_effect_guard(&self) -> Result<(), VMError> {
        if let State::Processing { run_state, .. } = self {
            if run_state.is_running() {
                return Err(INSIDE_RUN);
            }
        }
        Ok(())
    }
}

struct PopJournalEntry<M>(&'static str, M);

impl<M: RestateMessage + EntryMessageHeaderEq + EntryMessage + Clone>
    TransitionAndReturn<Context, PopJournalEntry<M>> for State
{
    type Output = M;

    fn transition_and_return(
        self,
        context: &mut Context,
        PopJournalEntry(sys_name, expected): PopJournalEntry<M>,
    ) -> Result<(Self, Self::Output), VMError> {
        match self {
            State::Replaying {
                mut entries,
                current_await_point,
                mut async_results,
            } => {
                let actual = entries
                    .pop_front()
                    .ok_or(UnavailableEntryError::new(M::ty()))?
                    .decode_to::<M>()?;
                let new_state = if entries.is_empty() {
                    async_results.notify_ack(context.journal.expect_index());
                    State::Processing {
                        run_state: RunState::NotRunning,
                        current_await_point,
                        async_results,
                    }
                } else {
                    State::Replaying {
                        current_await_point,
                        entries,
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

impl<M: RestateMessage + EntryMessageHeaderEq + EntryMessage + Clone + WriteableRestateMessage>
    TransitionAndReturn<Context, PopOrWriteJournalEntry<M>> for State
{
    type Output = M;

    fn transition_and_return(
        self,
        context: &mut Context,
        PopOrWriteJournalEntry(sys_name, expected): PopOrWriteJournalEntry<M>,
    ) -> Result<(Self, Self::Output), VMError> {
        match self {
            State::Processing { .. } => {
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
    ) -> Result<(Self, Self::Output), VMError> {
        context.journal.transition(&InputEntryMessage::default());
        self.check_side_effect_guard()?;
        let (s, msg) = TransitionAndReturn::transition_and_return(
            self,
            context,
            PopJournalEntry("SysInput", InputEntryMessage::default()),
        )?;
        let start_info = context.expect_start_info();

        Ok((
            s,
            Input {
                invocation_id: start_info.debug_id.clone(),
                random_seed: compute_random_seed(&start_info.id),
                key: start_info.key.clone(),
                headers: msg
                    .headers
                    .into_iter()
                    .map(|messages::Header { key, value }| Header {
                        key: key.into(),
                        value: value.into(),
                    })
                    .collect(),
                input: msg.value.to_vec(),
            },
        ))
    }
}

fn compute_random_seed(id: &[u8]) -> u64 {
    let id_hash = Sha256::digest(id);
    let mut b = id_hash.as_slice();
    b.get_u64()
}

pub(crate) struct SysNonCompletableEntry<M>(pub(crate) &'static str, pub(crate) M);

impl<M: RestateMessage + EntryMessageHeaderEq + EntryMessage + Clone + WriteableRestateMessage>
    Transition<Context, SysNonCompletableEntry<M>> for State
{
    fn transition(
        self,
        context: &mut Context,
        SysNonCompletableEntry(sys_name, expected): SysNonCompletableEntry<M>,
    ) -> Result<Self, VMError> {
        context.journal.transition(&expected);
        self.check_side_effect_guard()?;
        let (s, _) =
            self.transition_and_return(context, PopOrWriteJournalEntry(sys_name, expected))?;
        Ok(s)
    }
}

pub(crate) struct SysCompletableEntry<M>(pub(crate) &'static str, pub(crate) M);

impl<
        M: RestateMessage
            + CompletableEntryMessage
            + EntryMessageHeaderEq
            + EntryMessage
            + Clone
            + WriteableRestateMessage,
    > TransitionAndReturn<Context, SysCompletableEntry<M>> for State
{
    type Output = AsyncResultHandle;

    fn transition_and_return(
        self,
        context: &mut Context,
        SysCompletableEntry(sys_name, expected): SysCompletableEntry<M>,
    ) -> Result<(Self, Self::Output), VMError> {
        context.journal.transition(&expected);
        self.check_side_effect_guard()?;
        let (mut s, actual) = TransitionAndReturn::transition_and_return(
            self,
            context,
            PopOrWriteJournalEntry(sys_name, expected),
        )?;

        let ar_handle = AsyncResultHandle(context.journal.expect_index());
        if let Some(c) = actual.into_completion() {
            match s {
                State::Replaying {
                    ref mut async_results,
                    ..
                }
                | State::Processing {
                    ref mut async_results,
                    ..
                } => {
                    async_results.insert_ready_result(ar_handle.0, c);
                }
                s => return Err(UnexpectedStateError::new(s.into(), sys_name).into()),
            }
        }
        Ok((s, ar_handle))
    }
}

pub(crate) struct SysRunEnter(pub(crate) String);

impl TransitionAndReturn<Context, SysRunEnter> for State {
    type Output = RunEnterResult;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        SysRunEnter(name): SysRunEnter,
    ) -> Result<(Self, Self::Output), VMError> {
        let expected = RunEntryMessage {
            name: name.clone(),
            ..RunEntryMessage::default()
        };
        context.journal.transition(&expected);
        self.check_side_effect_guard()?;
        match self {
            State::Processing {
                ref mut run_state, ..
            } => {
                *run_state = RunState::Running(name);
                Ok((self, RunEnterResult::NotExecuted))
            }
            s => {
                let (s, msg) =
                    s.transition_and_return(context, PopJournalEntry("SysRunEnter", expected))?;
                Ok((
                    s,
                    RunEnterResult::Executed(msg.result.ok_or(UNEXPECTED_NONE_RUN_RESULT)?.into()),
                ))
            }
        }
    }
}

pub(crate) struct SysRunExit(pub(crate) NonEmptyValue);

impl TransitionAndReturn<Context, SysRunExit> for State {
    type Output = AsyncResultHandle;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        SysRunExit(value): SysRunExit,
    ) -> Result<(Self, Self::Output), VMError> {
        match self {
            State::Processing {
                ref mut async_results,
                ref mut run_state,
                ..
            } => {
                let name = match mem::replace(run_state, RunState::NotRunning) {
                    RunState::Running(n) => n,
                    RunState::NotRunning => {
                        return Err(INVOKED_RUN_EXIT_WITHOUT_ENTER);
                    }
                };

                let current_journal_index = context.journal.expect_index();

                async_results
                    .insert_waiting_ack_result(current_journal_index, value.clone().into());

                let expected = RunEntryMessage {
                    name,
                    result: Some(match value {
                        NonEmptyValue::Success(b) => run_entry_message::Result::Value(b.into()),
                        NonEmptyValue::Failure(f) => run_entry_message::Result::Failure(f.into()),
                    }),
                };
                context.output.send(&expected);

                Ok((self, AsyncResultHandle(current_journal_index)))
            }
            s => Err(UnexpectedStateError::new(s.into(), "SysRunExit").into()),
        }
    }
}

fn check_entry_header_match<M: EntryMessageHeaderEq + Clone + fmt::Debug>(
    actual: &M,
    expected: &M,
) -> Result<(), VMError> {
    if !actual.header_eq(expected) {
        return Err(EntryMismatchError::new(actual.clone(), expected.clone()).into());
    }

    Ok(())
}
