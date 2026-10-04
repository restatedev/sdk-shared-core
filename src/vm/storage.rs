//! Storage journal mode: the journal is used as a durable store of results, looked up by entry name,
//! rather than as a log to replay deterministically.
//!
//! At the beginning of each attempt, the VM doesn't replay the journal: it indexes it, then lets the handler
//! run from the beginning as regular, possibly non-deterministic, code. Every journaled operation has a key,
//! its entry name: the name given by the user, suffixed with `#n` for the n-th occurrence of the same name
//! in the same attempt. When the handler performs an operation, the VM looks up its key:
//!
//! * runs and steps whose result is in the journal return that result, without executing again,
//! * calls, one way calls and sleeps already in the journal are not issued again, their results are awaited,
//! * anything else is appended at the end of the journal.
//!
//! No determinism check is performed: the order of the operations, and which operations are performed,
//! can change between attempts.

use crate::service_protocol::messages::{
    CallCommandMessage, OneWayCallCommandMessage, OutputCommandMessage, RunCommandMessage,
    SleepCommandMessage,
};
use crate::service_protocol::{messages, CompletionId, MessageType, NotificationId, RawMessage};
use crate::vm::context::Context;
use crate::vm::transitions::TransitionAndReturn;
use crate::vm::tx::{TxCommitRecord, TxStateMutation};
use crate::vm::State;
use crate::Error;
use bytes::Bytes;
use std::collections::HashMap;

/// Prefix of the `RunCommand` names used for transactional steps.
pub(crate) const STEP_RUN_NAME_PREFIX: &str = "tx:";

#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub(crate) enum EntryKind {
    Run,
    Step,
    Call,
    Send,
    Sleep,
}

/// Index of the journal, keyed by entry name.
#[derive(Debug, Default)]
pub(crate) struct StorageJournal {
    /// Runs and steps whose result is in the journal.
    pub(crate) completed_runs: HashMap<String, CompletionId>,
    /// Calls: invocation id completion id, result completion id.
    pub(crate) calls: HashMap<String, (CompletionId, CompletionId)>,
    /// One way calls: invocation id completion id.
    pub(crate) sends: HashMap<String, CompletionId>,
    pub(crate) sleeps: HashMap<String, CompletionId>,
    /// Occurrences of each entry name in this attempt.
    occurrences: HashMap<(EntryKind, String), u32>,
}

impl StorageJournal {
    /// Returns the key of the next entry of the given kind and name:
    /// the name for the first occurrence in this attempt, `name#n` for the n-th one.
    pub(crate) fn next_key(&mut self, kind: EntryKind, name: &str) -> String {
        let occurrence = self
            .occurrences
            .entry((kind, name.to_owned()))
            .and_modify(|o| *o += 1)
            .or_insert(1);
        let key = if *occurrence == 1 {
            name.to_owned()
        } else {
            format!("{name}#{occurrence}")
        };
        match kind {
            EntryKind::Step => format!("{STEP_RUN_NAME_PREFIX}{key}"),
            _ => key,
        }
    }
}

/// A command to emit when applying a commit record.
pub(crate) enum ApplyCommand {
    ClearAllState,
    SetState(String, Bytes),
    ClearState(String),
    Send(OneWayCallCommandMessage),
}

/// The commands applying `record`, in order.
pub(crate) fn apply_commands(record: TxCommitRecord) -> Vec<ApplyCommand> {
    let mut commands = Vec::with_capacity(
        usize::from(record.clear_all_state) + record.state_mutations.len() + record.sends.len(),
    );
    if record.clear_all_state {
        commands.push(ApplyCommand::ClearAllState);
    }
    for TxStateMutation { key, value } in record.state_mutations {
        commands.push(match value {
            Some(value) => ApplyCommand::SetState(key, value),
            None => ApplyCommand::ClearState(key),
        });
    }
    commands.extend(record.sends.into_iter().map(ApplyCommand::Send));
    commands
}

fn is_apply_command(ty: MessageType) -> bool {
    matches!(
        ty,
        MessageType::SetStateCommand
            | MessageType::ClearStateCommand
            | MessageType::ClearAllStateCommand
            | MessageType::OneWayCallCommand
    )
}

/// Result of indexing the replayed journal.
pub(crate) struct IndexedJournal {
    pub(crate) journal: StorageJournal,
    /// Number of commands in the journal, input included.
    pub(crate) commands: u32,
    pub(crate) next_completion_id: CompletionId,
    pub(crate) output: Option<OutputCommandMessage>,
    /// Committed step whose apply commands were not all written,
    /// with the number of apply commands already in the journal.
    /// Because a step commit and its apply commands are written together, only the last command group can be incomplete.
    pub(crate) incomplete_step: Option<(CompletionId, usize)>,
}

/// Indexes the commands of the replayed journal, after the input command.
/// `is_completed` tells whether a completion id is in the replayed journal.
pub(crate) fn index_journal(
    commands: impl IntoIterator<Item = RawMessage>,
    is_completed: impl Fn(CompletionId) -> bool,
) -> Result<IndexedJournal, Error> {
    let mut journal = StorageJournal::default();
    // The input command was already consumed
    let mut command_count = 1;
    let mut max_completion_id = 0;
    let mut output = None;
    // Last committed step, with the apply commands that follow it
    let mut last_step: Option<(CompletionId, usize)> = None;

    for raw in commands {
        let ty = raw.ty();
        let command_index = i64::from(command_count);
        command_count += 1;

        if is_apply_command(ty) {
            if let Some((_, applied)) = &mut last_step {
                *applied += 1;
            }
        } else {
            last_step = None;
        }

        let completion_ids: Vec<CompletionId> = match ty {
            MessageType::RunCommand => {
                let cmd = raw.decode_to::<RunCommandMessage>(command_index)?;
                if is_completed(cmd.result_completion_id) {
                    if cmd.name.starts_with(STEP_RUN_NAME_PREFIX) {
                        last_step = Some((cmd.result_completion_id, 0));
                    }
                    journal
                        .completed_runs
                        .insert(cmd.name, cmd.result_completion_id);
                }
                vec![cmd.result_completion_id]
            }
            MessageType::CallCommand => {
                let cmd = raw.decode_to::<CallCommandMessage>(command_index)?;
                journal.calls.insert(
                    cmd.name,
                    (cmd.invocation_id_notification_idx, cmd.result_completion_id),
                );
                vec![cmd.invocation_id_notification_idx, cmd.result_completion_id]
            }
            MessageType::OneWayCallCommand => {
                let cmd = raw.decode_to::<OneWayCallCommandMessage>(command_index)?;
                // One way calls applied by steps are unnamed, and are never looked up
                if !cmd.name.is_empty() {
                    journal
                        .sends
                        .insert(cmd.name, cmd.invocation_id_notification_idx);
                }
                vec![cmd.invocation_id_notification_idx]
            }
            MessageType::SleepCommand => {
                let cmd = raw.decode_to::<SleepCommandMessage>(command_index)?;
                journal.sleeps.insert(cmd.name, cmd.result_completion_id);
                vec![cmd.result_completion_id]
            }
            MessageType::OutputCommand => {
                output = Some(raw.decode_to::<OutputCommandMessage>(command_index)?);
                vec![]
            }
            MessageType::GetLazyStateCommand => vec![
                raw.decode_to::<messages::GetLazyStateCommandMessage>(command_index)?
                    .result_completion_id,
            ],
            MessageType::GetLazyStateKeysCommand => vec![
                raw.decode_to::<messages::GetLazyStateKeysCommandMessage>(command_index)?
                    .result_completion_id,
            ],
            MessageType::GetPromiseCommand => vec![
                raw.decode_to::<messages::GetPromiseCommandMessage>(command_index)?
                    .result_completion_id,
            ],
            MessageType::PeekPromiseCommand => vec![
                raw.decode_to::<messages::PeekPromiseCommandMessage>(command_index)?
                    .result_completion_id,
            ],
            MessageType::CompletePromiseCommand => vec![
                raw.decode_to::<messages::CompletePromiseCommandMessage>(command_index)?
                    .result_completion_id,
            ],
            MessageType::AttachInvocationCommand => vec![
                raw.decode_to::<messages::AttachInvocationCommandMessage>(command_index)?
                    .result_completion_id,
            ],
            MessageType::GetInvocationOutputCommand => vec![
                raw.decode_to::<messages::GetInvocationOutputCommandMessage>(command_index)?
                    .result_completion_id,
            ],
            _ => vec![],
        };
        if let Some(max) = completion_ids.into_iter().max() {
            max_completion_id = max_completion_id.max(max);
        }
    }

    Ok(IndexedJournal {
        journal,
        commands: command_count,
        next_completion_id: max_completion_id + 1,
        output,
        // Only the tail group can be incomplete, this is checked against the record by the caller
        incomplete_step: last_step,
    })
}

/// Indexes the replayed journal instead of replaying it, and moves to processing.
pub(crate) struct RestoreFromJournal;

impl TransitionAndReturn<Context, RestoreFromJournal> for State {
    type Output = IndexedJournal;

    fn transition_and_return(
        self,
        _: &mut Context,
        _: RestoreFromJournal,
    ) -> Result<(Self, Self::Output), Error> {
        match self {
            State::Replaying {
                commands,
                run_state,
                mut async_results,
                eager_state,
            } => {
                // The order of the notifications doesn't matter, they're looked up by id
                async_results.drain_to_ready();
                let indexed = index_journal(commands, |completion_id| {
                    async_results
                        .get_ready(&NotificationId::CompletionId(completion_id))
                        .is_some()
                })?;
                Ok((
                    State::Processing {
                        processing_first_entry: true,
                        run_state,
                        async_results,
                        eager_state,
                    },
                    indexed,
                ))
            }
            State::Processing {
                processing_first_entry,
                run_state,
                mut async_results,
                eager_state,
            } => {
                // Only the input was in the journal
                async_results.drain_to_ready();
                Ok((
                    State::Processing {
                        processing_first_entry,
                        run_state,
                        async_results,
                        eager_state,
                    },
                    index_journal(std::iter::empty(), |_| false)?,
                ))
            }
            s => Err(s.as_unexpected_state("restore from journal")),
        }
    }
}
