use crate::error::{CommandMetadata, NotificationMetadata};
use crate::service_protocol::{MessageType, NotificationId, CANCEL_SIGNAL_ID};
use crate::vm::context::Context;
use crate::vm::errors::UncompletedDoProgressDuringReplay;
use crate::vm::transitions::{HitSuspensionPoint, Transition, TransitionAndReturn};
use crate::vm::{awakeable_id_str, State};
use crate::{DoProgressResponse, Error, NotificationHandle, Value};
use std::collections::HashMap;
use tracing::trace;

pub(crate) struct Suspended;

pub(crate) struct DoProgress(pub(crate) Vec<NotificationHandle>);

impl TransitionAndReturn<Context, DoProgress> for State {
    type Output = Result<DoProgressResponse, Suspended>;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        DoProgress(awaiting_on): DoProgress,
    ) -> Result<(Self, Self::Output), Error> {
        match self {
            State::Replaying {
                ref mut async_results,
                ref run_state,
                ..
            } => {
                // Check first if any was completed already
                if awaiting_on
                    .iter()
                    .any(|h| async_results.is_handle_completed(*h))
                {
                    // We're good, let's give back control to user code
                    return Ok((self, Ok(DoProgressResponse::AnyCompleted)));
                }

                let notification_ids = async_results.resolve_notification_handles(&awaiting_on);
                if notification_ids.is_empty() {
                    // This can happen if `do_progress` was called while the SDK has all the results already.
                    return Ok((self, Ok(DoProgressResponse::AnyCompleted)));
                }

                // Let's try to find it in the notifications we already have
                if async_results.process_next_until_any_found(&notification_ids) {
                    // We're good, let's give back control to user code
                    return Ok((self, Ok(DoProgressResponse::AnyCompleted)));
                }

                // This assertion proves the user mutated the code, adding an await point.
                //
                // Proof by contradiction:
                //
                // 1. During replay, we transition to processing AFTER replaying all COMMANDS
                //    (not after replaying the entire journal, which includes both commands and notifications)
                // 2. This code path handles awaits ONLY during replay
                // 3. If we reach this point, none of the previous checks succeeded, meaning we don't have enough notifications to complete this await point
                // 4. But if this await cannot be completed during replay, then in previous replays/execution attempts, no progress should have been made afterward,
                //    meaning there should be no more commands to replay in the state machine
                // 5. However, we ARE still replaying (as evidenced by being in this code path), which means
                //    there ARE commands to replay after this await point
                //
                // This contradiction proves the code was mutated: an await must have been added after
                // the journal was originally created.

                // Prepare error metadata here, we gotta be nice to make sure users can debug this
                let mut known_notification_metadata = HashMap::with_capacity(2);
                let mut known_command_metadata: Option<CommandMetadata> = None;
                // Collect run info
                for handle in awaiting_on {
                    if let Some((command_index, name)) = run_state.get_run_info(&handle) {
                        let notification_id =
                            async_results.must_resolve_notification_handle(&handle);
                        let command_metadata = CommandMetadata::new_named(
                            name.to_owned(),
                            command_index,
                            MessageType::RunCommand,
                        );
                        known_command_metadata = Some(command_metadata.clone());
                        known_notification_metadata.insert(
                            notification_id,
                            NotificationMetadata::RelatedToCommand(command_metadata),
                        );
                    }
                }
                // For awakeables, prep ids
                for notification_id in &notification_ids {
                    if let NotificationId::SignalId(id) = notification_id {
                        if *id == CANCEL_SIGNAL_ID {
                            known_notification_metadata.insert(
                                notification_id.clone(),
                                NotificationMetadata::Cancellation,
                            );
                        } else if *id > 16 {
                            known_notification_metadata.insert(
                                notification_id.clone(),
                                NotificationMetadata::Awakeable(awakeable_id_str(
                                    &context.expect_start_info().id,
                                    *id,
                                )),
                            );
                        }
                    }
                }
                let mut error = Error::from(UncompletedDoProgressDuringReplay::new(
                    notification_ids,
                    known_notification_metadata,
                ));
                if let Some(command_metadata) = known_command_metadata {
                    error = error.with_related_command_metadata(command_metadata)
                }
                Err(error)
            }
            State::Processing {
                ref mut async_results,
                ref mut run_state,
                ..
            } => {
                // Check first if any was completed already
                if awaiting_on
                    .iter()
                    .any(|h| async_results.is_handle_completed(*h))
                {
                    // We're good, let's give back control to user code
                    return Ok((self, Ok(DoProgressResponse::AnyCompleted)));
                }

                let notification_ids = async_results.resolve_notification_handles(&awaiting_on);
                if notification_ids.is_empty() {
                    trace!("Could not resolve any of the {awaiting_on:?} handles");
                    return Ok((self, Ok(DoProgressResponse::AnyCompleted)));
                }

                // Let's try to find it in the notifications we already have
                if async_results.process_next_until_any_found(&notification_ids) {
                    // We're good, let's give back control to user code
                    return Ok((self, Ok(DoProgressResponse::AnyCompleted)));
                }

                // We couldn't find any notification for the given ids, let's check if there's some run to execute
                if let Some(run_to_execute) =
                    run_state.try_execute_run(&awaiting_on.iter().cloned().collect())
                {
                    return Ok((self, Ok(DoProgressResponse::ExecuteRun(run_to_execute))));
                }

                // Check suspension condition
                if context.input_is_closed {
                    // Maybe something is executing and we're awaiting it to complete,
                    // in this case we don't suspend yet!
                    if run_state.any_executing(&awaiting_on) {
                        return Ok((self, Ok(DoProgressResponse::WaitingPendingRun)));
                    }

                    let state = self.transition(context, HitSuspensionPoint(notification_ids))?;
                    return Ok((state, Err(Suspended)));
                };

                // Nothing else can be done, we need more input
                Ok((self, Ok(DoProgressResponse::ReadFromInput)))
            }
            s => Err(s.as_unexpected_state(crate::fmt::format_do_progress())),
        }
    }
}

pub(crate) struct TakeNotification(pub(crate) NotificationHandle);

impl TransitionAndReturn<Context, TakeNotification> for State {
    type Output = Result<Option<Value>, Suspended>;

    fn transition_and_return(
        mut self,
        _: &mut Context,
        TakeNotification(handle): TakeNotification,
    ) -> Result<(Self, Self::Output), Error> {
        match self {
            State::Processing {
                ref mut async_results,
                ..
            }
            | State::Replaying {
                ref mut async_results,
                ..
            } => {
                let opt = async_results.take_handle(handle);
                Ok((self, Ok(opt.map(Into::into))))
            }
            s => Err(s.as_unexpected_state("TakeNotification")),
        }
    }
}

pub(crate) struct CopyNotification(pub(crate) NotificationHandle);

impl TransitionAndReturn<Context, CopyNotification> for State {
    type Output = Result<Option<Value>, Suspended>;

    fn transition_and_return(
        mut self,
        _: &mut Context,
        CopyNotification(handle): CopyNotification,
    ) -> Result<(Self, Self::Output), Error> {
        match self {
            State::Processing {
                ref mut async_results,
                ..
            }
            | State::Replaying {
                ref mut async_results,
                ..
            } => {
                let opt = async_results.copy_handle(handle);
                Ok((self, Ok(opt.map(Into::into))))
            }
            s => Err(s.as_unexpected_state("CopyNotification")),
        }
    }
}
