use crate::error::{CommandMetadata, NotificationMetadata};
use crate::service_protocol::messages::AwaitingOnMessage;
use crate::service_protocol::{MessageType, NotificationId, CANCEL_SIGNAL_ID};
use crate::vm::async_results_state::ResolveFutureResult;
use crate::vm::context::Context;
use crate::vm::errors::UncompletedDoProgressDuringReplay;
use crate::vm::transitions::{HitSuspensionPoint, Transition, TransitionAndReturn};
use crate::vm::{awakeable_id_str, State};
use crate::{
    AwaitingOnPolicy, DoProgressResponse, Error, NotificationHandle, UnresolvedFuture, Value,
    Version,
};
use std::collections::HashMap;

pub(crate) struct Suspended;

pub(crate) struct DoProgress(pub(crate) UnresolvedFuture);

impl TransitionAndReturn<Context, DoProgress> for State {
    type Output = Result<DoProgressResponse, Suspended>;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        DoProgress(unresolved_future): DoProgress,
    ) -> Result<(Self, Self::Output), Error> {
        match self {
            State::Replaying {
                ref mut async_results,
                ref run_state,
                ..
            } => {
                let ResolveFutureResult::Unresolved(unresolved_future) =
                    async_results.try_resolve_future(unresolved_future)
                else {
                    // We're good, let's give back control to user code
                    return Ok((self, Ok(DoProgressResponse::AnyCompleted)));
                };
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
                let awaiting_on_handles = unresolved_future.handles();
                let notification_ids =
                    async_results.resolve_notification_handles(&awaiting_on_handles);
                let mut known_notification_metadata = HashMap::with_capacity(2);
                let mut known_command_metadata: Option<CommandMetadata> = None;
                // Collect run info
                for handle in awaiting_on_handles {
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
                let ResolveFutureResult::Unresolved(unresolved_future) =
                    async_results.try_resolve_future(unresolved_future)
                else {
                    // We're good, let's give back control to user code
                    return Ok((self, Ok(DoProgressResponse::AnyCompleted)));
                };

                let awaiting_on_handles = unresolved_future.handles();

                // We couldn't find any notification for the given ids, let's check if there's some run to execute
                if let Some(run_to_execute) = run_state.try_execute_run(&awaiting_on_handles) {
                    return Ok((self, Ok(DoProgressResponse::ExecuteRun(run_to_execute))));
                }

                // Maybe any of the handles in the awaiting on set is executing?
                let waiting_run_proposal =
                    run_state.any_executing_in_this_set(&awaiting_on_handles);

                // Check suspension condition
                if context.input_is_closed {
                    // Some run still executing; it's not time to suspend yet!
                    if waiting_run_proposal {
                        return Ok((
                            self,
                            Ok(DoProgressResponse::WaitingExternalProgress {
                                waiting_input: false,
                                waiting_run_proposal: true,
                            }),
                        ));
                    }

                    let state = self.transition(context, HitSuspensionPoint(unresolved_future))?;
                    return Ok((state, Err(Suspended)));
                };

                // The only thing we can do at this point is to wait for some notification from the runtime,
                // which will be received reading from input.

                // Before returning, let's see if we need to send AwaitingOnMessage
                if context.negotiated_protocol_version >= Version::V7
                    && match context.awaiting_on_policy {
                        AwaitingOnPolicy::SendAlways => true,
                        AwaitingOnPolicy::DontSendWhenExecutingRun => !run_state.any_executing(),
                        AwaitingOnPolicy::DontSend => false,
                    }
                {
                    context.output.send(&AwaitingOnMessage {
                        awaiting_on: Some(
                            async_results.resolve_unresolved_future(unresolved_future),
                        ),
                        executing_side_effects: false,
                    })
                }

                // Nothing else can be done, we need more input
                Ok((
                    self,
                    Ok(DoProgressResponse::WaitingExternalProgress {
                        waiting_input: true,
                        waiting_run_proposal,
                    }),
                ))
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
