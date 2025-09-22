use crate::vm::context::Context;
use crate::vm::transitions::{HitSuspensionPoint, Transition, TransitionAndReturn};
use crate::vm::State;
use crate::{DoProgressResponse, Error, NotificationHandle, Value};
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

                let notification_ids = async_results.resolve_notification_handles(awaiting_on);
                if notification_ids.is_empty() {
                    // This can happen if `do_progress` was called while the SDK has all the results already.
                    return Ok((self, Ok(DoProgressResponse::AnyCompleted)));
                }

                // Let's try to find it in the notifications we already have
                if async_results.process_next_until_any_found(&notification_ids) {
                    // We're good, let's give back control to user code
                    return Ok((self, Ok(DoProgressResponse::AnyCompleted)));
                }

                // Check suspension condition
                if context.input_is_closed {
                    let state = self.transition(context, HitSuspensionPoint(notification_ids))?;
                    return Ok((state, Err(Suspended)));
                };

                // Nothing else can be done, we need more input
                Ok((self, Ok(DoProgressResponse::ReadFromInput)))
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

                let notification_ids =
                    async_results.resolve_notification_handles(awaiting_on.clone());
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
