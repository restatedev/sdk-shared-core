use crate::vm::context::Context;
use crate::vm::errors::{
    AwaitingTwoAsyncResultError, UnexpectedStateError, INPUT_CLOSED_WHILE_WAITING_ENTRIES,
};
use crate::vm::transitions::{HitSuspensionPoint, Transition, TransitionAndReturn};
use crate::vm::State;
use crate::{SuspendedError, VMError, Value};
use tracing::warn;

pub(crate) struct NotifyInputClosed;

impl Transition<Context, NotifyInputClosed> for State {
    fn transition(self, context: &mut Context, _: NotifyInputClosed) -> Result<Self, VMError> {
        match self {
            State::Replaying {
                current_await_point: Some(await_point),
                ref async_results,
                ..
            }
            | State::Processing {
                current_await_point: Some(await_point),
                ref async_results,
                ..
            } if !async_results.has_ready_result(await_point) => {
                self.transition(context, HitSuspensionPoint(await_point))
            }
            State::WaitingStart | State::WaitingReplayEntries { .. } => {
                Err(INPUT_CLOSED_WHILE_WAITING_ENTRIES)
            }
            _ => Ok(self),
        }
    }
}

pub(crate) struct NotifyAwaitPoint(pub(crate) u32);

impl Transition<Context, NotifyAwaitPoint> for State {
    fn transition(
        mut self,
        context: &mut Context,
        NotifyAwaitPoint(await_point): NotifyAwaitPoint,
    ) -> Result<Self, VMError> {
        match self {
            State::Replaying {
                ref mut current_await_point,
                ref async_results,
                ..
            }
            | State::Processing {
                ref mut current_await_point,
                ref async_results,
                ..
            } => {
                if let Some(previous) = current_await_point {
                    if *previous != await_point {
                        if context.options.fail_on_wait_concurrent_async_result {
                            return Err(AwaitingTwoAsyncResultError {
                                previous: *previous,
                                current: await_point,
                            }
                            .into());
                        } else {
                            warn!(
                                "{}",
                                AwaitingTwoAsyncResultError {
                                    previous: *previous,
                                    current: await_point,
                                }
                            )
                        }
                    }
                }
                if context.input_is_closed && !async_results.has_ready_result(await_point) {
                    return self.transition(context, HitSuspensionPoint(await_point));
                };

                *current_await_point = Some(await_point);
            }
            s => return Err(UnexpectedStateError::new(s.into(), "NotifyAwaitPoint").into()),
        };

        Ok(self)
    }
}

pub(crate) struct TakeAsyncResult(pub(crate) u32);

impl TransitionAndReturn<Context, TakeAsyncResult> for State {
    type Output = Result<Option<Value>, SuspendedError>;

    fn transition_and_return(
        mut self,
        _: &mut Context,
        TakeAsyncResult(async_result): TakeAsyncResult,
    ) -> Result<(Self, Self::Output), VMError> {
        match self {
            State::Processing {
                ref mut current_await_point,
                ref mut async_results,
                ..
            }
            | State::Replaying {
                ref mut current_await_point,
                ref mut async_results,
                ..
            } => {
                let opt = async_results.take_ready_result(async_result);

                // Reset current await point if matches
                if opt.is_some() && current_await_point.is_some_and(|i| i == async_result) {
                    *current_await_point = None;
                }

                Ok((self, Ok(opt)))
            }
            State::Suspended => Ok((self, Err(SuspendedError))),
            s => Err(UnexpectedStateError::new(s.into(), "TakeAsyncResult").into()),
        }
    }
}
