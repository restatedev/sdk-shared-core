use crate::service_protocol::messages::{EndMessage, SuspensionMessage};
use crate::vm::context::Context;
use crate::vm::transitions::Transition;
use crate::vm::State;
use crate::{fmt, Error, UnresolvedFuture};

pub(crate) struct HitError(pub(crate) Error);

impl Transition<Context, HitError> for State {
    fn transition(self, _: &mut Context, HitError(error): HitError) -> Result<Self, Error> {
        // We let CoreVM::do_transition handle this
        Err(error)
    }
}

pub(crate) struct HitSuspensionPoint(pub(crate) UnresolvedFuture);

impl Transition<Context, HitSuspensionPoint> for State {
    fn transition(
        self,
        context: &mut Context,
        HitSuspensionPoint(awaiting_on): HitSuspensionPoint,
    ) -> Result<Self, Error> {
        if matches!(self, State::Closed) {
            // Nothing to do
            return Ok(self);
        }
        let async_results = match self {
            State::Processing { async_results, .. } => async_results,
            s => return Err(s.as_unexpected_state("HitSuspensionPoint")),
        };
        tracing::debug!("Suspending");

        let future = async_results.resolve_unresolved_future(awaiting_on);
        let suspension_message = SuspensionMessage {
            awaiting_on: Some(future),
        };

        context.output.send(&suspension_message);
        context.output.send_eof();

        Ok(State::Closed)
    }
}

pub(crate) struct SysEnd;

impl Transition<Context, SysEnd> for State {
    fn transition(self, context: &mut Context, _: SysEnd) -> Result<Self, Error> {
        match self {
            State::Processing { .. } => {
                context.output.send(&EndMessage {});
                context.output.send_eof();
                Ok(State::Closed)
            }
            s @ State::Closed => {
                // Tolerate the case where the state machine is already ended/suspended
                Ok(s)
            }
            s => Err(s.as_unexpected_state(fmt::format_sys_end())),
        }
    }
}
