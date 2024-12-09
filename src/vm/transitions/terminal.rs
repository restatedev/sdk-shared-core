use crate::service_protocol::messages::{EndMessage, SuspensionMessage};
use crate::vm::context::Context;
use crate::vm::errors::UnexpectedStateError;
use crate::vm::transitions::Transition;
use crate::vm::State;
use crate::Error;
use std::time::Duration;

pub(crate) struct HitError {
    pub(crate) error: Error,
    pub(crate) next_retry_delay: Option<Duration>,
}

impl Transition<Context, HitError> for State {
    fn transition(
        self,
        ctx: &mut Context,
        HitError {
            error,
            next_retry_delay,
        }: HitError,
    ) -> Result<Self, Error> {
        ctx.next_retry_delay = next_retry_delay;

        // We let CoreVM::do_transition handle this
        Err(error)
    }
}

pub(crate) struct HitSuspensionPoint(pub(crate) u32);

impl Transition<Context, HitSuspensionPoint> for State {
    fn transition(
        self,
        context: &mut Context,
        HitSuspensionPoint(await_point): HitSuspensionPoint,
    ) -> Result<Self, Error> {
        if matches!(self, State::Suspended | State::Ended) {
            // Nothing to do
            return Ok(self);
        }
        tracing::debug!("Suspending");
        context.output.send(&SuspensionMessage {
            entry_indexes: vec![await_point],
        });
        context.output.send_eof();

        Ok(State::Suspended)
    }
}

pub(crate) struct SysEnd;

impl Transition<Context, SysEnd> for State {
    fn transition(self, context: &mut Context, _: SysEnd) -> Result<Self, Error> {
        match self {
            State::Processing { .. } => {
                context.output.send(&EndMessage {});
                context.output.send_eof();
                Ok(State::Ended)
            }
            s @ State::Ended | s @ State::Suspended => {
                // Tolerate the case where the state machine is already ended/suspended
                Ok(s)
            }
            s => Err(UnexpectedStateError::new(s.into(), "SysEnd").into()),
        }
    }
}
