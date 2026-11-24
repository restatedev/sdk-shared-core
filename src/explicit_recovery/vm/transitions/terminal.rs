use crate::explicit_recovery::vm::context::Context;
use crate::explicit_recovery::vm::errors::attempt_shut_down;
use crate::explicit_recovery::vm::transitions::Transition;
use crate::explicit_recovery::vm::State;
use crate::service_protocol::explicit_recovery::messages::{
    end_message, EndMessage, ShutdownMessage,
};
use crate::{Error, NonEmptyValue};
use tracing::debug;

pub(crate) struct HitError(pub(crate) Error);

impl Transition<Context, HitError> for State {
    fn transition(self, _: &mut Context, HitError(error): HitError) -> Result<Self, Error> {
        // We let CoreExplicitRecoveryVM::do_transition handle this
        Err(error)
    }
}

/// The runtime shut the attempt down, closing its side of the stream: no notification can arrive
/// anymore.
///
/// The VM answers at once, without waiting for user code, with how many notifications it had
/// handed out: those the SDK took, not those it merely received. The runtime decides what
/// comes next. An open atomic group is dropped: it was never closed, so it is not sent.
pub(crate) struct HitShutdown;

impl Transition<Context, HitShutdown> for State {
    fn transition(self, context: &mut Context, _: HitShutdown) -> Result<Self, Error> {
        let attempt = match self {
            State::Running(attempt) | State::InAtomicGroup(attempt, _) => attempt,
            s => return Err(s.as_unexpected_state("HitShutdown")),
        };
        let processed_notifications = attempt.async_results.processed_notifications();
        debug!("Shut down, {processed_notifications} notifications processed");
        context.output.send(&ShutdownMessage {
            processed_notifications,
        });
        context.output.send_eof();

        // do_transition keeps this error, and writes nothing more: the output is closed.
        Err(attempt_shut_down())
    }
}

pub(crate) struct CmdEnd(pub(crate) NonEmptyValue);

impl Transition<Context, CmdEnd> for State {
    fn transition(self, context: &mut Context, CmdEnd(value): CmdEnd) -> Result<Self, Error> {
        match self {
            State::Running(_) => {
                context.output.send(&EndMessage {
                    output: Some(match value {
                        NonEmptyValue::Success(b) => end_message::Output::Value(b.into()),
                        NonEmptyValue::Failure(f) => end_message::Output::Failure(f.into()),
                    }),
                });
                context.output.send_eof();
                debug!("End of the invocation");
                Ok(State::Closed)
            }
            // Also with a group still open.
            s => Err(s.as_unexpected_state("CmdEnd")),
        }
    }
}
