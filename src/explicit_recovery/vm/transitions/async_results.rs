use crate::explicit_recovery::vm::context::Context;
use crate::explicit_recovery::vm::transitions::TransitionAndReturn;
use crate::explicit_recovery::vm::State;
use crate::explicit_recovery::Notification;
use crate::service_protocol::explicit_recovery::messages::HeartbeatMessage;
use crate::{Error, CANCEL_NOTIFICATION_HANDLE};

pub(crate) struct TakeNextNotification;

impl TransitionAndReturn<Context, TakeNextNotification> for State {
    type Output = Option<Notification>;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        _: TakeNextNotification,
    ) -> Result<(Self, Self::Output), Error> {
        let notification = match &mut self {
            State::Running(attempt) | State::InAtomicGroup(attempt, _) => {
                attempt.async_results.take_next()
            }
            // Nothing before the handler starts, nor after it ended.
            _ => None,
        };
        if notification
            .as_ref()
            .is_some_and(|n| n.handle == CANCEL_NOTIFICATION_HANDLE)
        {
            context.cancel_taken = true;
        }
        Ok((self, notification))
    }
}

/// Tells the runtime the handler works on something it does not see. A control message: it is
/// not an action, and leaves at once also while an atomic group is open.
pub(crate) struct Heartbeat;

impl TransitionAndReturn<Context, Heartbeat> for State {
    type Output = ();

    fn transition_and_return(
        self,
        context: &mut Context,
        _: Heartbeat,
    ) -> Result<(Self, Self::Output), Error> {
        match &self {
            State::Running(_) | State::InAtomicGroup(..) => {
                context.output.send(&HeartbeatMessage {});
                Ok((self, ()))
            }
            s => Err(s.as_unexpected_state("Heartbeat")),
        }
    }
}
