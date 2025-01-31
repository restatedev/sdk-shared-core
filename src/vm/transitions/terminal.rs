use crate::service_protocol::messages::{EndMessage, SuspensionMessage};
use crate::service_protocol::NotificationId;
use crate::vm::context::Context;
use crate::vm::transitions::Transition;
use crate::vm::State;
use crate::Error;
use std::collections::HashSet;
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

pub(crate) struct HitSuspensionPoint(pub(crate) HashSet<NotificationId>);

impl Transition<Context, HitSuspensionPoint> for State {
    fn transition(
        self,
        context: &mut Context,
        HitSuspensionPoint(awaiting_on): HitSuspensionPoint,
    ) -> Result<Self, Error> {
        if matches!(self, State::Suspended | State::Ended) {
            // Nothing to do
            return Ok(self);
        }
        tracing::debug!("Suspending");

        let mut suspension_message = SuspensionMessage {
            waiting_completions: vec![],
            waiting_signals: vec![],
            waiting_named_signals: vec![],
        };
        for notification_id in awaiting_on {
            match notification_id {
                NotificationId::CompletionId(cid) => {
                    suspension_message.waiting_completions.push(cid)
                }
                NotificationId::SignalId(sid) => suspension_message.waiting_signals.push(sid),
                NotificationId::SignalName(name) => {
                    suspension_message.waiting_named_signals.push(name)
                }
            }
        }

        context.output.send(&suspension_message);
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
            s => Err(s.as_unexpected_state("SysEnd")),
        }
    }
}
