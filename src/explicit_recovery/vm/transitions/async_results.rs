use crate::explicit_recovery::vm::async_results_state::NotificationId;
use crate::explicit_recovery::vm::context::Context;
use crate::explicit_recovery::vm::transitions::TransitionAndReturn;
use crate::explicit_recovery::vm::State;
use crate::explicit_recovery::Notification;
use crate::service_protocol::messages::{AwaitingOnMessage, CombinatorType, Future};
use crate::service_protocol::CANCEL_SIGNAL_ID;
use crate::Error;

pub(crate) struct TakeNextNotification;

impl TransitionAndReturn<Context, TakeNextNotification> for State {
    type Output = Option<Notification>;

    fn transition_and_return(
        mut self,
        _: &mut Context,
        _: TakeNextNotification,
    ) -> Result<(Self, Self::Output), Error> {
        let notification = match &mut self {
            State::Processing { async_results, .. } => async_results.take_ready(),
            // Nothing before the handler starts, nor after it ended.
            _ => None,
        };
        Ok((self, notification))
    }
}

/// Tells the runtime the attempt makes progress, with an [`AwaitingOnMessage`] (which resets the
/// runtime's inactivity timer) on the responses it waits for.
pub(crate) struct Heartbeat;

impl TransitionAndReturn<Context, Heartbeat> for State {
    type Output = ();

    fn transition_and_return(
        self,
        context: &mut Context,
        _: Heartbeat,
    ) -> Result<(Self, Self::Output), Error> {
        let waiting_notifications = match &self {
            State::Processing { async_results, .. } => async_results.waiting_notifications(),
            s => return Err(s.as_unexpected_state("Heartbeat")),
        };

        // TODO the current protocol versions dont support a way to notify im waiting for an ack, only for completions...
        let waiting_completions: Vec<u32> = waiting_notifications
            .into_iter()
            .filter_map(|notification_id| match notification_id {
                NotificationId::CompletionId(completion_id) => Some(completion_id.into()),
                _ => None,
            })
            .collect();

        context.output.send(&AwaitingOnMessage {
            awaiting_on: Some(Future {
                waiting_completions,
                // The runtime refuses an empty future: the cancellation is always there to wait on.
                // TODO this might not make any sense here, but we currently need it as workaround.
                waiting_signals: vec![CANCEL_SIGNAL_ID],
                combinator_type: CombinatorType::FirstCompleted as i32,
                ..Default::default()
            }),
            executing_side_effects: true,
        });
        Ok((self, ()))
    }
}
