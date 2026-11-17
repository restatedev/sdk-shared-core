use crate::explicit_recovery::vm::async_results_state::NotificationId;
use crate::explicit_recovery::vm::context::Context;
use crate::explicit_recovery::vm::errors::{attempt_shut_down, restart_attempt};
use crate::explicit_recovery::vm::transitions::Transition;
use crate::explicit_recovery::vm::State;
use crate::service_protocol::messages::{
    output_command_message, propose_run_completion_message, CombinatorType, EndMessage,
    ErrorMessage, Future, OutputCommandMessage, ProposeRunCompletionMessage, SuspensionMessage,
    WithFlags,
};
use crate::service_protocol::{MessageFlags, CANCEL_SIGNAL_ID};
use crate::{Error, NonEmptyValue, TerminalFailure};
use tracing::debug;

/// The failure of a trace still open when the handler ends.
const UNENDED_TRACE_MESSAGE: &str = "the handler ended before the trace";

pub(crate) struct HitError(pub(crate) Error);

impl Transition<Context, HitError> for State {
    fn transition(self, _: &mut Context, HitError(error): HitError) -> Result<Self, Error> {
        // We let CoreExplicitRecoveryVM::do_transition handle this
        Err(error)
    }
}

/// The runtime shut the attempt down, closing the input stream: no response can arrive anymore.
///
/// The attempt suspends on the responses that take long (calls, attachments), on which the
/// runtime starts the next attempt, and on the cancellation of the invocation unless it was
/// cancelled already. With no response pending, it fails with a retry delay of zero: the runtime
/// starts the next attempt at once, without counting a retry. Either way the attempt ends with
/// [`attempt_shut_down`], which every later call returns.
pub(crate) struct HitShutdown;

impl Transition<Context, HitShutdown> for State {
    fn transition(self, context: &mut Context, _: HitShutdown) -> Result<Self, Error> {
        let async_results = match self {
            State::Processing { async_results, .. } => async_results,
            s => return Err(s.as_unexpected_state("HitShutdown")),
        };

        let waiting_notifications = async_results.waiting_notifications();
        // TODO the current protocol versions dont support a way to notify im waiting for an ack, only for completions...
        let waiting_completions = waiting_notifications
            .into_iter()
            .filter_map(|notification_id| match notification_id {
                NotificationId::CompletionId(completion_id) => Some(completion_id.into()),
                _ => None,
            })
            .collect::<Vec<_>>();
        if waiting_completions.is_empty() {
            debug!("Shut down with no call pending, starting a new attempt");
            context.output.send(&ErrorMessage::from(&restart_attempt()));
        } else {
            debug!("Shut down, suspending until any of the completions {waiting_completions:?}");
            context.output.send(&SuspensionMessage {
                awaiting_on: Some(Future {
                    waiting_completions,
                    // A cancellation that comes while the invocation is suspended starts the next
                    // attempt, which learns of it: without it, the invocation would stay suspended
                    // until a call answers, and for good if none ever does. Once the invocation
                    // was cancelled, waiting on the cancellation would resume it right away, every
                    // time: the attempts that follow wait on the responses alone.
                    waiting_signals: if context.cancelled {
                        vec![]
                    } else {
                        vec![CANCEL_SIGNAL_ID]
                    },
                    combinator_type: CombinatorType::FirstCompleted as i32,
                    ..Default::default()
                }),
            });
        }
        context.output.send_eof();

        // do_transition keeps this error, and writes nothing more: the output is closed.
        Err(attempt_shut_down())
    }
}

pub(crate) struct CmdEnd(pub(crate) NonEmptyValue);

impl Transition<Context, CmdEnd> for State {
    fn transition(self, context: &mut Context, CmdEnd(value): CmdEnd) -> Result<Self, Error> {
        match self {
            State::Processing { trace_state, .. } => {
                // Send messages to end dangling traces.
                for completion_id in trace_state.end_dangling_traces() {
                    let result = propose_run_completion_message::Result::Failure(
                        TerminalFailure {
                            code: 500,
                            message: UNENDED_TRACE_MESSAGE.to_owned(),
                            metadata: vec![],
                        }
                        .into(),
                    );
                    context.output.send(&WithFlags(
                        &ProposeRunCompletionMessage {
                            result_completion_id: completion_id,
                            result: Some(result),
                        },
                        &[MessageFlags::RequestedAck],
                    ));
                }

                // Send output message
                context.journal.next_command_index();
                context.output.send(&OutputCommandMessage {
                    result: Some(match value {
                        NonEmptyValue::Success(b) => {
                            output_command_message::Result::Value(b.into())
                        }
                        NonEmptyValue::Failure(f) => {
                            output_command_message::Result::Failure(f.into())
                        }
                    }),
                    ..Default::default()
                });

                // End
                context.output.send(&EndMessage {});
                context.output.send_eof();
                debug!("End of the invocation");
                Ok(State::Closed)
            }
            s => Err(s.as_unexpected_state("CmdEnd")),
        }
    }
}
