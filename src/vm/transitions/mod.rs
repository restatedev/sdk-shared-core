mod async_results;
mod input;
mod journal;
mod terminal;

use crate::service_protocol::messages::ErrorMessage;
use crate::vm::context::Context;
use crate::vm::{errors, State};
use crate::{CoreVM, Error, NonDeterministicChecksRetryPolicy, Version};
pub(crate) use async_results::*;
pub(crate) use input::*;
pub(crate) use journal::*;
use std::mem;
pub(crate) use terminal::*;

trait Transition<CTX, E>
where
    Self: Sized,
{
    fn transition(self, context: &mut CTX, event: E) -> Result<Self, Error>;
}

pub(crate) trait TransitionAndReturn<CTX, E>
where
    Self: Sized,
{
    type Output;
    fn transition_and_return(
        self,
        context: &mut CTX,
        event: E,
    ) -> Result<(Self, Self::Output), Error>;
}

impl<STATE, CTX, E> TransitionAndReturn<CTX, E> for STATE
where
    Self: Transition<CTX, E>,
{
    type Output = ();

    fn transition_and_return(
        self,
        context: &mut CTX,
        event: E,
    ) -> Result<(Self, Self::Output), Error> {
        Transition::transition(self, context, event).map(|s| (s, ()))
    }
}

impl CoreVM {
    pub(super) fn do_transition<E, O>(&mut self, event: E) -> Result<O, Error>
    where
        State: TransitionAndReturn<Context, E, Output = O>,
    {
        match mem::replace(&mut self.last_transition, Ok(State::WaitingStart)) {
            Err(e) => {
                // The state machine is in error mode, we just propagate back the error
                self.last_transition = Err(e.clone());
                Err(e)
            }
            Ok(s) => {
                let was_closed = matches!(s, State::Closed);
                match TransitionAndReturn::transition_and_return(s, &mut self.context, event) {
                    Ok((new_state, output)) => {
                        self.last_transition = Ok(new_state);
                        Ok(output)
                    }
                    Err(mut e) => {
                        tracing::debug!("Failed with error {e}");

                        if self.should_pause_on_non_determinism_error(&e) {
                            e.should_pause = true;
                        }

                        self.last_transition = Err(e.clone());

                        if !was_closed {
                            // We write it out only if it wasn't closed before
                            self.context.output.send(&e.as_error_message());
                            self.context.output.send_eof();
                        }

                        Err(e)
                    }
                }
            }
        }
    }

    /// Pausing requires service protocol version V7 or newer; on older versions this returns
    /// false and the error follows the normal retry policy.
    fn should_pause_on_non_determinism_error(&self, error: &Error) -> bool {
        matches!(
            self.options.non_determinism_checks_retry_policy,
            NonDeterministicChecksRetryPolicy::Pause
        ) && error.code() == errors::codes::JOURNAL_MISMATCH.code()
            && self.context.negotiated_protocol_version >= Version::V7
    }
}

impl Error {
    fn as_error_message(&self) -> ErrorMessage {
        ErrorMessage {
            code: self.code as u32,
            message: self.message.clone().into_owned(),
            stacktrace: self.stacktrace.clone(),
            related_command_index: self.related_command.as_ref().map(|cmd| cmd.index),
            related_command_name: self
                .related_command
                .as_ref()
                .and_then(|rc| rc.name.as_ref())
                .and_then(|name| if name.is_empty() { None } else { Some(name) })
                .map(|name| name.clone().into_owned()),
            related_command_type: self
                .related_command
                .as_ref()
                .map(|cmd| u16::from(cmd.ty).into()),
            next_retry_delay: self.next_retry_delay.map(|d| d.as_millis() as u64),
            should_pause: self.should_pause,
        }
    }
}
