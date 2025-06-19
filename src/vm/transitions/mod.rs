mod async_results;
mod input;
mod journal;
mod terminal;

use crate::service_protocol::messages::ErrorMessage;
use crate::vm::context::Context;
use crate::vm::State;
use crate::{CoreVM, Error};
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
                let was_closed = matches!(s, State::Ended | State::Suspended);
                match TransitionAndReturn::transition_and_return(s, &mut self.context, event) {
                    Ok((new_state, output)) => {
                        self.last_transition = Ok(new_state);
                        Ok(output)
                    }
                    Err(e) => {
                        if was_closed {
                            // Do nothing, it was already closed!
                            return Err(e);
                        }

                        tracing::debug!("Failed with error {e}");

                        // We need to handle this error and register it!
                        self.last_transition = Err(e.clone());
                        self.context.output.send(&e.as_error_message());
                        self.context.output.send_eof();

                        Err(e)
                    }
                }
            }
        }
    }
}

impl Error {
    fn as_error_message(&self) -> ErrorMessage {
        ErrorMessage {
            code: self.code as u32,
            message: self.message.clone().into_owned(),
            stacktrace: self.stacktrace.clone().into_owned(),
            related_command_index: self.related_command.as_ref().and_then(|rc| rc.index),
            related_command_name: self
                .related_command
                .as_ref()
                .and_then(|rc| rc.name.as_ref())
                .map(|name| name.clone().into_owned()),
            related_command_type: self
                .related_command
                .as_ref()
                .and_then(|rc| rc.ty)
                .map(|ty| u16::from(ty).into()),
            next_retry_delay: self.next_retry_delay.map(|d| d.as_millis() as u64),
        }
    }
}
