mod actions;
mod async_results;
mod group;
mod input;
mod terminal;

use crate::explicit_recovery::vm::context::Context;
use crate::explicit_recovery::vm::{CoreExplicitRecoveryVM, State};
use crate::service_protocol::explicit_recovery::messages::ErrorMessage;
use crate::Error;
pub(crate) use actions::*;
pub(crate) use async_results::*;
pub(crate) use group::*;
pub(crate) use input::*;
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

impl CoreExplicitRecoveryVM {
    pub(super) fn do_transition<E, O>(&mut self, event: E) -> Result<O, Error>
    where
        State: TransitionAndReturn<Context, E, Output = O>,
    {
        match mem::replace(&mut self.last_transition, Ok(State::WaitingStart)) {
            Err(e) => {
                // The attempt failed, or was shut down: every call returns why.
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
                    Err(e) => {
                        tracing::debug!("Failed with error {e}");

                        self.last_transition = Err(e.clone());

                        if !was_closed {
                            // Ignored after a shutdown, which closed the output: nothing
                            // follows `ShutdownMessage`.
                            self.context.output.send(&ErrorMessage::from(&e));
                            self.context.output.send_eof();
                        }

                        Err(e)
                    }
                }
            }
        }
    }
}
