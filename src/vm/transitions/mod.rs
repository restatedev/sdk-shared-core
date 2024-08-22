mod async_results;
mod input;
mod journal;
mod terminal;

use crate::service_protocol::messages::ErrorMessage;
use crate::vm::context::Context;
use crate::vm::State;
use crate::{CoreVM, VMError};
pub(crate) use async_results::*;
pub(crate) use input::*;
pub(crate) use journal::*;
use std::mem;
pub(crate) use terminal::*;

trait Transition<CTX, E>
where
    Self: Sized,
{
    fn transition(self, context: &mut CTX, event: E) -> Result<Self, VMError>;
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
    ) -> Result<(Self, Self::Output), VMError>;
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
    ) -> Result<(Self, Self::Output), VMError> {
        Transition::transition(self, context, event).map(|s| (s, ()))
    }
}

impl CoreVM {
    pub(super) fn do_transition<E, O>(&mut self, event: E) -> Result<O, VMError>
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
                        // We need to handle this error and register it!
                        self.last_transition = Err(e.clone());
                        let msg = ErrorMessage {
                            code: e.code as u32,
                            message: e.message.clone().into_owned(),
                            description: e.description.clone().into_owned(),
                            related_entry_index: Some(self.context.journal.index() as u32),
                            related_entry_name: Some(
                                self.context.journal.current_entry_name.clone(),
                            ),
                            related_entry_type: Some(u16::from(
                                self.context.journal.current_entry_ty,
                            ) as u32),
                            next_retry_delay: self
                                .context
                                .next_retry_delay
                                .map(|d| d.as_millis() as u64),
                        };
                        self.context.output.send(&msg);
                        self.context.output.send_eof();

                        Err(e)
                    }
                }
            }
        }
    }
}
