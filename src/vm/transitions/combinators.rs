use crate::service_protocol::messages::{CombinatorEntryMessage, SuspensionMessage};
use crate::vm::context::Context;
use crate::vm::errors::{UnexpectedStateError, BAD_COMBINATOR_ENTRY};
use crate::vm::transitions::{PopJournalEntry, TransitionAndReturn};
use crate::vm::State;
use crate::{
    AsyncResultAccessTracker, AsyncResultCombinator, AsyncResultHandle, AsyncResultState, Error,
    Value,
};
use std::collections::HashMap;
use std::iter::Peekable;
use std::vec::IntoIter;

pub(crate) enum AsyncResultAccessTrackerInner {
    Processing {
        known_results: HashMap<AsyncResultHandle, AsyncResultState>,
        tracked_access_to_completed_results: Vec<AsyncResultHandle>,
        tracked_access_to_uncompleted_results: Vec<AsyncResultHandle>,
    },
    Replaying {
        replay_combinators: Peekable<IntoIter<(AsyncResultHandle, AsyncResultState)>>,
    },
}

impl AsyncResultAccessTrackerInner {
    pub fn get_state(&mut self, handle: AsyncResultHandle) -> AsyncResultState {
        match self {
            AsyncResultAccessTrackerInner::Processing {
                known_results,
                tracked_access_to_completed_results,
                tracked_access_to_uncompleted_results,
            } => {
                // Record if a known result is available
                if let Some(res) = known_results.get(&handle) {
                    tracked_access_to_completed_results.push(handle);
                    *res
                } else {
                    tracked_access_to_uncompleted_results.push(handle);
                    AsyncResultState::NotReady
                }
            }
            AsyncResultAccessTrackerInner::Replaying {
                replay_combinators: replay_status,
            } => {
                if let Some((_, result)) =
                    replay_status.next_if(|(peeked_handle, _)| *peeked_handle == handle)
                {
                    result
                } else {
                    // It's a not completed handle!
                    AsyncResultState::NotReady
                }
            }
        }
    }
}

pub(crate) struct SysTryCompleteCombinator<C>(pub(crate) C);

impl<C> TransitionAndReturn<Context, SysTryCompleteCombinator<C>> for State
where
    C: AsyncResultCombinator,
{
    type Output = Option<AsyncResultHandle>;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        SysTryCompleteCombinator(combinator): SysTryCompleteCombinator<C>,
    ) -> Result<(Self, Self::Output), Error> {
        self.check_side_effect_guard()?;
        match self {
            State::Processing {
                ref mut async_results,
                ..
            } => {
                // Try complete the combinator
                let mut async_result_tracker =
                    AsyncResultAccessTracker(AsyncResultAccessTrackerInner::Processing {
                        known_results: async_results.get_ready_results_state(),
                        tracked_access_to_completed_results: vec![],
                        tracked_access_to_uncompleted_results: vec![],
                    });

                if let Some(combinator_result) = combinator.try_complete(&mut async_result_tracker)
                {
                    // --- Combinator is ready!

                    // Prepare the message to write out
                    let completed_entries_order = match async_result_tracker.0 {
                        AsyncResultAccessTrackerInner::Processing {
                            tracked_access_to_completed_results,
                            ..
                        } => tracked_access_to_completed_results,
                        _ => unreachable!(),
                    };
                    let message = CombinatorEntryMessage {
                        completed_entries_order: completed_entries_order
                            .into_iter()
                            .map(Into::into)
                            .collect(),
                        ..CombinatorEntryMessage::default()
                    };

                    // Let's execute the transition
                    context.journal.transition(&message);
                    let current_journal_index = context.journal.expect_index();

                    // Cache locally the Combinator result, the user will be able to access this once the ack is received.
                    async_results.insert_waiting_ack_result(
                        current_journal_index,
                        Value::CombinatorResult(combinator_result),
                    );

                    // Write out the combinator message
                    context.output.send(&message);

                    Ok((self, Some(AsyncResultHandle(current_journal_index))))
                } else {
                    // --- The combinator is not ready yet! Let's wait for more completions to come.

                    if context.input_is_closed {
                        let uncompleted_entries_order = match async_result_tracker.0 {
                            AsyncResultAccessTrackerInner::Processing {
                                tracked_access_to_uncompleted_results,
                                ..
                            } => tracked_access_to_uncompleted_results,
                            _ => unreachable!(),
                        };

                        // We can't do progress anymore, let's suspend
                        context.output.send(&SuspensionMessage {
                            entry_indexes: uncompleted_entries_order
                                .into_iter()
                                .map(Into::into)
                                .collect(),
                        });
                        context.output.send_eof();

                        Ok((State::Suspended, None))
                    } else {
                        Ok((self, None))
                    }
                }
            }
            s => {
                let expected = CombinatorEntryMessage::default();

                // We increment the index now only if we're not processing.
                context.journal.transition(&expected);
                let current_journal_index = context.journal.expect_index();

                // We should get the combinator message now
                let (mut s, msg) = s.transition_and_return(
                    context,
                    PopJournalEntry("SysTryCompleteCombinator", expected),
                )?;

                match s {
                    State::Replaying {
                        ref mut async_results,
                        ..
                    }
                    | State::Processing {
                        ref mut async_results,
                        ..
                    } => {
                        let ar_states = async_results.get_ready_results_state();

                        // Compute the replay_combinators
                        let mut replay_combinators =
                            Vec::with_capacity(msg.completed_entries_order.capacity());
                        for idx in msg.completed_entries_order {
                            let handle = AsyncResultHandle(idx);
                            let async_result_state =
                                ar_states.get(&handle).ok_or(BAD_COMBINATOR_ENTRY)?;
                            replay_combinators.push((handle, *async_result_state));
                        }

                        // Replay combinator
                        let mut async_result_tracker =
                            AsyncResultAccessTracker(AsyncResultAccessTrackerInner::Replaying {
                                replay_combinators: replay_combinators.into_iter().peekable(),
                            });
                        let combinator_result = combinator
                            .try_complete(&mut async_result_tracker)
                            .ok_or(BAD_COMBINATOR_ENTRY)?;

                        // Store the ready result
                        async_results.insert_ready_result(
                            current_journal_index,
                            Value::CombinatorResult(combinator_result),
                        );

                        Ok((s, Some(AsyncResultHandle(current_journal_index))))
                    }
                    s => {
                        Err(UnexpectedStateError::new(s.into(), "SysTryCompleteCombinator").into())
                    }
                }
            }
        }
    }
}
