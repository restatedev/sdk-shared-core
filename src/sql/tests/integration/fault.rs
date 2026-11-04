//! Injecting a storage fault, or a cancellation, into one call.
use crate::state::{
    simulation::{Effect, Event, Failpoint, FaultKv, Operation},
    Result,
};
use futures::future::{select, Either};
use std::future::Future;
use std::{
    pin::pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    thread::{self, Thread},
};

/// The failpoint at the `nth` `operation` on a key that starts with `prefix`.
pub(super) fn failpoint(
    operation: Operation,
    prefix: &str,
    nth: usize,
    effect: Effect,
    cancel: bool,
) -> Failpoint {
    Failpoint {
        operation,
        key_prefix: prefix.into(),
        nth,
        effect,
        cancel,
    }
}

/// How a call under a failpoint ended.
pub(super) enum Ended<T> {
    Returned(Result<T>),
    /// Dropped once the failpoint signalled.
    Cancelled,
}

impl<T> Ended<T> {
    /// Whether the call failed: returned an error, or was cancelled.
    pub(super) fn failed(&self) -> bool {
        !matches!(self, Ended::Returned(Ok(_)))
    }
}

/// Run `call` with `plan` armed on `store`, dropping it once the failpoint
/// signals if `plan` cancels; disarmed after. Whether the failpoint was hit,
/// and how the call ended.
pub(super) async fn under<T>(
    store: &FaultKv,
    plan: Failpoint,
    call: impl Future<Output = Result<T>>,
) -> (bool, Ended<T>) {
    let cancel = plan.cancel;
    let gate = store.arm(plan);
    let ended = match cancel {
        true => match select(pin!(call), gate).await {
            Either::Left((result, _)) => Ended::Returned(result),
            Either::Right(_) => Ended::Cancelled,
        },
        false => Ended::Returned(call.await),
    };
    let hit = store.events().iter().any(|event| event.faulted);
    store.disarm();
    (hit, ended)
}

/// The requests `call` issues to `store`, and its result.
pub(super) async fn probe<T>(
    store: &FaultKv,
    call: impl Future<Output = Result<T>>,
) -> (Vec<Event>, Result<T>) {
    let _gate = store.arm(failpoint(
        Operation::Get,
        "",
        usize::MAX,
        Effect::None,
        false,
    ));
    let result = call.await;
    let events = store.events();
    store.disarm();
    (events, result)
}

/// A failpoint at each request of `events`, each effect, cancelling or not:
/// every way one of those requests can fail.
pub(super) fn every_failure(events: &[Event]) -> Vec<Failpoint> {
    let mut plans = Vec::new();
    for (i, event) in events.iter().enumerate() {
        let nth = (events[..=i].iter())
            .filter(|e| e.operation == event.operation)
            .count();
        for effect in [Effect::None, Effect::After] {
            for cancel in [false, true] {
                plans.push(failpoint(event.operation, "", nth, effect, cancel));
            }
        }
    }
    plans
}

/// Poll `future` at most `polls` times, each time once it is woken: its
/// output if it finished by then; otherwise it is dropped, pending.
pub(super) fn poll_at_most<F: Future>(future: F, polls: usize) -> Option<F::Output> {
    struct Unpark(Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    for _ in 0..polls {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return Some(output);
        }
        thread::park();
    }
    None
}
