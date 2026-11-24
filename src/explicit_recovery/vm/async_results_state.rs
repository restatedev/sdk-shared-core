use crate::explicit_recovery::Notification;
use crate::service_protocol::explicit_recovery::messages::result_notification_message;
use crate::{NotificationHandle, Value, CANCEL_NOTIFICATION_HANDLE};
use std::collections::{HashMap, VecDeque};
use std::num::NonZero;

/// The variants of `ResultNotificationMessage.result` an action allows: what the proto says on
/// its `result_id`.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum Expect {
    Void,
    ValueOrVoid,
    ValueOrFailure,
    VoidValueOrFailure,
    InvocationId,
    StateKeys,
}

impl Expect {
    fn allows(&self, result: &result_notification_message::Result) -> bool {
        use result_notification_message::Result;
        matches!(
            (self, result),
            (Expect::Void, Result::Void(_))
                | (Expect::ValueOrVoid, Result::Value(_) | Result::Void(_))
                | (
                    Expect::ValueOrFailure,
                    Result::Value(_) | Result::Failure(_)
                )
                | (
                    Expect::VoidValueOrFailure,
                    Result::Void(_) | Result::Value(_) | Result::Failure(_)
                )
                | (Expect::InvocationId, Result::InvocationId(_))
                | (Expect::StateKeys, Result::StateKeys(_))
        )
    }
}

pub(crate) enum Notified {
    Ok,
    /// No action of this attempt waits for that result id.
    UnknownResultId,
    /// The result is of a variant the action does not allow.
    UnexpectedVariant(Expect),
}

#[derive(Debug)]
struct Ready {
    notification: Notification,
    /// Whether the runtime sent it: those are counted in `ShutdownMessage`. A result the VM knew
    /// by itself (a read answered from the state it holds) is not.
    from_runtime: bool,
}

/// The results of the attempt: which handle each result id notifies, and the notifications ready
/// to be taken, in the order they arrived.
#[derive(Debug)]
pub(crate) struct AsyncResultsState {
    pending: HashMap<NonZero<u32>, (NotificationHandle, Expect)>,
    ready: VecDeque<Ready>,
    next_notification_handle: u32,
    /// How many of the notifications the runtime sent were handed out:
    /// `ShutdownMessage.processed_notifications`.
    processed_notifications: u32,
}

impl Default for AsyncResultsState {
    fn default() -> Self {
        Self {
            pending: Default::default(),
            ready: Default::default(),
            // 1 to 16 are reserved, as CoreVM's built-in signals: 1 is the cancel signal.
            next_notification_handle: 17,
            processed_notifications: 0,
        }
    }
}

impl AsyncResultsState {
    /// A handle for the result `result_id`.
    pub(crate) fn create_handle(
        &mut self,
        result_id: NonZero<u32>,
        expect: Expect,
    ) -> NotificationHandle {
        let handle = self.next_handle();
        self.pending.insert(result_id, (handle, expect));
        handle
    }

    /// A handle for a result the VM knows by itself, ready at once.
    pub(crate) fn create_resolved_handle(&mut self, value: Value) -> NotificationHandle {
        let handle = self.next_handle();
        self.ready.push_back(Ready {
            notification: Notification { handle, value },
            from_runtime: false,
        });
        handle
    }

    /// The runtime sent the result `result_id`.
    pub(crate) fn notify_result(
        &mut self,
        result_id: NonZero<u32>,
        result: result_notification_message::Result,
    ) -> Notified {
        let Some((_, expect)) = self.pending.get(&result_id) else {
            return Notified::UnknownResultId;
        };
        if !expect.allows(&result) {
            return Notified::UnexpectedVariant(*expect);
        }
        let (handle, _) = self.pending.remove(&result_id).unwrap();
        self.ready.push_back(Ready {
            notification: Notification {
                handle,
                value: result.into(),
            },
            from_runtime: true,
        });
        Notified::Ok
    }

    /// The runtime sent the cancel signal.
    pub(crate) fn notify_cancel(&mut self) {
        self.ready.push_back(Ready {
            notification: Notification {
                handle: CANCEL_NOTIFICATION_HANDLE,
                value: Value::Void,
            },
            from_runtime: true,
        });
    }

    /// Hand out the next notification, in arrival order.
    pub(crate) fn take_next(&mut self) -> Option<Notification> {
        let ready = self.ready.pop_front()?;
        if ready.from_runtime {
            self.processed_notifications += 1;
        }
        Some(ready.notification)
    }

    /// Forget `handle`, of an action that was never sent.
    pub(crate) fn forget(&mut self, handle: NotificationHandle) {
        self.pending.retain(|_, (pending, _)| *pending != handle);
        self.ready.retain(|r| r.notification.handle != handle);
    }

    pub(crate) fn processed_notifications(&self) -> u32 {
        self.processed_notifications
    }

    fn next_handle(&mut self) -> NotificationHandle {
        let handle = NotificationHandle::from(self.next_notification_handle);
        self.next_notification_handle += 1;
        handle
    }
}
