use crate::explicit_recovery::Notification;
use crate::service_protocol::CompletionId;
use crate::{NotificationHandle, Value};
use std::collections::HashMap;
use std::num::NonZero;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub(crate) enum NotificationId {
    CompletionId(NonZero<CompletionId>),
    EphemeralCompletionId(NonZero<u32>),
    CommandAck(NonZero<u32>),
}

/// The async results of the attempt: which handle each response notifies, and the notifications
/// ready to be taken, in the order they arrived.
///
/// Unlike CoreVM's, there is no future to resolve: a notification is ready as soon as its response
/// arrives (or at once, for a result known locally), and the SDK takes them in order.
#[derive(Debug)]
pub(crate) struct AsyncResultsState {
    async_results: HashMap<NotificationId, (NotificationHandle, Option<Value>)>,

    next_notification_handle: u32,
}

impl Default for AsyncResultsState {
    fn default() -> Self {
        Self {
            async_results: Default::default(),
            // 1 to 16 are reserved, as CoreVM's built-in signals.
            next_notification_handle: 17,
        }
    }
}

impl AsyncResultsState {
    /// A handle for the given notification id.
    pub(crate) fn create_handle(&mut self, notification_id: NotificationId) -> NotificationHandle {
        let handle = self.next_handle();
        self.async_results.insert(notification_id, (handle, None));
        handle
    }

    /// A handle for the given notification id, already resolved.
    pub(crate) fn create_resolved_handle(
        &mut self,
        notification_id: NotificationId,
        result: Value,
    ) -> NotificationHandle {
        let handle = self.next_handle();
        self.async_results
            .insert(notification_id, (handle, Some(result)));
        handle
    }

    /// Returns false if result is dropped because the handle didnt exist.
    pub(crate) fn notify_result(
        &mut self,
        notification_id: NotificationId,
        value: impl Into<Value>,
    ) -> bool {
        let Some((_, result)) = self.async_results.get_mut(&notification_id) else {
            return false;
        };
        *result = Some(value.into());
        true
    }

    // Take any ready notification.
    pub(crate) fn take_ready(&mut self) -> Option<Notification> {
        self.async_results
            .extract_if(|_, (_, result)| result.is_some())
            .next()
            .map(|(_, (handle, result))| Notification {
                handle,
                value: result.unwrap(),
            })
    }

    /// The completion ids of the pending responses.
    pub(crate) fn waiting_notifications(&self) -> Vec<NotificationId> {
        self.async_results
            .iter()
            .filter_map(|(notif_id, (_, result))| {
                if result.is_none() {
                    Some(*notif_id)
                } else {
                    None
                }
            })
            .collect()
    }

    fn next_handle(&mut self) -> NotificationHandle {
        let handle = NotificationHandle::from(self.next_notification_handle);
        self.next_notification_handle += 1;
        handle
    }
}
