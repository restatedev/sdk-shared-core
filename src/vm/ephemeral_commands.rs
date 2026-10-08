use crate::{EphemeralCompletionId, Value};
use std::collections::HashMap;
use tracing::warn;

/// Tracks ephemeral commands, that is commands that are neither recorded in the journal, nor replayed,
/// such as the ephemeral state get. Their results are delivered as ephemeral notifications.
///
/// Ephemeral completion ids are scoped to the current attempt and are detached from journaled completion ids.
#[derive(Debug, Default)]
pub(crate) struct EphemeralCommands {
    last_completion_id: u32,
    /// `None` when the command is in-flight (sent to the runtime), `Some` when its notification is ready to be taken.
    commands: HashMap<u32, Option<Value>>,
}

impl EphemeralCommands {
    pub(crate) fn next_completion_id(&mut self) -> u32 {
        self.last_completion_id += 1;
        self.last_completion_id
    }

    pub(crate) fn register_in_flight(&mut self, completion_id: u32) {
        self.commands.insert(completion_id, None);
    }

    pub(crate) fn register_ready(&mut self, completion_id: u32, value: Value) {
        self.commands.insert(completion_id, Some(value));
    }

    pub(crate) fn complete(&mut self, completion_id: u32, value: Value) {
        match self.commands.get_mut(&completion_id) {
            Some(slot) => {
                *slot = Some(value);
            }
            _ => {
                warn!(
                    "Received an ephemeral notification for the ephemeral completion id {completion_id}, but there is no in-flight ephemeral command with this id. Ignoring it."
                );
            }
        }
    }

    pub(crate) fn any_ready(&self) -> Option<EphemeralCompletionId> {
        self.commands
            .iter()
            .find(|(_, v)| v.is_some())
            .map(|(id, _)| EphemeralCompletionId::from(*id))
    }

    pub(crate) fn take_notification(
        &mut self,
        completion_id: EphemeralCompletionId,
    ) -> Option<Value> {
        let completion_id = u32::from(completion_id);
        if matches!(self.commands.get(&completion_id), Some(Some(_))) {
            self.commands.remove(&completion_id).flatten()
        } else {
            None
        }
    }

    pub(crate) fn has_in_flight(&self) -> bool {
        self.commands.values().any(Option::is_none)
    }
}
