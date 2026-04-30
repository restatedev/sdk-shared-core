use crate::NotificationHandle;
use std::collections::HashMap;

#[derive(Debug)]
struct Run {
    command_index: u32,
    command_name: String,
    state: RunStateInner,
}

#[derive(Debug, Eq, PartialEq)]
enum RunStateInner {
    ToExecute,
    Executing,
}

#[derive(Debug, Default)]
pub(crate) struct RunState(HashMap<NotificationHandle, Run>);

impl RunState {
    pub fn insert_run_to_execute(
        &mut self,
        handle: NotificationHandle,
        command_index: u32,
        command_name: String,
    ) {
        self.0.insert(
            handle,
            Run {
                command_index,
                command_name,
                state: RunStateInner::ToExecute,
            },
        );
    }

    pub fn try_execute_run(
        &mut self,
        any_handle: &[NotificationHandle],
    ) -> Option<NotificationHandle> {
        if let Some((handle, run)) = self.0.iter_mut().find(|(handle, run)| {
            run.state == RunStateInner::ToExecute && any_handle.contains(handle)
        }) {
            run.state = RunStateInner::Executing;
            return Some(*handle);
        }
        None
    }

    pub fn get_run_info(&self, handle: &NotificationHandle) -> Option<(u32, &str)> {
        self.0
            .get(handle)
            .map(|run| (run.command_index, run.command_name.as_str()))
    }

    pub fn any_executing_in_this_set(&self, any_handle: &[NotificationHandle]) -> bool {
        any_handle.iter().any(|h| {
            self.0
                .get(h)
                .is_some_and(|r| r.state == RunStateInner::Executing)
        })
    }

    pub fn any_executing(&self) -> bool {
        self.0
            .iter()
            .any(|(_, r)| r.state == RunStateInner::Executing)
    }

    pub fn notify_execution_completed(&mut self, executed: NotificationHandle) -> (String, u32) {
        let run = self
            .0
            .remove(&executed)
            .expect("There must be a corresponding run for the given handle");
        (run.command_name, run.command_index)
    }
}
