use crate::service_protocol::CompletionId;
use std::collections::HashSet;

/// The traces of the attempt, by the completion id of their run.
#[derive(Debug, Default)]
pub(crate) struct TraceState {
    open: HashSet<CompletionId>,
    started: HashSet<CompletionId>,
}

impl TraceState {
    pub(crate) fn start(&mut self, completion_id: CompletionId) {
        self.open.insert(completion_id);
        self.started.insert(completion_id);
    }

    /// Whether this attempt started the trace, open or ended.
    pub(crate) fn is_started(&self, completion_id: CompletionId) -> bool {
        self.started.contains(&completion_id)
    }

    /// Returns false if the trace is not open.
    pub(crate) fn end(&mut self, completion_id: CompletionId) -> bool {
        self.open.remove(&completion_id)
    }

    /// The open traces.
    pub(crate) fn end_dangling_traces(self) -> Vec<CompletionId> {
        self.open.into_iter().collect()
    }
}
