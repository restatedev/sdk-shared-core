use std::collections::HashSet;
use std::num::NonZero;

/// A change this attempt makes to its spans.
#[derive(Debug, Clone, Copy)]
pub(crate) enum SpanOp {
    Start(NonZero<u32>),
    End(NonZero<u32>),
}

/// The spans of the attempt, by id. The runtime closes those the attempt leaves open.
#[derive(Debug, Default)]
pub(crate) struct SpanState {
    open: HashSet<NonZero<u32>>,
    started: HashSet<NonZero<u32>>,
}

impl SpanState {
    pub(crate) fn apply(&mut self, op: SpanOp) {
        match op {
            SpanOp::Start(id) => {
                self.open.insert(id);
                self.started.insert(id);
            }
            SpanOp::End(id) => {
                self.open.remove(&id);
            }
        }
    }

    /// Whether this attempt started the span, open or ended: it can be a parent.
    pub(crate) fn is_started(&self, id: NonZero<u32>) -> bool {
        self.started.contains(&id)
    }

    pub(crate) fn is_open(&self, id: NonZero<u32>) -> bool {
        self.open.contains(&id)
    }
}
