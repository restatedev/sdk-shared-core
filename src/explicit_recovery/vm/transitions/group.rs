use crate::explicit_recovery::vm::async_results_state::{AsyncResultsState, Expect};
use crate::explicit_recovery::vm::context::{Context, Ids, Output, StartInfo};
use crate::explicit_recovery::vm::errors::{
    no_open_atomic_group, object_state_without_key, unknown_span,
};
use crate::explicit_recovery::vm::local_state::StateOp;
use crate::explicit_recovery::vm::span_state::{SpanOp, SpanState};
use crate::explicit_recovery::vm::transitions::{Transition, TransitionAndReturn};
use crate::explicit_recovery::vm::{OpenGroup, State};
use crate::explicit_recovery::{ActionOptions, RecordLevel, SpanId, StateScope};
use crate::service_protocol::explicit_recovery::messages::{
    self, AtomicGroupActionMessage, FlushActionMessage, GroupableAction,
};
use crate::{Error, NotificationHandle};
use std::num::NonZero;

/// The three fields every action has, beside its result id.
pub(super) struct RecordFields {
    pub(super) name: String,
    pub(super) parent_span_id: u32,
    pub(super) record: i32,
}

impl From<RecordLevel> for messages::RecordLevel {
    fn from(value: RecordLevel) -> Self {
        match value {
            RecordLevel::Default => messages::RecordLevel::RecordDefault,
            RecordLevel::None => messages::RecordLevel::RecordNone,
            RecordLevel::Summary => messages::RecordLevel::RecordSummary,
            RecordLevel::Full => messages::RecordLevel::RecordFull,
        }
    }
}

impl From<StateScope> for messages::StateScope {
    fn from(value: StateScope) -> Self {
        match value {
            StateScope::Object => messages::StateScope::Object,
            StateScope::Invocation => messages::StateScope::Invocation,
        }
    }
}

/// Whether the attempt started the span `id`: also in the open group, if there is one.
fn span_is_started(spans: &SpanState, group: Option<&OpenGroup>, id: NonZero<u32>) -> bool {
    spans.is_started(id)
        || group.is_some_and(|group| {
            group
                .span_ops
                .iter()
                .any(|op| matches!(op, SpanOp::Start(started) if *started == id))
        })
}

pub(super) fn record_fields(
    options: ActionOptions,
    spans: &SpanState,
    group: Option<&OpenGroup>,
) -> Result<RecordFields, Error> {
    if let Some(parent) = options.parent_span {
        if !span_is_started(spans, group, parent.0) {
            return Err(unknown_span(parent));
        }
    }
    Ok(RecordFields {
        name: options.name.unwrap_or_default(),
        parent_span_id: options.parent_span.map(u32::from).unwrap_or_default(),
        record: messages::RecordLevel::from(options.record) as i32,
    })
}

pub(super) fn check_scope(start_info: &StartInfo, scope: StateScope) -> Result<(), Error> {
    if scope == StateScope::Object && start_info.key.is_empty() {
        return Err(object_state_without_key());
    }
    Ok(())
}

/// What an action an atomic group can hold writes to: the output, or the open group.
pub(super) struct Writing<'a> {
    pub(super) async_results: &'a mut AsyncResultsState,
    pub(super) ids: &'a mut Ids,
    pub(super) start_info: &'a StartInfo,
    local_state: &'a mut crate::explicit_recovery::vm::local_state::LocalState,
    spans: &'a mut SpanState,
    group: Option<&'a mut OpenGroup>,
    output: &'a mut Output,
    keep_written_values_up_to: usize,
}

impl State {
    /// The attempt, for an action that an atomic group can hold: `event`.
    pub(super) fn writing<'a>(
        &'a mut self,
        context: &'a mut Context,
        event: &'static str,
    ) -> Result<Writing<'a>, Error> {
        let Context {
            start_info,
            ids,
            output,
            options,
            ..
        } = context;
        let start_info = start_info.as_ref().expect("state is not WaitingStart");
        let (attempt, group) = match self {
            State::Running(attempt) => (attempt, None),
            State::InAtomicGroup(attempt, group) => (attempt, Some(group)),
            s => return Err(s.as_unexpected_state(event)),
        };
        Ok(Writing {
            async_results: &mut attempt.async_results,
            ids,
            start_info,
            local_state: &mut attempt.local_state,
            spans: &mut attempt.spans,
            group,
            output,
            keep_written_values_up_to: options.keep_written_values_up_to,
        })
    }
}

impl Writing<'_> {
    pub(super) fn record_fields(&self, options: ActionOptions) -> Result<RecordFields, Error> {
        record_fields(options, self.spans, self.group.as_deref())
    }

    pub(super) fn span_is_open(&self, id: NonZero<u32>) -> bool {
        let mut open = self.spans.is_open(id);
        for op in self.group.iter().flat_map(|group| group.span_ops.iter()) {
            match op {
                SpanOp::Start(started) if *started == id => open = true,
                SpanOp::End(ended) if *ended == id => open = false,
                _ => {}
            }
        }
        open
    }

    /// A result id and its handle, for a result the action always has.
    pub(super) fn result(&mut self, expect: Expect) -> Result<(u32, NotificationHandle), Error> {
        let result_id = self.ids.next_result_id()?;
        let handle = self.async_results.create_handle(result_id, expect);
        if let Some(group) = &mut self.group {
            group.handles.push(handle);
        }
        Ok((result_id.get(), handle))
    }

    /// A result id and its handle if the caller wants the result; else 0, the absent id.
    pub(super) fn optional_result(
        &mut self,
        want_result: bool,
        expect: Expect,
    ) -> Result<(u32, Option<NotificationHandle>), Error> {
        if !want_result {
            return Ok((0, None));
        }
        let (result_id, handle) = self.result(expect)?;
        Ok((result_id, Some(handle)))
    }

    /// Send the action, or keep it in the open group.
    pub(super) fn send<M: GroupableAction>(&mut self, msg: M) {
        match &mut self.group {
            Some(group) => group.actions.push(msg.into_group_action()),
            None => self.output.send(&msg),
        }
    }

    /// The change the action makes to the state the VM holds: now, or once the group is closed.
    pub(super) fn state_op(&mut self, op: StateOp) {
        match &mut self.group {
            Some(group) => group.state_ops.push(op),
            None => self.local_state.apply(op, self.keep_written_values_up_to),
        }
    }

    pub(super) fn span_op(&mut self, op: SpanOp) {
        match &mut self.group {
            Some(group) => group.span_ops.push(op),
            None => self.spans.apply(op),
        }
    }
}

pub(crate) struct AtomicGroupStart;

impl Transition<Context, AtomicGroupStart> for State {
    fn transition(self, _: &mut Context, _: AtomicGroupStart) -> Result<Self, Error> {
        match self {
            State::Running(attempt) => Ok(State::InAtomicGroup(attempt, OpenGroup::default())),
            // Also a group inside a group.
            s => Err(s.as_unexpected_state("AtomicGroupStart")),
        }
    }
}

pub(crate) struct AtomicGroupEnd(pub(crate) ActionOptions, pub(crate) bool);

impl TransitionAndReturn<Context, AtomicGroupEnd> for State {
    type Output = Option<NotificationHandle>;

    fn transition_and_return(
        self,
        context: &mut Context,
        AtomicGroupEnd(options, want_result): AtomicGroupEnd,
    ) -> Result<(Self, Self::Output), Error> {
        let (mut attempt, group) = match self {
            State::InAtomicGroup(attempt, group) => (attempt, group),
            State::Running(_) => return Err(no_open_atomic_group("AtomicGroupEnd")),
            s => return Err(s.as_unexpected_state("AtomicGroupEnd")),
        };
        let RecordFields {
            name,
            parent_span_id,
            record,
        } = record_fields(options, &attempt.spans, None)?;
        // The group's id comes after those of the actions in it.
        let (result_id, handle) = if want_result {
            let result_id = context.ids.next_result_id()?;
            let handle = attempt.async_results.create_handle(result_id, Expect::Void);
            (result_id.get(), Some(handle))
        } else {
            (0, None)
        };

        if group.actions.is_empty() {
            // A group holds at least one action. An empty one whose result is wanted is a
            // flush: its result says the same, that everything before it is applied.
            if want_result {
                context.output.send(&FlushActionMessage {
                    result_id,
                    name,
                    parent_span_id,
                    record,
                });
            }
        } else {
            // Sent: from now on the attempt's own reads see what the group changes.
            for op in group.state_ops {
                attempt
                    .local_state
                    .apply(op, context.options.keep_written_values_up_to);
            }
            for op in group.span_ops {
                attempt.spans.apply(op);
            }
            context.output.send(&AtomicGroupActionMessage {
                actions: group.actions,
                result_id,
                name,
                parent_span_id,
                record,
            });
        }
        Ok((State::Running(attempt), handle))
    }
}

/// Drop the open group: none of its actions was sent, and their handles are never notified.
pub(crate) struct AtomicGroupAbort;

impl TransitionAndReturn<Context, AtomicGroupAbort> for State {
    type Output = Vec<NotificationHandle>;

    fn transition_and_return(
        self,
        _: &mut Context,
        _: AtomicGroupAbort,
    ) -> Result<(Self, Self::Output), Error> {
        match self {
            State::InAtomicGroup(mut attempt, group) => {
                for handle in &group.handles {
                    attempt.async_results.forget(*handle);
                }
                Ok((State::Running(attempt), group.handles))
            }
            State::Running(_) => Err(no_open_atomic_group("AtomicGroupAbort")),
            s => Err(s.as_unexpected_state("AtomicGroupAbort")),
        }
    }
}

// SpanId is only built from ids this attempt handed out.
impl From<NonZero<u32>> for SpanId {
    fn from(value: NonZero<u32>) -> Self {
        SpanId(value)
    }
}
