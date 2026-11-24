use crate::explicit_recovery::vm::async_results_state::Expect;
use crate::explicit_recovery::vm::context::Context;
use crate::explicit_recovery::vm::errors::unknown_span;
use crate::explicit_recovery::vm::local_state::{LocalGet, LocalGetKeys, StateOp};
use crate::explicit_recovery::vm::span_state::SpanOp;
use crate::explicit_recovery::vm::transitions::group::{check_scope, record_fields, RecordFields};
use crate::explicit_recovery::vm::transitions::TransitionAndReturn;
use crate::explicit_recovery::vm::State;
use crate::explicit_recovery::{ActionOptions, Input, SpanId, StateScope};
use crate::service_protocol::explicit_recovery::messages::{
    self, attach_invocation_action_message, send_signal_action_message, span_end_action_message,
    AttachInvocationActionMessage, CallActionMessage, CancelInvocationActionMessage,
    ClearAllStateActionMessage, ClearStateActionMessage, FlushActionMessage, GetStateActionMessage,
    GetStateKeysActionMessage, IdempotentRequestTarget, ReadSignalActionMessage,
    SendSignalActionMessage, SetStateActionMessage, SpanEndActionMessage, SpanStartActionMessage,
    WorkflowTarget,
};
use crate::{
    AttachInvocationTarget, Error, Header, NonEmptyValue, NotificationHandle, Target, Value,
};
use bytes::Bytes;

// --- Input

pub(crate) struct GetInput;

impl TransitionAndReturn<Context, GetInput> for State {
    type Output = Input;

    fn transition_and_return(
        self,
        context: &mut Context,
        _: GetInput,
    ) -> Result<(Self, Self::Output), Error> {
        if !matches!(self, State::Running(_) | State::InAtomicGroup(..)) {
            return Err(self.as_unexpected_state("GetInput"));
        }
        let start_info = context.expect_start_info();
        let input = Input {
            invocation_id: start_info.debug_id.clone(),
            key: start_info.key.clone(),
            headers: start_info
                .headers
                .iter()
                .cloned()
                .map(Header::from)
                .collect(),
            input: start_info.input.clone(),
            scope: start_info.scope.clone(),
            limit_key: start_info.limit_key.clone(),
            idempotency_key: start_info.idempotency_key.clone(),
            attempt: start_info.attempt,
        };
        Ok((self, input))
    }
}

// --- State: the reads. Their result is what they are for, and a group cannot hold them.

pub(crate) struct CmdStateGet(
    pub(crate) StateScope,
    pub(crate) String,
    pub(crate) ActionOptions,
);

impl TransitionAndReturn<Context, CmdStateGet> for State {
    type Output = NotificationHandle;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        CmdStateGet(scope, key, options): CmdStateGet,
    ) -> Result<(Self, Self::Output), Error> {
        let State::Running(attempt) = &mut self else {
            return Err(self.as_unexpected_state("CmdStateGet"));
        };
        check_scope(context.expect_start_info(), scope)?;

        let handle = match attempt.local_state.get(scope, &key) {
            // Known here: answered without asking, which G6 makes sound.
            LocalGet::Value(value) => attempt
                .async_results
                .create_resolved_handle(Value::Success(value)),
            LocalGet::Absent => attempt.async_results.create_resolved_handle(Value::Void),
            LocalGet::Unknown => {
                let RecordFields {
                    name,
                    parent_span_id,
                    record,
                } = record_fields(options, &attempt.spans, None)?;
                let result_id = context.ids.next_result_id()?;
                let handle = attempt
                    .async_results
                    .create_handle(result_id, Expect::ValueOrVoid);
                context.output.send(&GetStateActionMessage {
                    scope: messages::StateScope::from(scope) as i32,
                    key: Bytes::from(key),
                    result_id: result_id.get(),
                    name,
                    parent_span_id,
                    record,
                });
                handle
            }
        };
        Ok((self, handle))
    }
}

pub(crate) struct CmdStateGetKeys(pub(crate) StateScope, pub(crate) ActionOptions);

impl TransitionAndReturn<Context, CmdStateGetKeys> for State {
    type Output = NotificationHandle;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        CmdStateGetKeys(scope, options): CmdStateGetKeys,
    ) -> Result<(Self, Self::Output), Error> {
        let State::Running(attempt) = &mut self else {
            return Err(self.as_unexpected_state("CmdStateGetKeys"));
        };
        check_scope(context.expect_start_info(), scope)?;

        let handle = match attempt.local_state.get_keys(scope) {
            LocalGetKeys::Keys(keys) => attempt
                .async_results
                .create_resolved_handle(Value::StateKeys(keys)),
            LocalGetKeys::Unknown => {
                let RecordFields {
                    name,
                    parent_span_id,
                    record,
                } = record_fields(options, &attempt.spans, None)?;
                let result_id = context.ids.next_result_id()?;
                let handle = attempt
                    .async_results
                    .create_handle(result_id, Expect::StateKeys);
                context.output.send(&GetStateKeysActionMessage {
                    scope: messages::StateScope::from(scope) as i32,
                    result_id: result_id.get(),
                    name,
                    parent_span_id,
                    record,
                });
                handle
            }
        };
        Ok((self, handle))
    }
}

// --- State: the writes. Their result only says "applied", and a group can hold them.

pub(crate) struct CmdStateSet(
    pub(crate) StateScope,
    pub(crate) String,
    pub(crate) Bytes,
    pub(crate) ActionOptions,
    pub(crate) bool,
);

impl TransitionAndReturn<Context, CmdStateSet> for State {
    type Output = Option<NotificationHandle>;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        CmdStateSet(scope, key, value, options, want_result): CmdStateSet,
    ) -> Result<(Self, Self::Output), Error> {
        let mut writing = self.writing(context, "CmdStateSet")?;
        check_scope(writing.start_info, scope)?;
        let RecordFields {
            name,
            parent_span_id,
            record,
        } = writing.record_fields(options)?;
        let (result_id, handle) = writing.optional_result(want_result, Expect::Void)?;
        writing.state_op(StateOp::Set(scope, key.clone(), value.clone()));
        writing.send(SetStateActionMessage {
            scope: messages::StateScope::from(scope) as i32,
            key: Bytes::from(key),
            value: Some(value.into()),
            result_id,
            name,
            parent_span_id,
            record,
        });
        Ok((self, handle))
    }
}

pub(crate) struct CmdStateClear(
    pub(crate) StateScope,
    pub(crate) String,
    pub(crate) ActionOptions,
    pub(crate) bool,
);

impl TransitionAndReturn<Context, CmdStateClear> for State {
    type Output = Option<NotificationHandle>;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        CmdStateClear(scope, key, options, want_result): CmdStateClear,
    ) -> Result<(Self, Self::Output), Error> {
        let mut writing = self.writing(context, "CmdStateClear")?;
        check_scope(writing.start_info, scope)?;
        let RecordFields {
            name,
            parent_span_id,
            record,
        } = writing.record_fields(options)?;
        let (result_id, handle) = writing.optional_result(want_result, Expect::Void)?;
        writing.state_op(StateOp::Clear(scope, key.clone()));
        writing.send(ClearStateActionMessage {
            scope: messages::StateScope::from(scope) as i32,
            key: Bytes::from(key),
            result_id,
            name,
            parent_span_id,
            record,
        });
        Ok((self, handle))
    }
}

pub(crate) struct CmdStateClearAll(
    pub(crate) StateScope,
    pub(crate) ActionOptions,
    pub(crate) bool,
);

impl TransitionAndReturn<Context, CmdStateClearAll> for State {
    type Output = Option<NotificationHandle>;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        CmdStateClearAll(scope, options, want_result): CmdStateClearAll,
    ) -> Result<(Self, Self::Output), Error> {
        let mut writing = self.writing(context, "CmdStateClearAll")?;
        check_scope(writing.start_info, scope)?;
        let RecordFields {
            name,
            parent_span_id,
            record,
        } = writing.record_fields(options)?;
        let (result_id, handle) = writing.optional_result(want_result, Expect::Void)?;
        writing.state_op(StateOp::ClearAll(scope));
        writing.send(ClearAllStateActionMessage {
            scope: messages::StateScope::from(scope) as i32,
            result_id,
            name,
            parent_span_id,
            record,
        });
        Ok((self, handle))
    }
}

// --- Calls, sends and invocations

/// A call, or with `false` a send: a call without a result id.
pub(crate) struct CmdCall(
    pub(crate) Target,
    pub(crate) Bytes,
    /// Invoke time, in milliseconds since the UNIX epoch; 0: as soon as possible.
    pub(crate) u64,
    pub(crate) ActionOptions,
    pub(crate) bool,
);

impl TransitionAndReturn<Context, CmdCall> for State {
    /// The handle of the invocation id, and that of what the callee returned, if wanted.
    type Output = (NotificationHandle, Option<NotificationHandle>);

    fn transition_and_return(
        mut self,
        context: &mut Context,
        CmdCall(target, input, invoke_time, options, want_result): CmdCall,
    ) -> Result<(Self, Self::Output), Error> {
        let mut writing = self.writing(context, "CmdCall")?;
        let RecordFields {
            name,
            parent_span_id,
            record,
        } = writing.record_fields(options)?;
        // The invocation id always comes, and comes first.
        let (invocation_id_result_id, invocation_id_handle) =
            writing.result(Expect::InvocationId)?;
        let (result_id, result_handle) =
            writing.optional_result(want_result, Expect::ValueOrFailure)?;
        writing.send(CallActionMessage {
            service_name: target.service,
            handler_name: target.handler,
            key: target.key.unwrap_or_default(),
            parameter: Some(input.into()),
            headers: target
                .headers
                .into_iter()
                .map(messages::Header::from)
                .collect(),
            idempotency_key: target.idempotency_key,
            scope: target.scope,
            limit_key: target.limit_key,
            invoke_time,
            invocation_id_result_id,
            result_id,
            name,
            parent_span_id,
            record,
        });
        Ok((self, (invocation_id_handle, result_handle)))
    }
}

pub(crate) struct CmdAttachInvocation(pub(crate) AttachInvocationTarget, pub(crate) ActionOptions);

impl TransitionAndReturn<Context, CmdAttachInvocation> for State {
    type Output = NotificationHandle;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        CmdAttachInvocation(target, options): CmdAttachInvocation,
    ) -> Result<(Self, Self::Output), Error> {
        let State::Running(attempt) = &mut self else {
            return Err(self.as_unexpected_state("CmdAttachInvocation"));
        };
        let RecordFields {
            name,
            parent_span_id,
            record,
        } = record_fields(options, &attempt.spans, None)?;
        let result_id = context.ids.next_result_id()?;
        let handle = attempt
            .async_results
            .create_handle(result_id, Expect::ValueOrFailure);
        context.output.send(&AttachInvocationActionMessage {
            target: Some(match target {
                AttachInvocationTarget::InvocationId(id) => {
                    attach_invocation_action_message::Target::InvocationId(id)
                }
                AttachInvocationTarget::WorkflowId { name, key, scope } => {
                    attach_invocation_action_message::Target::WorkflowTarget(WorkflowTarget {
                        workflow_name: name,
                        workflow_key: key,
                        scope,
                    })
                }
                AttachInvocationTarget::IdempotencyId {
                    service_name,
                    service_key,
                    handler_name,
                    idempotency_key,
                    scope,
                } => attach_invocation_action_message::Target::IdempotentRequestTarget(
                    IdempotentRequestTarget {
                        service_name,
                        service_key,
                        handler_name,
                        idempotency_key,
                        scope,
                    },
                ),
            }),
            result_id: result_id.get(),
            name,
            parent_span_id,
            record,
        });
        Ok((self, handle))
    }
}

pub(crate) struct CmdCancelInvocation(pub(crate) String, pub(crate) ActionOptions, pub(crate) bool);

impl TransitionAndReturn<Context, CmdCancelInvocation> for State {
    type Output = Option<NotificationHandle>;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        CmdCancelInvocation(invocation_id, options, want_result): CmdCancelInvocation,
    ) -> Result<(Self, Self::Output), Error> {
        let mut writing = self.writing(context, "CmdCancelInvocation")?;
        let RecordFields {
            name,
            parent_span_id,
            record,
        } = writing.record_fields(options)?;
        let (result_id, handle) = writing.optional_result(want_result, Expect::Void)?;
        writing.send(CancelInvocationActionMessage {
            invocation_id,
            result_id,
            name,
            parent_span_id,
            record,
        });
        Ok((self, handle))
    }
}

// --- Signals

pub(crate) struct CmdSignalRead(pub(crate) String, pub(crate) u32, pub(crate) ActionOptions);

impl TransitionAndReturn<Context, CmdSignalRead> for State {
    type Output = NotificationHandle;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        CmdSignalRead(signal_name, signal_index, options): CmdSignalRead,
    ) -> Result<(Self, Self::Output), Error> {
        let State::Running(attempt) = &mut self else {
            return Err(self.as_unexpected_state("CmdSignalRead"));
        };
        let RecordFields {
            name,
            parent_span_id,
            record,
        } = record_fields(options, &attempt.spans, None)?;
        let result_id = context.ids.next_result_id()?;
        let handle = attempt
            .async_results
            .create_handle(result_id, Expect::VoidValueOrFailure);
        context.output.send(&ReadSignalActionMessage {
            signal_name,
            signal_index,
            result_id: result_id.get(),
            name,
            parent_span_id,
            record,
        });
        Ok((self, handle))
    }
}

pub(crate) struct CmdSignalSend(
    pub(crate) String,
    pub(crate) String,
    pub(crate) Option<NonEmptyValue>,
    pub(crate) ActionOptions,
    pub(crate) bool,
);

impl TransitionAndReturn<Context, CmdSignalSend> for State {
    type Output = Option<NotificationHandle>;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        CmdSignalSend(target_invocation_id, signal_name, value, options, want_result): CmdSignalSend,
    ) -> Result<(Self, Self::Output), Error> {
        let mut writing = self.writing(context, "CmdSignalSend")?;
        let RecordFields {
            name,
            parent_span_id,
            record,
        } = writing.record_fields(options)?;
        let (result_id, handle) = writing.optional_result(want_result, Expect::Void)?;
        writing.send(SendSignalActionMessage {
            target_invocation_id,
            signal_name,
            result: Some(match value {
                None => send_signal_action_message::Result::Void(Default::default()),
                Some(NonEmptyValue::Success(value)) => {
                    send_signal_action_message::Result::Value(value.into())
                }
                Some(NonEmptyValue::Failure(failure)) => {
                    send_signal_action_message::Result::Failure(failure.into())
                }
            }),
            result_id,
            name,
            parent_span_id,
            record,
        });
        Ok((self, handle))
    }
}

// --- Flush

pub(crate) struct CmdFlush(pub(crate) ActionOptions);

impl TransitionAndReturn<Context, CmdFlush> for State {
    type Output = NotificationHandle;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        CmdFlush(options): CmdFlush,
    ) -> Result<(Self, Self::Output), Error> {
        let State::Running(attempt) = &mut self else {
            return Err(self.as_unexpected_state("CmdFlush"));
        };
        let RecordFields {
            name,
            parent_span_id,
            record,
        } = record_fields(options, &attempt.spans, None)?;
        let result_id = context.ids.next_result_id()?;
        let handle = attempt.async_results.create_handle(result_id, Expect::Void);
        context.output.send(&FlushActionMessage {
            result_id: result_id.get(),
            name,
            parent_span_id,
            record,
        });
        Ok((self, handle))
    }
}

// --- Spans

pub(crate) struct SpanStart(pub(crate) String, pub(crate) Option<SpanId>);

impl TransitionAndReturn<Context, SpanStart> for State {
    type Output = SpanId;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        SpanStart(name, parent): SpanStart,
    ) -> Result<(Self, Self::Output), Error> {
        let mut writing = self.writing(context, "SpanStart")?;
        // The span's name and parent are the action's own.
        let RecordFields { parent_span_id, .. } = writing.record_fields(ActionOptions {
            parent_span: parent,
            ..Default::default()
        })?;
        let span_id = writing.ids.next_span_id();
        writing.span_op(SpanOp::Start(span_id));
        writing.send(SpanStartActionMessage {
            span_id: span_id.get(),
            name,
            parent_span_id,
            ..Default::default()
        });
        Ok((self, SpanId::from(span_id)))
    }
}

pub(crate) struct SpanEnd(pub(crate) SpanId, pub(crate) Option<NonEmptyValue>);

impl TransitionAndReturn<Context, SpanEnd> for State {
    type Output = ();

    fn transition_and_return(
        mut self,
        context: &mut Context,
        SpanEnd(span, result): SpanEnd,
    ) -> Result<(Self, Self::Output), Error> {
        let mut writing = self.writing(context, "SpanEnd")?;
        if !writing.span_is_open(span.0) {
            return Err(unknown_span(span));
        }
        writing.span_op(SpanOp::End(span.0));
        writing.send(SpanEndActionMessage {
            span_id: span.0.get(),
            result: Some(match result {
                None => span_end_action_message::Result::Void(Default::default()),
                Some(NonEmptyValue::Success(value)) => {
                    span_end_action_message::Result::Value(value.into())
                }
                Some(NonEmptyValue::Failure(failure)) => {
                    span_end_action_message::Result::Failure(failure.into())
                }
            }),
            ..Default::default()
        });
        Ok((self, ()))
    }
}
