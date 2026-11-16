use crate::explicit_recovery::vm::async_results_state::NotificationId;
use crate::explicit_recovery::vm::context::{Context, EagerGetState, EagerGetStateKeys};
use crate::explicit_recovery::vm::errors::unknown_trace;
use crate::explicit_recovery::vm::transitions::TransitionAndReturn;
use crate::explicit_recovery::vm::State;
use crate::explicit_recovery::{CallHandle, SendHandle, TraceId};
use crate::service_protocol::messages::{
    attach_invocation_command_message, propose_run_completion_message, send_signal_command_message,
    AttachInvocationCommandMessage, CallCommandMessage, ClearAllStateCommandMessage,
    ClearStateCommandMessage, GetLazyStateCommandMessage, GetLazyStateKeysCommandMessage,
    GetStateEphemeralCommandMessage, IdempotentRequestTarget, OneWayCallCommandMessage,
    ProposeRunCompletionMessage, RunCommandMessage, SendSignalCommandMessage,
    SetStateCommandMessage, WithFlags, WorkflowTarget,
};
use crate::service_protocol::{MessageFlags, Version, CANCEL_SIGNAL_ID};
use crate::{
    AttachInvocationTarget, Error, Header, Input, NonEmptyValue, NotificationHandle, Target, Value,
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
        let State::Processing { ref input, .. } = self else {
            return Err(self.as_unexpected_state("SysInput"));
        };
        let start_info = context.expect_start_info();
        let input = Input {
            invocation_id: start_info.debug_id.clone(),
            random_seed: start_info.random_seed,
            key: start_info.key.clone(),
            headers: input.headers.iter().cloned().map(Header::from).collect(),
            input: input
                .value
                .as_ref()
                .map(|v| v.content.clone())
                .unwrap_or_default(),
            scope: start_info.scope.clone(),
            limit_key: start_info.limit_key.clone(),
            idempotency_key: start_info.idempotency_key.clone(),
        };
        Ok((self, input))
    }
}

// --- State

pub(crate) struct CmdStateGet(pub(crate) String, pub(crate) Option<String>);

impl TransitionAndReturn<Context, CmdStateGet> for State {
    type Output = NotificationHandle;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        CmdStateGet(key, name): CmdStateGet,
    ) -> Result<(Self, Self::Output), Error> {
        let State::Processing {
            ref mut async_results,
            ref eager_state,
            ..
        } = self
        else {
            return Err(self.as_unexpected_state("SysStateGet"));
        };

        let handle = match eager_state.get(&key) {
            EagerGetState::Value(value) => async_results.create_resolved_handle(
                NotificationId::EphemeralCompletionId(
                    context.journal.next_ephemeral_completion_id(),
                ),
                Value::Success(value),
            ),
            EagerGetState::Empty => async_results.create_resolved_handle(
                NotificationId::EphemeralCompletionId(
                    context.journal.next_ephemeral_completion_id(),
                ),
                Value::Void,
            ),
            EagerGetState::Unknown if context.negotiated_protocol_version >= Version::V8 => {
                let ephemeral_completion_id = context.journal.next_ephemeral_completion_id();
                let handle = async_results.create_handle(NotificationId::EphemeralCompletionId(
                    ephemeral_completion_id,
                ));
                context.output.send(&GetStateEphemeralCommandMessage {
                    ephemeral_completion_id: ephemeral_completion_id.into(),
                    key: Bytes::from(key),
                });
                handle
            }
            EagerGetState::Unknown => {
                // This command's index in the journal.
                context.journal.next_command_index();
                let result_completion_id = context.journal.next_completion_notification_id();
                let handle =
                    async_results.create_handle(NotificationId::CompletionId(result_completion_id));
                let msg = &GetLazyStateCommandMessage {
                    key: Bytes::from(key),
                    result_completion_id: result_completion_id.into(),
                    name: name.unwrap_or_default(),
                };
                context.output.send(msg);
                handle
            }
        };
        Ok((self, handle))
    }
}

pub(crate) struct CmdStateGetKeys(pub(crate) Option<String>);

impl TransitionAndReturn<Context, CmdStateGetKeys> for State {
    type Output = NotificationHandle;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        CmdStateGetKeys(name): CmdStateGetKeys,
    ) -> Result<(Self, Self::Output), Error> {
        let State::Processing {
            ref mut async_results,
            ref eager_state,
            ..
        } = self
        else {
            return Err(self.as_unexpected_state("SysStateGetKeys"));
        };

        let handle = match eager_state.get_keys() {
            EagerGetStateKeys::Keys(keys) => async_results.create_resolved_handle(
                NotificationId::EphemeralCompletionId(
                    context.journal.next_ephemeral_completion_id(),
                ),
                Value::StateKeys(keys),
            ),
            EagerGetStateKeys::Unknown => {
                // This command's index in the journal.
                context.journal.next_command_index();
                let result_completion_id = context.journal.next_completion_notification_id();
                let handle =
                    async_results.create_handle(NotificationId::CompletionId(result_completion_id));
                let msg = &GetLazyStateKeysCommandMessage {
                    result_completion_id: result_completion_id.into(),
                    name: name.unwrap_or_default(),
                };
                context.output.send(msg);
                handle
            }
        };
        Ok((self, handle))
    }
}

pub(crate) struct CmdStateSet(
    pub(crate) String,
    pub(crate) Bytes,
    pub(crate) Option<String>,
    pub(crate) bool,
);

impl TransitionAndReturn<Context, CmdStateSet> for State {
    type Output = NotificationHandle;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        CmdStateSet(key, value, name, ack): CmdStateSet,
    ) -> Result<(Self, Self::Output), Error> {
        let State::Processing {
            ref mut async_results,
            ref mut eager_state,
            ..
        } = self
        else {
            return Err(self.as_unexpected_state("SysStateSet"));
        };

        // This command's index in the journal, which its ack carries.
        let command_index = context.journal.next_command_index_nonzero();

        eager_state.set(key.clone(), value.clone());
        let msg = &SetStateCommandMessage {
            key: Bytes::from(key),
            value: Some(value.into()),
            name: name.unwrap_or_default(),
        };
        let handle = match if ack {
            context
                .output
                .send(&WithFlags(msg, &[MessageFlags::RequestedAck]));
            Some(async_results.create_handle(NotificationId::CommandAck(command_index)))
        } else {
            context.output.send(msg);
            None
        } {
            Some(handle) => handle,
            None => async_results.create_resolved_handle(
                NotificationId::EphemeralCompletionId(
                    context.journal.next_ephemeral_completion_id(),
                ),
                Value::Void,
            ),
        };
        Ok((self, handle))
    }
}

pub(crate) struct CmdStateClear(
    pub(crate) String,
    pub(crate) Option<String>,
    pub(crate) bool,
);

impl TransitionAndReturn<Context, CmdStateClear> for State {
    type Output = NotificationHandle;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        CmdStateClear(key, name, ack): CmdStateClear,
    ) -> Result<(Self, Self::Output), Error> {
        let State::Processing {
            ref mut async_results,
            ref mut eager_state,
            ..
        } = self
        else {
            return Err(self.as_unexpected_state("SysStateClear"));
        };

        // This command's index in the journal, which its ack carries.
        let command_index = context.journal.next_command_index_nonzero();

        eager_state.clear(key.clone());
        let msg = &ClearStateCommandMessage {
            key: Bytes::from(key),
            name: name.unwrap_or_default(),
        };
        let handle = match if ack {
            context
                .output
                .send(&WithFlags(msg, &[MessageFlags::RequestedAck]));
            Some(async_results.create_handle(NotificationId::CommandAck(command_index)))
        } else {
            context.output.send(msg);
            None
        } {
            Some(handle) => handle,
            None => async_results.create_resolved_handle(
                NotificationId::EphemeralCompletionId(
                    context.journal.next_ephemeral_completion_id(),
                ),
                Value::Void,
            ),
        };
        Ok((self, handle))
    }
}

pub(crate) struct CmdStateClearAll(pub(crate) Option<String>, pub(crate) bool);

impl TransitionAndReturn<Context, CmdStateClearAll> for State {
    type Output = NotificationHandle;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        CmdStateClearAll(name, ack): CmdStateClearAll,
    ) -> Result<(Self, Self::Output), Error> {
        let State::Processing {
            ref mut async_results,
            ref mut eager_state,
            ..
        } = self
        else {
            return Err(self.as_unexpected_state("SysStateClearAll"));
        };

        // This command's index in the journal, which its ack carries.
        let command_index = context.journal.next_command_index_nonzero();

        eager_state.clear_all();
        let msg = &ClearAllStateCommandMessage {
            name: name.unwrap_or_default(),
        };
        let handle = match if ack {
            context
                .output
                .send(&WithFlags(msg, &[MessageFlags::RequestedAck]));
            Some(async_results.create_handle(NotificationId::CommandAck(command_index)))
        } else {
            context.output.send(msg);
            None
        } {
            Some(handle) => handle,
            None => async_results.create_resolved_handle(
                NotificationId::EphemeralCompletionId(
                    context.journal.next_ephemeral_completion_id(),
                ),
                Value::Void,
            ),
        };
        Ok((self, handle))
    }
}

// --- Calls, sends and invocations

pub(crate) struct CmdCall(
    pub(crate) Target,
    pub(crate) Bytes,
    pub(crate) Option<String>,
);

impl TransitionAndReturn<Context, CmdCall> for State {
    type Output = CallHandle;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        CmdCall(target, input, name): CmdCall,
    ) -> Result<(Self, Self::Output), Error> {
        let State::Processing {
            ref mut async_results,
            ..
        } = self
        else {
            return Err(self.as_unexpected_state("SysCall"));
        };

        // This command's index in the journal.
        context.journal.next_command_index();

        let invocation_id_notification_idx = context.journal.next_completion_notification_id();
        let invocation_id_notification_handle = async_results
            .create_handle(NotificationId::CompletionId(invocation_id_notification_idx));
        let result_completion_id = context.journal.next_completion_notification_id();
        let call_notification_handle =
            async_results.create_handle(NotificationId::CompletionId(result_completion_id));
        let target_headers = target.headers;
        let msg = &CallCommandMessage {
            service_name: target.service,
            handler_name: target.handler,
            key: target.key.unwrap_or_default(),
            idempotency_key: target.idempotency_key,
            scope: target.scope,
            limit_key: target.limit_key,
            headers: target_headers
                .into_iter()
                .map(crate::service_protocol::messages::Header::from)
                .collect(),
            parameter: input,
            invocation_id_notification_idx: invocation_id_notification_idx.into(),
            result_completion_id: result_completion_id.into(),
            name: name.unwrap_or_default(),
        };
        context.output.send(msg);
        Ok((
            self,
            CallHandle {
                invocation_id_notification_handle,
                call_notification_handle,
            },
        ))
    }
}

pub(crate) struct CmdSend(
    pub(crate) Target,
    pub(crate) Bytes,
    /// Invoke time, in milliseconds since the UNIX epoch.
    pub(crate) u64,
    pub(crate) Option<String>,
);

impl TransitionAndReturn<Context, CmdSend> for State {
    type Output = SendHandle;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        CmdSend(target, input, invoke_time, name): CmdSend,
    ) -> Result<(Self, Self::Output), Error> {
        let State::Processing {
            ref mut async_results,
            ..
        } = self
        else {
            return Err(self.as_unexpected_state("SysSend"));
        };

        // This command's index in the journal.
        context.journal.next_command_index();

        let invocation_id_notification_idx = context.journal.next_completion_notification_id();
        let invocation_id_notification_handle = async_results
            .create_handle(NotificationId::CompletionId(invocation_id_notification_idx));
        let target_headers = target.headers;
        let msg = &OneWayCallCommandMessage {
            service_name: target.service,
            handler_name: target.handler,
            key: target.key.unwrap_or_default(),
            idempotency_key: target.idempotency_key,
            scope: target.scope,
            limit_key: target.limit_key,
            headers: target_headers
                .into_iter()
                .map(crate::service_protocol::messages::Header::from)
                .collect(),
            parameter: input,
            invoke_time,
            invocation_id_notification_idx: invocation_id_notification_idx.into(),
            name: name.unwrap_or_default(),
        };
        context.output.send(msg);
        Ok((
            self,
            SendHandle {
                invocation_id_notification_handle,
            },
        ))
    }
}

pub(crate) struct CmdAttachInvocation(pub(crate) AttachInvocationTarget, pub(crate) Option<String>);

impl TransitionAndReturn<Context, CmdAttachInvocation> for State {
    type Output = NotificationHandle;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        CmdAttachInvocation(target, name): CmdAttachInvocation,
    ) -> Result<(Self, Self::Output), Error> {
        let State::Processing {
            ref mut async_results,
            ..
        } = self
        else {
            return Err(self.as_unexpected_state("SysAttachInvocation"));
        };

        // This command's index in the journal.
        context.journal.next_command_index();

        let result_completion_id = context.journal.next_completion_notification_id();
        let handle =
            async_results.create_handle(NotificationId::CompletionId(result_completion_id));
        let msg = &AttachInvocationCommandMessage {
            target: Some(match target {
                AttachInvocationTarget::InvocationId(id) => {
                    attach_invocation_command_message::Target::InvocationId(id)
                }
                AttachInvocationTarget::WorkflowId { name, key, scope } => {
                    attach_invocation_command_message::Target::WorkflowTarget(WorkflowTarget {
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
                } => attach_invocation_command_message::Target::IdempotentRequestTarget(
                    IdempotentRequestTarget {
                        service_name,
                        service_key,
                        handler_name,
                        idempotency_key,
                        scope,
                    },
                ),
            }),
            result_completion_id: result_completion_id.into(),
            name: name.unwrap_or_default(),
        };
        context.output.send(msg);
        Ok((self, handle))
    }
}

pub(crate) struct CmdCancelInvocation(
    pub(crate) String,
    pub(crate) Option<String>,
    pub(crate) bool,
);

impl TransitionAndReturn<Context, CmdCancelInvocation> for State {
    type Output = NotificationHandle;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        CmdCancelInvocation(target_invocation_id, name, ack): CmdCancelInvocation,
    ) -> Result<(Self, Self::Output), Error> {
        let State::Processing {
            ref mut async_results,
            ..
        } = self
        else {
            return Err(self.as_unexpected_state("SysCancelInvocation"));
        };

        // This command's index in the journal, which its ack carries.
        let command_index = context.journal.next_command_index_nonzero();

        let msg = &SendSignalCommandMessage {
            target_invocation_id,
            entry_name: name.unwrap_or_default(),
            signal_id: Some(send_signal_command_message::SignalId::Idx(CANCEL_SIGNAL_ID)),
            result: Some(send_signal_command_message::Result::Void(Default::default())),
        };
        let handle = match if ack {
            context
                .output
                .send(&WithFlags(msg, &[MessageFlags::RequestedAck]));
            Some(async_results.create_handle(NotificationId::CommandAck(command_index)))
        } else {
            context.output.send(msg);
            None
        } {
            Some(handle) => handle,
            None => async_results.create_resolved_handle(
                NotificationId::EphemeralCompletionId(
                    context.journal.next_ephemeral_completion_id(),
                ),
                Value::Void,
            ),
        };
        Ok((self, handle))
    }
}

// --- Traces

pub(crate) struct TraceStart(pub(crate) String, pub(crate) Option<TraceId>);

impl TransitionAndReturn<Context, TraceStart> for State {
    type Output = TraceId;

    fn transition_and_return(
        mut self,
        context: &mut Context,
        TraceStart(name, parent): TraceStart,
    ) -> Result<(Self, Self::Output), Error> {
        let State::Processing {
            ref mut trace_state,
            ..
        } = self
        else {
            return Err(self.as_unexpected_state("SysStartTrace"));
        };

        // In this version of the protocol, the parent is in the run's name.
        let name = match parent {
            None => name,
            Some(parent) if trace_state.is_started(parent.into()) => {
                format!("{name} (parent trace {})", u32::from(parent))
            }
            Some(parent) => return Err(unknown_trace(parent)),
        };

        // This command's index in the journal.
        context.journal.next_command_index();
        let result_completion_id = context.journal.next_completion_notification_id().into();
        trace_state.start(result_completion_id);
        let msg = &RunCommandMessage {
            name,
            result_completion_id,
        };
        context.output.send(msg);
        Ok((self, TraceId::from(result_completion_id)))
    }
}

pub(crate) struct TraceEnd(pub(crate) TraceId, pub(crate) NonEmptyValue);

impl TransitionAndReturn<Context, TraceEnd> for State {
    type Output = ();

    fn transition_and_return(
        mut self,
        context: &mut Context,
        TraceEnd(trace, trace_result): TraceEnd,
    ) -> Result<(Self, Self::Output), Error> {
        let State::Processing {
            ref mut trace_state,
            ..
        } = self
        else {
            return Err(self.as_unexpected_state("SysEndTrace"));
        };

        if !trace_state.end(trace.into()) {
            return Err(unknown_trace(trace));
        }
        let completion_id = trace.into();
        let result = match trace_result {
            NonEmptyValue::Success(value) => propose_run_completion_message::Result::Value(value),
            NonEmptyValue::Failure(failure) => {
                propose_run_completion_message::Result::Failure(failure.into())
            }
        };
        context.output.send(&WithFlags(
            &ProposeRunCompletionMessage {
                result_completion_id: completion_id,
                result: Some(result),
            },
            &[MessageFlags::RequestedAck],
        ));
        Ok((self, ()))
    }
}
