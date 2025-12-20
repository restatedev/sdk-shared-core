use crate::service_protocol::{MessageHeader, MessageType, NotificationResult};
use bytes::Bytes;
use paste::paste;
use std::borrow::Cow;
use std::fmt;

pub trait RestateMessage: prost::Message + Default {
    fn ty() -> MessageType;

    fn generate_header(&self) -> MessageHeader {
        MessageHeader::new(Self::ty(), self.encoded_len() as u32)
    }
}

pub trait NamedCommandMessage {
    fn name(&self) -> String;
}

pub trait CommandMessageHeaderEq {
    /// Compare command message headers for equality.
    /// - `ignore_payload_equality`: if true, skip payload equality checks
    fn header_eq(&self, other: &Self, ignore_payload_equality: bool) -> bool;
}

pub trait CommandMessageHeaderDiff {
    fn write_diff(&self, expected: &Self, f: crate::fmt::DiffFormatter<'_, '_>) -> fmt::Result;
}

include!("./generated/dev.restate.service.protocol.rs");

macro_rules! impl_message_traits {
    ($name:ident: core) => {
        impl_message_traits!($name: message);
    };
    ($name:ident: notification) => {
        impl_message_traits!($name: message);
    };
    ($name:ident: command) => {
        impl_message_traits!($name: message);
        impl_message_traits!($name: named_command);
    };
    ($name:ident: command eq) => {
        impl_message_traits!($name: message);
        impl_message_traits!($name: named_command);
        impl_message_traits!($name: command_header_eq);
    };
    ($name:ident: message) => {
         impl RestateMessage for paste! { [<$name Message>] } {
            fn ty() -> MessageType {
                MessageType::$name
            }
        }
    };
    ($name:ident: named_command) => {
        impl NamedCommandMessage for paste! { [<$name Message>] } {
            fn name(&self) -> String {
                self.name.clone()
            }
        }
    };
    ($name:ident: command_header_eq) => {
        impl CommandMessageHeaderEq for paste! { [<$name Message>] } {
            fn header_eq(&self, other: &Self, _: bool) -> bool {
                self.eq(other)
            }
        }
    };
}

// --- Core messages
impl_message_traits!(Start: core);
impl_message_traits!(Suspension: core);
impl_message_traits!(Error: core);
impl_message_traits!(End: core);
impl_message_traits!(ProposeRunCompletion: core);

// -- Entries
impl_message_traits!(InputCommand: command);
impl CommandMessageHeaderEq for InputCommandMessage {
    fn header_eq(&self, _: &Self, _: bool) -> bool {
        true
    }
}

impl_message_traits!(OutputCommand: command);
impl CommandMessageHeaderEq for OutputCommandMessage {
    fn header_eq(&self, other: &Self, ignore_payload_equality: bool) -> bool {
        if ignore_payload_equality {
            self.name == other.name
                && match (&self.result, &other.result) {
                    (
                        Some(output_command_message::Result::Value(_)),
                        Some(output_command_message::Result::Value(_)),
                    ) => true,
                    (x, y) => x.eq(y),
                }
        } else {
            self.eq(other)
        }
    }
}

impl_message_traits!(GetLazyStateCommand: command eq);
impl_message_traits!(GetLazyStateCompletionNotification: notification);

impl_message_traits!(SetStateCommand: command);
impl CommandMessageHeaderEq for SetStateCommandMessage {
    fn header_eq(&self, other: &Self, ignore_payload_equality: bool) -> bool {
        if ignore_payload_equality {
            self.name == other.name
                && self.key == other.key
                && match (&self.value, &other.value) {
                    (Some(_), Some(_)) => true,
                    (x, y) => x.eq(y),
                }
        } else {
            self.eq(other)
        }
    }
}

impl_message_traits!(ClearStateCommand: command eq);

impl_message_traits!(ClearAllStateCommand: command eq);

impl_message_traits!(GetLazyStateKeysCommand: command eq);
impl_message_traits!(GetLazyStateKeysCompletionNotification: notification);

impl_message_traits!(GetEagerStateCommand: command);
impl CommandMessageHeaderEq for GetEagerStateCommandMessage {
    fn header_eq(&self, other: &Self, ignore_payload_equality: bool) -> bool {
        if ignore_payload_equality {
            self.name == other.name
                && self.key == other.key
                && match (&self.result, &other.result) {
                    (
                        Some(get_eager_state_command_message::Result::Value(_)),
                        Some(get_eager_state_command_message::Result::Value(_)),
                    ) => true,
                    (x, y) => x.eq(y),
                }
        } else {
            self.eq(other)
        }
    }
}

impl_message_traits!(GetEagerStateKeysCommand: command eq);

impl_message_traits!(GetPromiseCommand: command eq);
impl_message_traits!(GetPromiseCompletionNotification: notification);

impl_message_traits!(PeekPromiseCommand: command eq);
impl_message_traits!(PeekPromiseCompletionNotification: notification);

impl_message_traits!(CompletePromiseCommand: command);
impl CommandMessageHeaderEq for CompletePromiseCommandMessage {
    fn header_eq(&self, other: &Self, ignore_payload_equality: bool) -> bool {
        if ignore_payload_equality {
            self.name == other.name
                && self.key == other.key
                && self.result_completion_id == other.result_completion_id
                && match (&self.completion, &other.completion) {
                    (
                        Some(complete_promise_command_message::Completion::CompletionValue(_)),
                        Some(complete_promise_command_message::Completion::CompletionValue(_)),
                    ) => true,
                    (x, y) => x.eq(y),
                }
        } else {
            self.eq(other)
        }
    }
}
impl_message_traits!(CompletePromiseCompletionNotification: notification);

impl_message_traits!(SleepCommand: command);
impl CommandMessageHeaderEq for SleepCommandMessage {
    fn header_eq(&self, other: &Self, _: bool) -> bool {
        self.name == other.name
    }
}
impl_message_traits!(SleepCompletionNotification: notification);

impl_message_traits!(CallCommand: command);
impl CommandMessageHeaderEq for CallCommandMessage {
    fn header_eq(&self, other: &Self, ignore_payload_equality: bool) -> bool {
        self.service_name == other.service_name
            && self.handler_name == other.handler_name
            && (ignore_payload_equality || (self.parameter == other.parameter))
            && self.headers == other.headers
            && self.key == other.key
            && self.idempotency_key == other.idempotency_key
            && self.invocation_id_notification_idx == other.invocation_id_notification_idx
            && self.result_completion_id == other.result_completion_id
            && self.name == other.name
    }
}
impl_message_traits!(CallInvocationIdCompletionNotification: notification);
impl_message_traits!(CallCompletionNotification: notification);

impl_message_traits!(OneWayCallCommand: command);
impl CommandMessageHeaderEq for OneWayCallCommandMessage {
    fn header_eq(&self, other: &Self, ignore_payload_equality: bool) -> bool {
        self.service_name == other.service_name
            && self.handler_name == other.handler_name
            && (ignore_payload_equality || (self.parameter == other.parameter))
            && self.headers == other.headers
            && self.key == other.key
            && self.idempotency_key == other.idempotency_key
            && self.invocation_id_notification_idx == other.invocation_id_notification_idx
            && self.name == other.name
    }
}

impl_message_traits!(SendSignalCommand: message);
impl NamedCommandMessage for SendSignalCommandMessage {
    fn name(&self) -> String {
        self.entry_name.clone()
    }
}
impl_message_traits!(SendSignalCommand: command_header_eq);

impl_message_traits!(RunCommand: command eq);
impl_message_traits!(RunCompletionNotification: notification);

impl_message_traits!(AttachInvocationCommand: command eq);
impl_message_traits!(AttachInvocationCompletionNotification: notification);

impl_message_traits!(GetInvocationOutputCommand: command eq);
impl_message_traits!(GetInvocationOutputCompletionNotification: notification);

impl_message_traits!(CompleteAwakeableCommand: command);
impl CommandMessageHeaderEq for CompleteAwakeableCommandMessage {
    fn header_eq(&self, other: &Self, ignore_payload_equality: bool) -> bool {
        if ignore_payload_equality {
            self.name == other.name
                && self.awakeable_id == other.awakeable_id
                && match (&self.result, &other.result) {
                    (
                        Some(complete_awakeable_command_message::Result::Value(_)),
                        Some(complete_awakeable_command_message::Result::Value(_)),
                    ) => true,
                    (x, y) => x.eq(y),
                }
        } else {
            self.eq(other)
        }
    }
}

impl_message_traits!(SignalNotification: notification);

// --- Diffs and Formatting

impl CommandMessageHeaderDiff for InputCommandMessage {
    fn write_diff(&self, _: &Self, _: crate::fmt::DiffFormatter<'_, '_>) -> fmt::Result {
        Ok(())
    }
}

impl CommandMessageHeaderDiff for OutputCommandMessage {
    fn write_diff(&self, expected: &Self, mut f: crate::fmt::DiffFormatter<'_, '_>) -> fmt::Result {
        struct DisplayResult<'a>(&'a Option<output_command_message::Result>);

        impl<'a> fmt::Display for DisplayResult<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                match self.0 {
                    None => write!(f, "<empty>"),
                    Some(output_command_message::Result::Value(value)) => write!(f, "{value}"),
                    Some(output_command_message::Result::Failure(failure)) => {
                        write!(f, "{failure}")
                    }
                }
            }
        }

        if self.result != expected.result {
            f.write_diff(
                "result",
                DisplayResult(&self.result),
                DisplayResult(&expected.result),
            )?;
        }

        Ok(())
    }
}

impl CommandMessageHeaderDiff for GetLazyStateCommandMessage {
    fn write_diff(&self, expected: &Self, mut f: crate::fmt::DiffFormatter<'_, '_>) -> fmt::Result {
        if self.key != expected.key {
            f.write_bytes_diff("key", &self.key, &expected.key)?;
        }

        if self.result_completion_id != expected.result_completion_id {
            f.write_diff(
                "result_completion_id",
                self.result_completion_id,
                expected.result_completion_id,
            )?;
        }

        Ok(())
    }
}

impl CommandMessageHeaderDiff for SetStateCommandMessage {
    fn write_diff(&self, expected: &Self, mut f: crate::fmt::DiffFormatter<'_, '_>) -> fmt::Result {
        if self.key != expected.key {
            f.write_bytes_diff("key", &self.key, &expected.key)?;
        }

        if self.value != expected.value {
            struct DisplayValue<'a>(&'a Option<Value>);

            impl<'a> fmt::Display for DisplayValue<'a> {
                fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    match self.0 {
                        None => write!(f, "<empty>"),
                        Some(value) => write!(f, "{value}"),
                    }
                }
            }

            f.write_diff(
                "value",
                DisplayValue(&self.value),
                DisplayValue(&expected.value),
            )?;
        }

        Ok(())
    }
}

impl CommandMessageHeaderDiff for ClearStateCommandMessage {
    fn write_diff(&self, expected: &Self, mut f: crate::fmt::DiffFormatter<'_, '_>) -> fmt::Result {
        if self.key != expected.key {
            f.write_bytes_diff("key", &self.key, &expected.key)?;
        }

        Ok(())
    }
}

impl CommandMessageHeaderDiff for ClearAllStateCommandMessage {
    fn write_diff(&self, _: &Self, _: crate::fmt::DiffFormatter<'_, '_>) -> fmt::Result {
        // No fields to compare other than name, which is common to all command messages
        Ok(())
    }
}

impl CommandMessageHeaderDiff for GetLazyStateKeysCommandMessage {
    fn write_diff(&self, expected: &Self, mut f: crate::fmt::DiffFormatter<'_, '_>) -> fmt::Result {
        if self.result_completion_id != expected.result_completion_id {
            f.write_diff(
                "result_completion_id",
                self.result_completion_id,
                expected.result_completion_id,
            )?;
        }

        Ok(())
    }
}

impl CommandMessageHeaderDiff for GetEagerStateCommandMessage {
    fn write_diff(&self, expected: &Self, mut f: crate::fmt::DiffFormatter<'_, '_>) -> fmt::Result {
        if self.key != expected.key {
            f.write_bytes_diff("key", &self.key, &expected.key)?;
        }

        if self.result != expected.result {
            struct DisplayResult<'a>(&'a Option<get_eager_state_command_message::Result>);

            impl<'a> fmt::Display for DisplayResult<'a> {
                fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    match self.0 {
                        None => write!(f, "<empty>"),
                        Some(get_eager_state_command_message::Result::Void(_)) => write!(f, "void"),
                        Some(get_eager_state_command_message::Result::Value(value)) => {
                            write!(f, "{value}")
                        }
                    }
                }
            }

            f.write_diff(
                "result",
                DisplayResult(&self.result),
                DisplayResult(&expected.result),
            )?;
        }

        Ok(())
    }
}

impl CommandMessageHeaderDiff for GetEagerStateKeysCommandMessage {
    fn write_diff(&self, expected: &Self, mut f: crate::fmt::DiffFormatter<'_, '_>) -> fmt::Result {
        if self.value != expected.value {
            struct DisplayStateKeys<'a>(&'a Option<StateKeys>);

            impl<'a> fmt::Display for DisplayStateKeys<'a> {
                fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    match self.0 {
                        None => write!(f, "<empty>"),
                        Some(state_keys) => {
                            write!(f, "[")?;
                            for (i, key) in state_keys.keys.iter().enumerate() {
                                if i > 0 {
                                    write!(f, ", ")?;
                                }
                                write!(f, "'{}'", String::from_utf8_lossy(key))?;
                            }
                            write!(f, "]")
                        }
                    }
                }
            }

            f.write_diff(
                "value",
                DisplayStateKeys(&self.value),
                DisplayStateKeys(&expected.value),
            )?;
        }

        Ok(())
    }
}

impl CommandMessageHeaderDiff for GetPromiseCommandMessage {
    fn write_diff(&self, expected: &Self, mut f: crate::fmt::DiffFormatter<'_, '_>) -> fmt::Result {
        if self.key != expected.key {
            f.write_diff("key", &self.key, &expected.key)?;
        }

        if self.result_completion_id != expected.result_completion_id {
            f.write_diff(
                "result_completion_id",
                self.result_completion_id,
                expected.result_completion_id,
            )?;
        }

        Ok(())
    }
}

impl CommandMessageHeaderDiff for PeekPromiseCommandMessage {
    fn write_diff(&self, expected: &Self, mut f: crate::fmt::DiffFormatter<'_, '_>) -> fmt::Result {
        if self.key != expected.key {
            f.write_diff("key", &self.key, &expected.key)?;
        }

        if self.result_completion_id != expected.result_completion_id {
            f.write_diff(
                "result_completion_id",
                self.result_completion_id,
                expected.result_completion_id,
            )?;
        }

        Ok(())
    }
}

impl CommandMessageHeaderDiff for CompletePromiseCommandMessage {
    fn write_diff(&self, expected: &Self, mut f: crate::fmt::DiffFormatter<'_, '_>) -> fmt::Result {
        if self.key != expected.key {
            f.write_diff("key", &self.key, &expected.key)?;
        }

        if self.result_completion_id != expected.result_completion_id {
            f.write_diff(
                "result_completion_id",
                self.result_completion_id,
                expected.result_completion_id,
            )?;
        }

        if self.completion != expected.completion {
            struct DisplayCompletion<'a>(&'a Option<complete_promise_command_message::Completion>);

            impl<'a> fmt::Display for DisplayCompletion<'a> {
                fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    match self.0 {
                        None => write!(f, "<empty>"),
                        Some(complete_promise_command_message::Completion::CompletionValue(
                            value,
                        )) => {
                            write!(f, "{value}")
                        }
                        Some(complete_promise_command_message::Completion::CompletionFailure(
                            failure,
                        )) => {
                            write!(f, "{failure}")
                        }
                    }
                }
            }

            f.write_diff(
                "completion",
                DisplayCompletion(&self.completion),
                DisplayCompletion(&expected.completion),
            )?;
        }

        Ok(())
    }
}

impl CommandMessageHeaderDiff for CallCommandMessage {
    fn write_diff(&self, expected: &Self, mut f: crate::fmt::DiffFormatter<'_, '_>) -> fmt::Result {
        if self.service_name != expected.service_name {
            f.write_diff("service_name", &self.service_name, &expected.service_name)?;
        }

        if self.handler_name != expected.handler_name {
            f.write_diff("handler_name", &self.handler_name, &expected.handler_name)?;
        }

        if self.parameter != expected.parameter {
            f.write_bytes_diff("parameter", &self.parameter, &expected.parameter)?;
        }

        if self.key != expected.key {
            f.write_diff("key", &self.key, &expected.key)?;
        }

        if self.idempotency_key != expected.idempotency_key {
            f.write_diff(
                "idempotency_key",
                DisplayOptionalString(&self.idempotency_key),
                DisplayOptionalString(&expected.idempotency_key),
            )?;
        }

        if self.invocation_id_notification_idx != expected.invocation_id_notification_idx {
            f.write_diff(
                "invocation_id_notification_idx",
                self.invocation_id_notification_idx,
                expected.invocation_id_notification_idx,
            )?;
        }

        if self.result_completion_id != expected.result_completion_id {
            f.write_diff(
                "result_completion_id",
                self.result_completion_id,
                expected.result_completion_id,
            )?;
        }

        Ok(())
    }
}

impl CommandMessageHeaderDiff for SleepCommandMessage {
    fn write_diff(&self, expected: &Self, mut f: crate::fmt::DiffFormatter<'_, '_>) -> fmt::Result {
        if self.wake_up_time != expected.wake_up_time {
            f.write_diff("wake_up_time", self.wake_up_time, expected.wake_up_time)?;
        }

        if self.result_completion_id != expected.result_completion_id {
            f.write_diff(
                "result_completion_id",
                self.result_completion_id,
                expected.result_completion_id,
            )?;
        }

        Ok(())
    }
}

impl CommandMessageHeaderDiff for OneWayCallCommandMessage {
    fn write_diff(&self, expected: &Self, mut f: crate::fmt::DiffFormatter<'_, '_>) -> fmt::Result {
        if self.service_name != expected.service_name {
            f.write_diff("service_name", &self.service_name, &expected.service_name)?;
        }

        if self.handler_name != expected.handler_name {
            f.write_diff("handler_name", &self.handler_name, &expected.handler_name)?;
        }

        if self.parameter != expected.parameter {
            f.write_bytes_diff("parameter", &self.parameter, &expected.parameter)?;
        }

        if self.invoke_time != expected.invoke_time {
            f.write_diff("invoke_time", self.invoke_time, expected.invoke_time)?;
        }

        if self.key != expected.key {
            f.write_diff("key", &self.key, &expected.key)?;
        }

        if self.idempotency_key != expected.idempotency_key {
            f.write_diff(
                "idempotency_key",
                DisplayOptionalString(&self.idempotency_key),
                DisplayOptionalString(&expected.idempotency_key),
            )?;
        }

        if self.invocation_id_notification_idx != expected.invocation_id_notification_idx {
            f.write_diff(
                "invocation_id_notification_idx",
                self.invocation_id_notification_idx,
                expected.invocation_id_notification_idx,
            )?;
        }

        Ok(())
    }
}

impl CommandMessageHeaderDiff for SendSignalCommandMessage {
    fn write_diff(&self, expected: &Self, mut f: crate::fmt::DiffFormatter<'_, '_>) -> fmt::Result {
        if self.target_invocation_id != expected.target_invocation_id {
            f.write_diff(
                "target_invocation_id",
                &self.target_invocation_id,
                &expected.target_invocation_id,
            )?;
        }

        if self.signal_id != expected.signal_id {
            struct DisplaySignalId<'a>(&'a Option<send_signal_command_message::SignalId>);

            impl<'a> fmt::Display for DisplaySignalId<'a> {
                fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    match self.0 {
                        None => write!(f, "<empty>"),
                        Some(send_signal_command_message::SignalId::Idx(idx)) => {
                            write!(f, "{idx}")
                        }
                        Some(send_signal_command_message::SignalId::Name(name)) => {
                            write!(f, "{name}")
                        }
                    }
                }
            }

            f.write_diff(
                "signal_id",
                DisplaySignalId(&self.signal_id),
                DisplaySignalId(&expected.signal_id),
            )?;
        }

        if self.result != expected.result {
            struct DisplayResult<'a>(&'a Option<send_signal_command_message::Result>);

            impl<'a> fmt::Display for DisplayResult<'a> {
                fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    match self.0 {
                        None => write!(f, "<empty>"),
                        Some(send_signal_command_message::Result::Void(_)) => write!(f, "void"),
                        Some(send_signal_command_message::Result::Value(value)) => {
                            write!(f, "{value}")
                        }
                        Some(send_signal_command_message::Result::Failure(failure)) => {
                            write!(f, "{failure}")
                        }
                    }
                }
            }

            f.write_diff(
                "result",
                DisplayResult(&self.result),
                DisplayResult(&expected.result),
            )?;
        }

        Ok(())
    }
}

impl CommandMessageHeaderDiff for RunCommandMessage {
    fn write_diff(&self, expected: &Self, mut f: crate::fmt::DiffFormatter<'_, '_>) -> fmt::Result {
        if self.result_completion_id != expected.result_completion_id {
            f.write_diff(
                "result_completion_id",
                self.result_completion_id,
                expected.result_completion_id,
            )?;
        }

        Ok(())
    }
}

impl CommandMessageHeaderDiff for AttachInvocationCommandMessage {
    fn write_diff(&self, expected: &Self, mut f: crate::fmt::DiffFormatter<'_, '_>) -> fmt::Result {
        if self.result_completion_id != expected.result_completion_id {
            f.write_diff(
                "result_completion_id",
                self.result_completion_id,
                expected.result_completion_id,
            )?;
        }

        if self.target != expected.target {
            struct DisplayTarget<'a>(&'a Option<attach_invocation_command_message::Target>);

            impl<'a> fmt::Display for DisplayTarget<'a> {
                fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    match self.0 {
                        None => write!(f, "<empty>"),
                        Some(attach_invocation_command_message::Target::InvocationId(id)) => {
                            write!(f, "{id}")
                        }
                        Some(
                            attach_invocation_command_message::Target::IdempotentRequestTarget(_),
                        ) => {
                            write!(f, "IdempotentRequestTarget")
                        }
                        Some(attach_invocation_command_message::Target::WorkflowTarget(_)) => {
                            write!(f, "WorkflowTarget")
                        }
                    }
                }
            }

            f.write_diff(
                "target",
                DisplayTarget(&self.target),
                DisplayTarget(&expected.target),
            )?;
        }

        Ok(())
    }
}

impl CommandMessageHeaderDiff for GetInvocationOutputCommandMessage {
    fn write_diff(&self, expected: &Self, mut f: crate::fmt::DiffFormatter<'_, '_>) -> fmt::Result {
        if self.result_completion_id != expected.result_completion_id {
            f.write_diff(
                "result_completion_id",
                self.result_completion_id,
                expected.result_completion_id,
            )?;
        }

        if self.target != expected.target {
            struct DisplayTarget<'a>(&'a Option<get_invocation_output_command_message::Target>);

            impl<'a> fmt::Display for DisplayTarget<'a> {
                fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    match self.0 {
                        None => write!(f, "<empty>"),
                        Some(get_invocation_output_command_message::Target::InvocationId(id)) => {
                            write!(f, "{id}")
                        }
                        Some(
                            get_invocation_output_command_message::Target::IdempotentRequestTarget(
                                _,
                            ),
                        ) => {
                            write!(f, "IdempotentRequestTarget")
                        }
                        Some(get_invocation_output_command_message::Target::WorkflowTarget(_)) => {
                            write!(f, "WorkflowTarget")
                        }
                    }
                }
            }

            f.write_diff(
                "target",
                DisplayTarget(&self.target),
                DisplayTarget(&expected.target),
            )?;
        }

        Ok(())
    }
}

impl CommandMessageHeaderDiff for CompleteAwakeableCommandMessage {
    fn write_diff(&self, expected: &Self, mut f: crate::fmt::DiffFormatter<'_, '_>) -> fmt::Result {
        if self.awakeable_id != expected.awakeable_id {
            f.write_diff("awakeable_id", &self.awakeable_id, &expected.awakeable_id)?;
        }

        if self.result != expected.result {
            struct DisplayResult<'a>(&'a Option<complete_awakeable_command_message::Result>);

            impl<'a> fmt::Display for DisplayResult<'a> {
                fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    match self.0 {
                        None => write!(f, "<empty>"),
                        Some(complete_awakeable_command_message::Result::Value(value)) => {
                            write!(f, "{value}")
                        }
                        Some(complete_awakeable_command_message::Result::Failure(failure)) => {
                            write!(f, "{failure}")
                        }
                    }
                }
            }

            f.write_diff(
                "result",
                DisplayResult(&self.result),
                DisplayResult(&expected.result),
            )?;
        }

        Ok(())
    }
}

struct DisplayOptionalString<'a>(&'a Option<String>);

impl<'a> fmt::Display for DisplayOptionalString<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.0 {
            None => write!(f, "<empty>"),
            Some(s) => write!(f, "{s}"),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Ok(content) = std::str::from_utf8(&self.content) {
            write!(f, "'{content}'")
        } else {
            write!(f, "{:?}", &self.content)
        }
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "error [{}] '{}'", self.code, self.message)
    }
}

// --- Other conversions

impl From<crate::TerminalFailure> for Failure {
    fn from(value: crate::TerminalFailure) -> Self {
        Self {
            code: value.code as u32,
            message: value.message,
            metadata: value
                .metadata
                .into_iter()
                .map(|(key, value)| FailureMetadata { key, value })
                .collect(),
        }
    }
}

impl From<Failure> for crate::TerminalFailure {
    fn from(value: Failure) -> Self {
        Self {
            code: value.code as u16,
            message: value.message,
            metadata: value
                .metadata
                .into_iter()
                .map(|fm| (fm.key, fm.value))
                .collect(),
        }
    }
}

impl From<Header> for crate::Header {
    fn from(header: Header) -> Self {
        Self {
            key: Cow::Owned(header.key),
            value: Cow::Owned(header.value),
        }
    }
}

impl From<crate::Header> for Header {
    fn from(header: crate::Header) -> Self {
        Self {
            key: header.key.into(),
            value: header.value.into(),
        }
    }
}

impl From<Bytes> for Value {
    fn from(content: Bytes) -> Self {
        Self { content }
    }
}

impl From<NotificationResult> for crate::Value {
    fn from(value: NotificationResult) -> Self {
        match value {
            NotificationResult::Void(_) => crate::Value::Void,
            NotificationResult::Value(v) => crate::Value::Success(v.content),
            NotificationResult::Failure(f) => crate::Value::Failure(f.into()),
            NotificationResult::InvocationId(id) => crate::Value::InvocationId(id),
            NotificationResult::StateKeys(sk) => crate::Value::StateKeys(
                sk.keys
                    .iter()
                    .map(|b| String::from_utf8_lossy(b).to_string())
                    .collect(),
            ),
        }
    }
}
