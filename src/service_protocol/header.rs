// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::CommandType;
use std::fmt;

const COMMAND_ENTRY_MASK: u16 = 0x0400;
const NOTIFICATION_ENTRY_MASK: u16 = 0x8000;
const CUSTOM_ENTRY_MASK: u16 = 0xFC00;

type MessageTypeId = u16;

#[derive(Debug, thiserror::Error)]
#[error("unknown protocol.message code {0:#x}")]
pub struct UnknownMessageType(u16);

// This macro generates the MessageKind enum, together with the conversions back and forth to MessageTypeId
macro_rules! gen_message_type_enum {
    (@gen_enum [] -> [$($body:tt)*]) => {
        #[derive(Copy, Clone, Debug, PartialEq, Eq)]
        pub enum MessageType {
            $($body)*
            CustomEntry(u16)
        }
    };
    (@gen_enum [$variant:ident = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message_type_enum!(@gen_enum [$($tail)*] -> [$variant, $($body)*]);
    };

    (@gen_id [] -> [$($variant:ident, $id:literal,)*]) => {
        impl TryFrom<MessageTypeId> for MessageType {
            type Error = UnknownMessageType;

            fn try_from(value: MessageTypeId) -> Result<Self, UnknownMessageType> {
                match value {
                    $($id => Ok(MessageType::$variant),)*
                    v if (v & CUSTOM_ENTRY_MASK) != 0 => Ok(MessageType::CustomEntry(v)),
                    v => Err(UnknownMessageType(v)),
                }
            }
        }

        impl From<MessageType> for MessageTypeId {
            fn from(mt: MessageType) -> Self {
                match mt {
                    $(MessageType::$variant => $id,)*
                    MessageType::CustomEntry(id) => id
                }
            }
        }
    };
    (@gen_id [$variant:ident = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message_type_enum!(@gen_id [$($tail)*] -> [$variant, $id, $($body)*]);
    };

    // Entrypoint of the macro
    ($($tokens:tt)*) => {
        gen_message_type_enum!(@gen_enum [$($tokens)*] -> []);
        gen_message_type_enum!(@gen_id [$($tokens)*] -> []);
    };
}

gen_message_type_enum!(
    Start = 0x0000,
    Suspension = 0x0001,
    Error = 0x0002,
    End = 0x0003,
    ProposeRunCompletion = 0x0005,
    InputCommand = 0x0400,
    OutputCommand = 0x0401,
    GetLazyStateCommand = 0x0402,
    GetLazyStateCompletionNotification = 0x8002,
    SetStateCommand = 0x0403,
    ClearStateCommand = 0x0404,
    ClearAllStateCommand = 0x0405,
    GetLazyStateKeysCommand = 0x0406,
    GetLazyStateKeysCompletionNotification = 0x8006,
    GetEagerStateCommand = 0x0407,
    GetEagerStateKeysCommand = 0x0408,
    GetPromiseCommand = 0x0409,
    GetPromiseCompletionNotification = 0x8009,
    PeekPromiseCommand = 0x040A,
    PeekPromiseCompletionNotification = 0x800A,
    CompletePromiseCommand = 0x040B,
    CompletePromiseCompletionNotification = 0x800B,
    SleepCommand = 0x040C,
    SleepCompletionNotification = 0x800C,
    CallCommand = 0x040D,
    CallInvocationIdCompletionNotification = 0x800E,
    CallCompletionNotification = 0x800D,
    OneWayCallCommand = 0x040E,
    SendSignalCommand = 0x0410,
    RunCommand = 0x0411,
    RunCompletionNotification = 0x8011,
    AttachInvocationCommand = 0x0412,
    AttachInvocationCompletionNotification = 0x8012,
    GetInvocationOutputCommand = 0x0413,
    GetInvocationOutputCompletionNotification = 0x8013,
    CompleteAwakeableCommand = 0x0414,
    SignalNotification = 0xFBFF,
);

impl MessageType {
    fn id(&self) -> MessageTypeId {
        MessageTypeId::from(*self)
    }

    pub fn is_command(&self) -> bool {
        (COMMAND_ENTRY_MASK..NOTIFICATION_ENTRY_MASK).contains(&self.id())
    }

    pub fn is_notification(&self) -> bool {
        (NOTIFICATION_ENTRY_MASK..CUSTOM_ENTRY_MASK).contains(&self.id())
    }
}

impl fmt::Display for MessageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match CommandType::try_from(*self) {
            Ok(ct) => write!(f, "{}", crate::fmt::display_command_ty(ct)),
            Err(mt) => write!(f, "{mt:?}"),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct MessageHeader {
    ty: MessageType,
    length: u32,
}

impl MessageHeader {
    #[inline]
    pub fn new(ty: MessageType, length: u32) -> Self {
        Self { ty, length }
    }

    #[inline]
    pub fn message_type(&self) -> MessageType {
        self.ty
    }

    #[inline]
    pub fn message_length(&self) -> u32 {
        self.length
    }
}

impl TryFrom<u64> for MessageHeader {
    type Error = UnknownMessageType;

    /// Deserialize the protocol header.
    /// See https://github.com/restatedev/service-protocol/blob/main/service-invocation-protocol.md#message-header
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        let ty_code = (value >> 48) as u16;
        let ty: MessageType = ty_code.try_into()?;
        let length = value as u32;

        Ok(MessageHeader::new(ty, length))
    }
}

impl From<MessageHeader> for u64 {
    /// Serialize the protocol header.
    /// See https://github.com/restatedev/service-protocol/blob/main/service-invocation-protocol.md#message-header
    fn from(message_header: MessageHeader) -> Self {
        ((u16::from(message_header.ty) as u64) << 48) | (message_header.length as u64)
    }
}

#[cfg(test)]
mod tests {

    use super::{MessageType::*, *};

    macro_rules! roundtrip_test {
        ($test_name:ident, $header:expr, $ty:expr, $len:expr) => {
            #[test]
            fn $test_name() {
                let serialized: u64 = $header.into();
                let header: MessageHeader = serialized.try_into().unwrap();

                assert_eq!(header.message_type(), $ty);
                assert_eq!(header.message_length(), $len);
            }
        };
    }

    roundtrip_test!(
        get_state_empty,
        MessageHeader::new(GetLazyStateCommand, 0),
        GetLazyStateCommand,
        0
    );

    roundtrip_test!(
        get_state_with_length,
        MessageHeader::new(GetLazyStateCommand, 22),
        GetLazyStateCommand,
        22
    );

    roundtrip_test!(
        custom_entry,
        MessageHeader::new(CustomEntry(0xFC01), 10341),
        CustomEntry(0xFC01),
        10341
    );
}
