// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

const CUSTOM_ENTRY_MASK: u16 = 0xFC00;
const COMPLETED_MASK: u64 = 0x0001_0000_0000;
const REQUIRES_ACK_MASK: u64 = 0x8000_0000_0000;

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
    (@gen_enum [$variant:ident Entry = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message_type_enum!(@gen_enum [$($tail)*] -> [[<$variant Entry>], $($body)*]); }
    };
    (@gen_enum [$variant:ident = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message_type_enum!(@gen_enum [$($tail)*] -> [$variant, $($body)*]);
    };

    (@gen_is_entry_impl [] -> [$($variant:ident, $is_entry:literal,)*]) => {
        impl MessageType {
            pub fn is_entry(&self) -> bool {
                match self {
                    $(MessageType::$variant => $is_entry,)*
                    MessageType::CustomEntry(_) => true
                }
            }
        }
    };
    (@gen_is_entry_impl [$variant:ident Entry = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message_type_enum!(@gen_is_entry_impl [$($tail)*] -> [[<$variant Entry>], true, $($body)*]); }
    };
    (@gen_is_entry_impl [$variant:ident = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message_type_enum!(@gen_is_entry_impl [$($tail)*] -> [$variant, false, $($body)*]);
    };

    (@gen_to_id [] -> [$($variant:ident, $id:literal,)*]) => {
        impl From<MessageType> for MessageTypeId {
            fn from(mt: MessageType) -> Self {
                match mt {
                    $(MessageType::$variant => $id,)*
                    MessageType::CustomEntry(id) => id
                }
            }
        }
    };
    (@gen_to_id [$variant:ident Entry = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message_type_enum!(@gen_to_id [$($tail)*] -> [[<$variant Entry>], $id, $($body)*]); }
    };
    (@gen_to_id [$variant:ident = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message_type_enum!(@gen_to_id [$($tail)*] -> [$variant, $id, $($body)*]);
    };

    (@gen_from_id [] -> [$($variant:ident, $id:literal,)*]) => {
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
    };
    (@gen_from_id [$variant:ident Entry = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message_type_enum!(@gen_from_id [$($tail)*] -> [[<$variant Entry>], $id, $($body)*]); }
    };
    (@gen_from_id [$variant:ident = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message_type_enum!(@gen_from_id [$($tail)*] -> [$variant, $id, $($body)*]);
    };

    // Entrypoint of the macro
    ($($tokens:tt)*) => {
        gen_message_type_enum!(@gen_enum [$($tokens)*] -> []);
        gen_message_type_enum!(@gen_is_entry_impl [$($tokens)*] -> []);
        gen_message_type_enum!(@gen_to_id [$($tokens)*] -> []);
        gen_message_type_enum!(@gen_from_id [$($tokens)*] -> []);
    };
}

gen_message_type_enum!(
    Start = 0x0000,
    Completion = 0x0001,
    Suspension = 0x0002,
    Error = 0x0003,
    End = 0x0005,
    EntryAck = 0x0004,
    Input Entry = 0x0400,
    Output Entry = 0x0401,
    GetState Entry = 0x0800,
    SetState Entry = 0x0801,
    ClearState Entry = 0x0802,
    GetStateKeys Entry = 0x0804,
    ClearAllState Entry = 0x0803,
    GetPromise Entry = 0x0808,
    PeekPromise Entry = 0x0809,
    CompletePromise Entry = 0x080A,
    Sleep Entry = 0x0C00,
    Call Entry = 0x0C01,
    OneWayCall Entry = 0x0C02,
    Awakeable Entry = 0x0C03,
    CompleteAwakeable Entry = 0x0C04,
    Run Entry = 0x0C05,
    CancelInvocation Entry = 0x0C06,
    GetCallInvocationId Entry = 0x0C07,
    Combinator Entry = 0xFC02,
);

impl MessageType {
    fn has_completed_flag(&self) -> bool {
        matches!(
            self,
            MessageType::GetStateEntry
                | MessageType::GetStateKeysEntry
                | MessageType::SleepEntry
                | MessageType::CallEntry
                | MessageType::AwakeableEntry
                | MessageType::GetPromiseEntry
                | MessageType::PeekPromiseEntry
                | MessageType::CompletePromiseEntry
                | MessageType::GetCallInvocationIdEntry
        )
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct MessageHeader {
    ty: MessageType,
    length: u32,

    // --- Flags
    /// Only `CompletableEntries` have completed flag. See [`MessageType#allows_completed_flag`].
    completed_flag: Option<bool>,
    /// All Entry messages may have requires ack flag.
    requires_ack_flag: Option<bool>,
}

impl MessageHeader {
    #[inline]
    pub fn new(ty: MessageType, length: u32) -> Self {
        Self::_new(ty, None, None, length)
    }

    #[inline]
    pub fn new_entry_header(ty: MessageType, completed_flag: Option<bool>, length: u32) -> Self {
        debug_assert!(completed_flag.is_some() == ty.has_completed_flag());

        MessageHeader {
            ty,
            length,
            completed_flag,
            requires_ack_flag: None,
        }
    }

    #[inline]
    pub fn new_ackable_entry_header(
        ty: MessageType,
        completed_flag: Option<bool>,
        requires_ack_flag: Option<bool>,
        length: u32,
    ) -> Self {
        debug_assert!(completed_flag.is_some() == ty.has_completed_flag());

        MessageHeader {
            ty,
            length,
            completed_flag,
            requires_ack_flag,
        }
    }

    #[inline]
    fn _new(
        ty: MessageType,
        completed_flag: Option<bool>,
        requires_ack_flag: Option<bool>,
        length: u32,
    ) -> Self {
        MessageHeader {
            ty,
            length,
            completed_flag,
            requires_ack_flag,
        }
    }

    #[inline]
    pub fn message_type(&self) -> MessageType {
        self.ty
    }

    #[inline]
    pub fn completed(&self) -> Option<bool> {
        self.completed_flag
    }

    #[inline]
    pub fn requires_ack(&self) -> Option<bool> {
        self.requires_ack_flag
    }

    #[inline]
    pub fn frame_length(&self) -> u32 {
        self.length
    }
}

macro_rules! read_flag_if {
    ($cond:expr, $value:expr, $mask:expr) => {
        if $cond {
            Some(($value & $mask) != 0)
        } else {
            None
        }
    };
}

impl TryFrom<u64> for MessageHeader {
    type Error = UnknownMessageType;

    /// Deserialize the protocol header.
    /// See https://github.com/restatedev/service-protocol/blob/main/service-invocation-protocol.md#message-header
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        let ty_code = (value >> 48) as u16;
        let ty: MessageType = ty_code.try_into()?;

        let completed_flag = read_flag_if!(ty.has_completed_flag(), value, COMPLETED_MASK);
        let requires_ack_flag = read_flag_if!(ty.is_entry(), value, REQUIRES_ACK_MASK);
        let length = value as u32;

        Ok(MessageHeader::_new(
            ty,
            completed_flag,
            requires_ack_flag,
            length,
        ))
    }
}

macro_rules! write_flag {
    ($flag:expr, $value:expr, $mask:expr) => {
        if let Some(true) = $flag {
            *$value |= $mask;
        }
    };
}

impl From<MessageHeader> for u64 {
    /// Serialize the protocol header.
    /// See https://github.com/restatedev/service-protocol/blob/main/service-invocation-protocol.md#message-header
    fn from(message_header: MessageHeader) -> Self {
        let mut res =
            ((u16::from(message_header.ty) as u64) << 48) | (message_header.length as u64);

        write_flag!(message_header.completed_flag, &mut res, COMPLETED_MASK);
        write_flag!(
            message_header.requires_ack_flag,
            &mut res,
            REQUIRES_ACK_MASK
        );

        res
    }
}

#[cfg(test)]
mod tests {

    use super::{MessageType::*, *};

    impl MessageHeader {
        fn new_completable_entry(ty: MessageType, completed: bool, length: u32) -> Self {
            Self::new_entry_header(ty, Some(completed), length)
        }
    }

    macro_rules! roundtrip_test {
        ($test_name:ident, $header:expr, $ty:expr, $len:expr) => {
            roundtrip_test!($test_name, $header, $ty, $len, None, None, None);
        };
        ($test_name:ident, $header:expr, $ty:expr, $len:expr, version: $protocol_version:expr) => {
            roundtrip_test!(
                $test_name,
                $header,
                $ty,
                $len,
                None,
                Some($protocol_version),
                None
            );
        };
        ($test_name:ident, $header:expr, $ty:expr, $len:expr, completed: $completed:expr) => {
            roundtrip_test!($test_name, $header, $ty, $len, Some($completed), None, None);
        };
        ($test_name:ident, $header:expr, $ty:expr, $len:expr, requires_ack: $requires_ack:expr) => {
            roundtrip_test!(
                $test_name,
                $header,
                $ty,
                $len,
                None,
                None,
                Some($requires_ack)
            );
        };
        ($test_name:ident, $header:expr, $ty:expr, $len:expr, requires_ack: $requires_ack:expr, completed: $completed:expr) => {
            roundtrip_test!(
                $test_name,
                $header,
                $ty,
                $len,
                Some($completed),
                None,
                Some($requires_ack)
            );
        };
        ($test_name:ident, $header:expr, $ty:expr, $len:expr, $completed:expr, $protocol_version:expr, $requires_ack:expr) => {
            #[test]
            fn $test_name() {
                let serialized: u64 = $header.into();
                let header: MessageHeader = serialized.try_into().unwrap();

                assert_eq!(header.message_type(), $ty);
                assert_eq!(header.completed(), $completed);
                assert_eq!(header.requires_ack(), $requires_ack);
                assert_eq!(header.frame_length(), $len);
            }
        };
    }

    roundtrip_test!(
        completion,
        MessageHeader::new(Completion, 22),
        Completion,
        22
    );

    roundtrip_test!(
        completed_get_state,
        MessageHeader::new_completable_entry(GetStateEntry, true, 0),
        GetStateEntry,
        0,
        requires_ack: false,
        completed: true
    );

    roundtrip_test!(
        not_completed_get_state,
        MessageHeader::new_completable_entry(GetStateEntry, false, 0),
        GetStateEntry,
        0,
        requires_ack: false,
        completed: false
    );

    roundtrip_test!(
        completed_get_state_with_len,
        MessageHeader::new_completable_entry(GetStateEntry, true, 10341),
        GetStateEntry,
        10341,
        requires_ack: false,
        completed: true
    );

    roundtrip_test!(
        set_state_with_requires_ack,
        MessageHeader::_new(SetStateEntry, None, Some(true), 10341),
        SetStateEntry,
        10341,
        requires_ack: true
    );

    roundtrip_test!(
        custom_entry,
        MessageHeader::new(MessageType::CustomEntry(0xFC00), 10341),
        MessageType::CustomEntry(0xFC00),
        10341,
        requires_ack: false
    );

    roundtrip_test!(
        custom_entry_with_requires_ack,
        MessageHeader::_new(MessageType::CustomEntry(0xFC00), None, Some(true), 10341),
        MessageType::CustomEntry(0xFC00),
        10341,
        requires_ack: true
    );
}
