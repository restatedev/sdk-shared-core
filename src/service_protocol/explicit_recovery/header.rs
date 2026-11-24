use crate::service_protocol::header::UnknownMessageType;

const ACTION_RANGE_START: u16 = 0x0400;
const NOTIFICATION_RANGE_START: u16 = 0x8000;

// Generates the family's MessageType, with its conversions from and to the 16-bit code. Unlike the
// durable execution protocol's, there is no custom entry: a type this version does not define is a
// protocol violation.
macro_rules! gen_message_type_enum {
    ($($variant:ident = $id:literal,)*) => {
        #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
        pub enum MessageType {
            $($variant,)*
        }

        impl TryFrom<u16> for MessageType {
            type Error = UnknownMessageType;

            fn try_from(value: u16) -> Result<Self, UnknownMessageType> {
                match value {
                    $($id => Ok(MessageType::$variant),)*
                    v => Err(UnknownMessageType(v)),
                }
            }
        }

        impl From<MessageType> for u16 {
            fn from(mt: MessageType) -> Self {
                match mt {
                    $(MessageType::$variant => $id,)*
                }
            }
        }
    };
}

gen_message_type_enum!(
    Start = 0x0000,
    Shutdown = 0x0001,
    Error = 0x0002,
    End = 0x0003,
    Heartbeat = 0x0004,
    GetStateAction = 0x0410,
    GetStateKeysAction = 0x0411,
    SetStateAction = 0x0412,
    ClearStateAction = 0x0413,
    ClearAllStateAction = 0x0414,
    CallAction = 0x0420,
    AttachInvocationAction = 0x0421,
    CancelInvocationAction = 0x0422,
    ReadSignalAction = 0x0430,
    SendSignalAction = 0x0431,
    FlushAction = 0x0440,
    AtomicGroupAction = 0x0441,
    SpanStartAction = 0x0450,
    SpanEndAction = 0x0451,
    ResultNotification = 0x8000,
    SignalNotification = 0x8001,
);

impl MessageType {
    fn id(&self) -> u16 {
        u16::from(*self)
    }

    /// `0x0000..0x0400`: control messages.
    pub fn is_control(&self) -> bool {
        self.id() < ACTION_RANGE_START
    }

    /// `0x0400..0x8000`: actions, SDK to runtime.
    pub fn is_action(&self) -> bool {
        (ACTION_RANGE_START..NOTIFICATION_RANGE_START).contains(&self.id())
    }

    /// `0x8000..`: notifications, runtime to SDK.
    pub fn is_notification(&self) -> bool {
        self.id() >= NOTIFICATION_RANGE_START
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service_protocol::MessageHeader;

    #[test]
    fn ranges() {
        for ty in [
            MessageType::Start,
            MessageType::Shutdown,
            MessageType::Heartbeat,
        ] {
            assert!(ty.is_control() && !ty.is_action() && !ty.is_notification());
        }
        for ty in [MessageType::GetStateAction, MessageType::SpanEndAction] {
            assert!(ty.is_action() && !ty.is_control() && !ty.is_notification());
        }
        for ty in [
            MessageType::ResultNotification,
            MessageType::SignalNotification,
        ] {
            assert!(ty.is_notification() && !ty.is_control() && !ty.is_action());
        }
    }

    #[test]
    fn header_roundtrip_on_the_shared_framing() {
        let serialized: u64 = MessageHeader::new_plain(MessageType::AtomicGroupAction, 42).into();
        assert_eq!(serialized >> 48, 0x0441);
        let header: MessageHeader<MessageType> = serialized.try_into().unwrap();
        assert_eq!(header.ty(), MessageType::AtomicGroupAction);
        assert_eq!(header.length(), 42);
        assert_eq!(header.flags(), 0);
    }

    #[test]
    fn unknown_types_are_refused() {
        // 0x0400 is the durable execution protocol's input command; 0xFC01 one of its custom entries.
        for code in [0x0400u16, 0x0005, 0x8002, 0xFC01] {
            assert!(MessageType::try_from(code).is_err());
            let serialized = (code as u64) << 48;
            assert!(MessageHeader::<MessageType>::try_from(serialized).is_err());
        }
    }
}
