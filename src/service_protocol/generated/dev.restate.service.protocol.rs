// This file is @generated by prost-build.
/// Type: 0x0000 + 0
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StartMessage {
    /// Unique id of the invocation. This id is unique across invocations and won't change when replaying the journal.
    #[prost(bytes = "bytes", tag = "1")]
    pub id: ::prost::bytes::Bytes,
    /// Invocation id that can be used for logging.
    /// The user can use this id to address this invocation in admin and status introspection apis.
    #[prost(string, tag = "2")]
    pub debug_id: ::prost::alloc::string::String,
    #[prost(uint32, tag = "3")]
    pub known_entries: u32,
    /// protolint:disable:next REPEATED_FIELD_NAMES_PLURALIZED
    #[prost(message, repeated, tag = "4")]
    pub state_map: ::prost::alloc::vec::Vec<start_message::StateEntry>,
    #[prost(bool, tag = "5")]
    pub partial_state: bool,
    /// If this invocation has a key associated (e.g. for objects and workflows), then this key is filled in. Empty otherwise.
    #[prost(string, tag = "6")]
    pub key: ::prost::alloc::string::String,
    /// Retry count since the last stored entry.
    ///
    /// Please note that this count might not be accurate, as it's not durably stored,
    /// thus it might get reset in case Restate crashes/changes leader.
    #[prost(uint32, tag = "7")]
    pub retry_count_since_last_stored_entry: u32,
    /// Duration since the last stored entry, in milliseconds.
    ///
    /// Please note this duration might not be accurate,
    /// and might change depending on which Restate replica executes the request.
    #[prost(uint64, tag = "8")]
    pub duration_since_last_stored_entry: u64,
}
/// Nested message and enum types in `StartMessage`.
pub mod start_message {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct StateEntry {
        #[prost(bytes = "bytes", tag = "1")]
        pub key: ::prost::bytes::Bytes,
        /// If value is an empty byte array,
        /// then it means the value is empty and not "missing" (e.g. empty string).
        #[prost(bytes = "bytes", tag = "2")]
        pub value: ::prost::bytes::Bytes,
    }
}
/// Type: 0x0000 + 1
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CompletionMessage {
    #[prost(uint32, tag = "1")]
    pub entry_index: u32,
    #[prost(oneof = "completion_message::Result", tags = "13, 14, 15")]
    pub result: ::core::option::Option<completion_message::Result>,
}
/// Nested message and enum types in `CompletionMessage`.
pub mod completion_message {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        #[prost(message, tag = "13")]
        Empty(super::Empty),
        #[prost(bytes, tag = "14")]
        Value(::prost::bytes::Bytes),
        #[prost(message, tag = "15")]
        Failure(super::Failure),
    }
}
/// Type: 0x0000 + 2
/// Implementations MUST send this message when suspending an invocation.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SuspensionMessage {
    /// This list represents any of the entry_index the invocation is waiting on to progress.
    /// The runtime will resume the invocation as soon as one of the given entry_index is completed.
    /// This list MUST not be empty.
    /// False positive, entry_indexes is a valid plural of entry_indices.
    /// <https://learn.microsoft.com/en-us/style-guide/a-z-word-list-term-collections/i/index-indexes-indices>
    ///
    /// protolint:disable:this REPEATED_FIELD_NAMES_PLURALIZED
    #[prost(uint32, repeated, tag = "1")]
    pub entry_indexes: ::prost::alloc::vec::Vec<u32>,
}
/// Type: 0x0000 + 3
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ErrorMessage {
    /// The code can be any HTTP status code, as described <https://www.iana.org/assignments/http-status-codes/http-status-codes.xhtml.>
    /// In addition, we define the following error codes that MAY be used by the SDK for better error reporting:
    /// * JOURNAL_MISMATCH = 570, that is when the SDK cannot replay a journal due to the mismatch between the journal and the actual code.
    /// * PROTOCOL_VIOLATION = 571, that is when the SDK receives an unexpected message or an expected message variant, given its state.
    #[prost(uint32, tag = "1")]
    pub code: u32,
    /// Contains a concise error message, e.g. Throwable#getMessage() in Java.
    #[prost(string, tag = "2")]
    pub message: ::prost::alloc::string::String,
    /// Contains a verbose error description, e.g. the exception stacktrace.
    #[prost(string, tag = "3")]
    pub description: ::prost::alloc::string::String,
    /// Entry that caused the failure. This may be outside the current stored journal size.
    /// If no specific entry caused the failure, the current replayed/processed entry can be used.
    #[prost(uint32, optional, tag = "4")]
    pub related_entry_index: ::core::option::Option<u32>,
    /// Name of the entry that caused the failure.
    #[prost(string, optional, tag = "5")]
    pub related_entry_name: ::core::option::Option<::prost::alloc::string::String>,
    /// Entry type.
    #[prost(uint32, optional, tag = "6")]
    pub related_entry_type: ::core::option::Option<u32>,
    /// Delay before executing the next retry, specified as duration in milliseconds.
    /// If provided, it will override the default retry policy used by Restate's invoker ONLY for the next retry attempt.
    #[prost(uint64, optional, tag = "8")]
    pub next_retry_delay: ::core::option::Option<u64>,
}
/// Type: 0x0000 + 4
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct EntryAckMessage {
    #[prost(uint32, tag = "1")]
    pub entry_index: u32,
}
/// Type: 0x0000 + 5
/// Implementations MUST send this message when the invocation lifecycle ends.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct EndMessage {}
/// Completable: No
/// Fallible: No
/// Type: 0x0400 + 0
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InputEntryMessage {
    #[prost(message, repeated, tag = "1")]
    pub headers: ::prost::alloc::vec::Vec<Header>,
    #[prost(bytes = "bytes", tag = "14")]
    pub value: ::prost::bytes::Bytes,
    /// Entry name
    #[prost(string, tag = "12")]
    pub name: ::prost::alloc::string::String,
}
/// Completable: No
/// Fallible: No
/// Type: 0x0400 + 1
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OutputEntryMessage {
    /// Entry name
    #[prost(string, tag = "12")]
    pub name: ::prost::alloc::string::String,
    #[prost(oneof = "output_entry_message::Result", tags = "14, 15")]
    pub result: ::core::option::Option<output_entry_message::Result>,
}
/// Nested message and enum types in `OutputEntryMessage`.
pub mod output_entry_message {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        #[prost(bytes, tag = "14")]
        Value(::prost::bytes::Bytes),
        #[prost(message, tag = "15")]
        Failure(super::Failure),
    }
}
/// Completable: Yes
/// Fallible: No
/// Type: 0x0800 + 0
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetStateEntryMessage {
    #[prost(bytes = "bytes", tag = "1")]
    pub key: ::prost::bytes::Bytes,
    /// Entry name
    #[prost(string, tag = "12")]
    pub name: ::prost::alloc::string::String,
    #[prost(oneof = "get_state_entry_message::Result", tags = "13, 14, 15")]
    pub result: ::core::option::Option<get_state_entry_message::Result>,
}
/// Nested message and enum types in `GetStateEntryMessage`.
pub mod get_state_entry_message {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        #[prost(message, tag = "13")]
        Empty(super::Empty),
        #[prost(bytes, tag = "14")]
        Value(::prost::bytes::Bytes),
        #[prost(message, tag = "15")]
        Failure(super::Failure),
    }
}
/// Completable: No
/// Fallible: No
/// Type: 0x0800 + 1
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetStateEntryMessage {
    #[prost(bytes = "bytes", tag = "1")]
    pub key: ::prost::bytes::Bytes,
    #[prost(bytes = "bytes", tag = "3")]
    pub value: ::prost::bytes::Bytes,
    /// Entry name
    #[prost(string, tag = "12")]
    pub name: ::prost::alloc::string::String,
}
/// Completable: No
/// Fallible: No
/// Type: 0x0800 + 2
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ClearStateEntryMessage {
    #[prost(bytes = "bytes", tag = "1")]
    pub key: ::prost::bytes::Bytes,
    /// Entry name
    #[prost(string, tag = "12")]
    pub name: ::prost::alloc::string::String,
}
/// Completable: No
/// Fallible: No
/// Type: 0x0800 + 3
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ClearAllStateEntryMessage {
    /// Entry name
    #[prost(string, tag = "12")]
    pub name: ::prost::alloc::string::String,
}
/// Completable: Yes
/// Fallible: No
/// Type: 0x0800 + 4
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetStateKeysEntryMessage {
    /// Entry name
    #[prost(string, tag = "12")]
    pub name: ::prost::alloc::string::String,
    #[prost(oneof = "get_state_keys_entry_message::Result", tags = "14, 15")]
    pub result: ::core::option::Option<get_state_keys_entry_message::Result>,
}
/// Nested message and enum types in `GetStateKeysEntryMessage`.
pub mod get_state_keys_entry_message {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct StateKeys {
        #[prost(bytes = "bytes", repeated, tag = "1")]
        pub keys: ::prost::alloc::vec::Vec<::prost::bytes::Bytes>,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        #[prost(message, tag = "14")]
        Value(StateKeys),
        #[prost(message, tag = "15")]
        Failure(super::Failure),
    }
}
/// Completable: Yes
/// Fallible: No
/// Type: 0x0800 + 8
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetPromiseEntryMessage {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
    /// Entry name
    #[prost(string, tag = "12")]
    pub name: ::prost::alloc::string::String,
    #[prost(oneof = "get_promise_entry_message::Result", tags = "14, 15")]
    pub result: ::core::option::Option<get_promise_entry_message::Result>,
}
/// Nested message and enum types in `GetPromiseEntryMessage`.
pub mod get_promise_entry_message {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        #[prost(bytes, tag = "14")]
        Value(::prost::bytes::Bytes),
        #[prost(message, tag = "15")]
        Failure(super::Failure),
    }
}
/// Completable: Yes
/// Fallible: No
/// Type: 0x0800 + 9
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PeekPromiseEntryMessage {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
    /// Entry name
    #[prost(string, tag = "12")]
    pub name: ::prost::alloc::string::String,
    #[prost(oneof = "peek_promise_entry_message::Result", tags = "13, 14, 15")]
    pub result: ::core::option::Option<peek_promise_entry_message::Result>,
}
/// Nested message and enum types in `PeekPromiseEntryMessage`.
pub mod peek_promise_entry_message {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        #[prost(message, tag = "13")]
        Empty(super::Empty),
        #[prost(bytes, tag = "14")]
        Value(::prost::bytes::Bytes),
        #[prost(message, tag = "15")]
        Failure(super::Failure),
    }
}
/// Completable: Yes
/// Fallible: No
/// Type: 0x0800 + A
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CompletePromiseEntryMessage {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
    /// Entry name
    #[prost(string, tag = "12")]
    pub name: ::prost::alloc::string::String,
    /// The value to use to complete the promise
    #[prost(oneof = "complete_promise_entry_message::Completion", tags = "2, 3")]
    pub completion: ::core::option::Option<complete_promise_entry_message::Completion>,
    #[prost(oneof = "complete_promise_entry_message::Result", tags = "13, 15")]
    pub result: ::core::option::Option<complete_promise_entry_message::Result>,
}
/// Nested message and enum types in `CompletePromiseEntryMessage`.
pub mod complete_promise_entry_message {
    /// The value to use to complete the promise
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Completion {
        #[prost(bytes, tag = "2")]
        CompletionValue(::prost::bytes::Bytes),
        #[prost(message, tag = "3")]
        CompletionFailure(super::Failure),
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        /// Returns empty if value was set successfully
        #[prost(message, tag = "13")]
        Empty(super::Empty),
        /// Returns a failure if the promise was already completed
        #[prost(message, tag = "15")]
        Failure(super::Failure),
    }
}
/// Completable: Yes
/// Fallible: No
/// Type: 0x0C00 + 0
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SleepEntryMessage {
    /// Wake up time.
    /// The time is set as duration since UNIX Epoch.
    #[prost(uint64, tag = "1")]
    pub wake_up_time: u64,
    /// Entry name
    #[prost(string, tag = "12")]
    pub name: ::prost::alloc::string::String,
    #[prost(oneof = "sleep_entry_message::Result", tags = "13, 15")]
    pub result: ::core::option::Option<sleep_entry_message::Result>,
}
/// Nested message and enum types in `SleepEntryMessage`.
pub mod sleep_entry_message {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        #[prost(message, tag = "13")]
        Empty(super::Empty),
        #[prost(message, tag = "15")]
        Failure(super::Failure),
    }
}
/// Completable: Yes
/// Fallible: Yes
/// Type: 0x0C00 + 1
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CallEntryMessage {
    #[prost(string, tag = "1")]
    pub service_name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub handler_name: ::prost::alloc::string::String,
    #[prost(bytes = "bytes", tag = "3")]
    pub parameter: ::prost::bytes::Bytes,
    #[prost(message, repeated, tag = "4")]
    pub headers: ::prost::alloc::vec::Vec<Header>,
    /// If this invocation has a key associated (e.g. for objects and workflows), then this key is filled in. Empty otherwise.
    #[prost(string, tag = "5")]
    pub key: ::prost::alloc::string::String,
    /// Entry name
    #[prost(string, tag = "12")]
    pub name: ::prost::alloc::string::String,
    #[prost(oneof = "call_entry_message::Result", tags = "14, 15")]
    pub result: ::core::option::Option<call_entry_message::Result>,
}
/// Nested message and enum types in `CallEntryMessage`.
pub mod call_entry_message {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        #[prost(bytes, tag = "14")]
        Value(::prost::bytes::Bytes),
        #[prost(message, tag = "15")]
        Failure(super::Failure),
    }
}
/// Completable: No
/// Fallible: Yes
/// Type: 0x0C00 + 2
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OneWayCallEntryMessage {
    #[prost(string, tag = "1")]
    pub service_name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub handler_name: ::prost::alloc::string::String,
    #[prost(bytes = "bytes", tag = "3")]
    pub parameter: ::prost::bytes::Bytes,
    /// Time when this BackgroundInvoke should be executed.
    /// The time is set as duration since UNIX Epoch.
    /// If this value is not set, equal to 0, or past in time,
    /// the runtime will execute this BackgroundInvoke as soon as possible.
    #[prost(uint64, tag = "4")]
    pub invoke_time: u64,
    #[prost(message, repeated, tag = "5")]
    pub headers: ::prost::alloc::vec::Vec<Header>,
    /// If this invocation has a key associated (e.g. for objects and workflows), then this key is filled in. Empty otherwise.
    #[prost(string, tag = "6")]
    pub key: ::prost::alloc::string::String,
    /// Entry name
    #[prost(string, tag = "12")]
    pub name: ::prost::alloc::string::String,
}
/// Completable: Yes
/// Fallible: No
/// Type: 0x0C00 + 3
/// Awakeables are addressed by an identifier exposed to the user. See the spec for more details.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AwakeableEntryMessage {
    /// Entry name
    #[prost(string, tag = "12")]
    pub name: ::prost::alloc::string::String,
    #[prost(oneof = "awakeable_entry_message::Result", tags = "14, 15")]
    pub result: ::core::option::Option<awakeable_entry_message::Result>,
}
/// Nested message and enum types in `AwakeableEntryMessage`.
pub mod awakeable_entry_message {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        #[prost(bytes, tag = "14")]
        Value(::prost::bytes::Bytes),
        #[prost(message, tag = "15")]
        Failure(super::Failure),
    }
}
/// Completable: No
/// Fallible: Yes
/// Type: 0x0C00 + 4
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CompleteAwakeableEntryMessage {
    /// Identifier of the awakeable. See the spec for more details.
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    /// Entry name
    #[prost(string, tag = "12")]
    pub name: ::prost::alloc::string::String,
    #[prost(oneof = "complete_awakeable_entry_message::Result", tags = "14, 15")]
    pub result: ::core::option::Option<complete_awakeable_entry_message::Result>,
}
/// Nested message and enum types in `CompleteAwakeableEntryMessage`.
pub mod complete_awakeable_entry_message {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        #[prost(bytes, tag = "14")]
        Value(::prost::bytes::Bytes),
        #[prost(message, tag = "15")]
        Failure(super::Failure),
    }
}
/// Completable: No
/// Fallible: No
/// Type: 0x0C00 + 5
/// Flag: RequiresRuntimeAck
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RunEntryMessage {
    /// Entry name
    #[prost(string, tag = "12")]
    pub name: ::prost::alloc::string::String,
    #[prost(oneof = "run_entry_message::Result", tags = "14, 15")]
    pub result: ::core::option::Option<run_entry_message::Result>,
}
/// Nested message and enum types in `RunEntryMessage`.
pub mod run_entry_message {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        #[prost(bytes, tag = "14")]
        Value(::prost::bytes::Bytes),
        #[prost(message, tag = "15")]
        Failure(super::Failure),
    }
}
/// This failure object carries user visible errors,
/// e.g. invocation failure return value or failure result of an InvokeEntryMessage.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Failure {
    /// The code can be any HTTP status code, as described <https://www.iana.org/assignments/http-status-codes/http-status-codes.xhtml.>
    #[prost(uint32, tag = "1")]
    pub code: u32,
    /// Contains a concise error message, e.g. Throwable#getMessage() in Java.
    #[prost(string, tag = "2")]
    pub message: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Header {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub value: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct Empty {}
/// Service protocol version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ServiceProtocolVersion {
    Unspecified = 0,
    /// initial service protocol version
    V1 = 1,
    /// Added
    /// * Entry retry mechanism: ErrorMessage.next_retry_delay, StartMessage.retry_count_since_last_stored_entry and StartMessage.duration_since_last_stored_entry
    V2 = 2,
}
impl ServiceProtocolVersion {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ServiceProtocolVersion::Unspecified => "SERVICE_PROTOCOL_VERSION_UNSPECIFIED",
            ServiceProtocolVersion::V1 => "V1",
            ServiceProtocolVersion::V2 => "V2",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "SERVICE_PROTOCOL_VERSION_UNSPECIFIED" => Some(Self::Unspecified),
            "V1" => Some(Self::V1),
            "V2" => Some(Self::V2),
            _ => None,
        }
    }
}
