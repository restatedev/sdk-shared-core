// This file is @generated by prost-build.
/// Type: 0xFC00 + 2
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CombinatorEntryMessage {
    #[prost(uint32, repeated, tag = "1")]
    pub completed_entries_order: ::prost::alloc::vec::Vec<u32>,
    /// Entry name
    #[prost(string, tag = "12")]
    pub name: ::prost::alloc::string::String,
}