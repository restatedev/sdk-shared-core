// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Wire-encoding helpers and the [`Message`] trait used by codegen.
//!
//! The host-aware sub-traits [`RestateBuf`] / [`RestateBufMut`] and their
//! impls live in [`crate::buffer`]; they are re-exported from this module
//! so the `restate_wire_derive::Message` proc-macro can resolve every
//! emitted path under `::restate_sdk_shared_core::proto::*`. The macro
//! NEVER spells `::prost::*` paths directly.

use ::bytes::Buf;

// Re-export the host-aware buffer machinery so derive-macro output can
// refer to a single namespace (`::restate_sdk_shared_core::proto::*`) for
// everything it needs.
pub use crate::buffer::{Buffer, HostBufferHandle, HostBufferRegistry, RestateBuf, RestateBufMut};

// =====================================================================
// Re-exports from prost
// =====================================================================
//
// The Message trait reuses prost's low-level wire primitives — they're
// generic over `BufMut`/`Buf` and therefore accept our sub-trait buffers
// directly. Re-exporting them here lets generated derive-macro code
// reference `::restate_sdk_shared_core::proto::*` for everything: the
// macro NEVER emits `::prost::*` paths directly.

pub use prost::encoding::{
    check_wire_type, decode_key, decode_varint, encode_key, encode_varint, encoded_len_varint,
    key_len, merge_loop, skip_field, DecodeContext, WireType,
};
pub use prost::DecodeError;
pub use prost::UnknownEnumValue;

// Per-scalar-type encoding modules. The derive-macro emits paths like
// `proto::string::encode`, `proto::uint32::encoded_len_repeated`, etc.
// These names mirror prost's module layout 1:1.
pub use prost::encoding::{
    bool, bytes, double, fixed32, fixed64, float, int32, int64, sfixed32, sfixed64, sint32, sint64,
    string, uint32, uint64,
};

// `prost::encoding::message` is the per-type encoding module for nested
// messages bound to `prost::Message`. The derive emits our own
// `message_*` helpers (bound to our `Message` trait) defined below in this
// module, so we deliberately do NOT re-export `prost::encoding::message`.

// `prost::alloc::{string, vec}` and `prost::bytes::Bytes` are referenced by
// the generated code emitted by `prost-build` (the .proto-derived structs),
// not by our derive macro. Re-export them here so the generated structs
// can rebind to `proto::alloc::*` if/when we choose to retarget; today the
// generated file still spells them as `::prost::*`, which is fine — that's
// the prost-build output, not the macro output.

/// Allocator-aware re-exports so derive-macro codegen never spells
/// `::alloc::*` or `::prost::alloc::*` directly. The macro emits
/// `::restate_sdk_shared_core::proto::alloc::vec::Vec::new()` for repeated
/// scalar field defaults and `::restate_sdk_shared_core::proto::alloc::string::String::new()`
/// for empty-string scalar defaults.
pub mod alloc {
    pub use prost::alloc::{string, vec};
}

// =====================================================================
// Buffer field helpers
// =====================================================================
//
// Codegen for `#[prost(bytes = "buffer", tag = "N")]` fields calls these
// helpers. The `Buffer::InMemory` vs `Buffer::Host` dispatch is delegated
// to `RestateBufMut::push_buffer`, so each buffer implementor owns its
// own policy and the derive-macro emit site stays mechanically simple.

/// Encode a `Buffer`-typed bytes field at protobuf wire format.
///
/// Emits `tag + length + bytes_or_host_reference`. The length is read
/// directly off [`Buffer::len`] — no registry round-trip required. The
/// body is handed to [`RestateBufMut::push_buffer`], which decides per
/// impl whether to copy the bytes inline or emit a host-buffer reference.
/// Byte-identical to prost's `bytes` field encoding for `InMemory`
/// (verified via round-trip tests).
pub fn encode_buffer<B: RestateBufMut>(tag: u32, value: &Buffer, buf: &mut B) {
    encode_key(tag, WireType::LengthDelimited, buf);
    encode_varint(value.len() as u64, buf);
    buf.push_buffer(value.clone());
}

/// Encoded length of a `Buffer`-typed bytes field at a given tag.
///
/// Mirrors `prost::encoding::bytes::encoded_len`. For `Host` buffers,
/// the length is taken directly from [`HostBufferHandle::len`] — no
/// registry round-trip required.
pub fn encoded_len_buffer(tag: u32, value: &Buffer) -> usize {
    let len = match value {
        Buffer::InMemory(b) => b.len(),
        Buffer::Host(h) => h.len() as usize,
    };
    key_len(tag) + encoded_len_varint(len as u64) + len
}

/// Decode a `Buffer`-typed bytes field from a `RestateBuf`.
///
/// Tries the host-slice fast path first (zero-copy when the source can
/// provide it). Falls back to copy_to_bytes (which is itself zero-copy
/// for `Bytes` sources and a fresh allocation for everything else).
// `prost::DecodeError::new` is the only public constructor; prost
// deprecated it but provides no replacement. Allow the warning at the
// function level so the rest of the file stays lint-clean.
#[allow(deprecated)]
pub fn merge_buffer<B: RestateBuf>(
    wire_type: WireType,
    value: &mut Buffer,
    buf: &mut B,
    _ctx: DecodeContext,
) -> Result<(), DecodeError> {
    if wire_type != WireType::LengthDelimited {
        return Err(DecodeError::new(format!(
            "invalid wire type for Buffer field: {:?}",
            wire_type
        )));
    }
    let len = decode_varint(buf)? as usize;
    if buf.remaining() < len {
        return Err(DecodeError::new("buffer underflow"));
    }
    *value = match buf.try_take_host_slice(len) {
        Some(handle) => Buffer::Host(handle),
        None => Buffer::InMemory(buf.copy_to_bytes(len)),
    };
    Ok(())
}

// =====================================================================
// Nested-message field helpers
// =====================================================================
//
// `prost::encoding::message::*` is bound to `prost::Message`. Our derive
// emits these wrappers instead so nested messages dispatch on the
// `restate_wire::Message` trait.

/// Encode a nested message field at protobuf wire format.
pub fn message_encode<M: Message, B: RestateBufMut>(tag: u32, msg: &M, buf: &mut B) {
    encode_key(tag, WireType::LengthDelimited, buf);
    encode_varint(msg.encoded_len() as u64, buf);
    msg.encode_raw(buf);
}

/// Merge a nested message field from a wire source.
pub fn message_merge<M: Message, B: RestateBuf>(
    wire_type: WireType,
    msg: &mut M,
    buf: &mut B,
    ctx: DecodeContext,
) -> Result<(), DecodeError> {
    check_wire_type(WireType::LengthDelimited, wire_type)?;
    merge_loop(msg, buf, ctx, |msg, buf, ctx| {
        let (tag, wire_type) = decode_key(buf)?;
        msg.merge_field(tag, wire_type, buf, ctx)
    })
}

/// Encoded length of a nested message field at the given tag.
pub fn message_encoded_len<M: Message>(tag: u32, msg: &M) -> usize {
    let len = msg.encoded_len();
    key_len(tag) + encoded_len_varint(len as u64) + len
}

/// Encode a repeated nested message field.
pub fn message_encode_repeated<M: Message, B: RestateBufMut>(
    tag: u32,
    messages: &[M],
    buf: &mut B,
) {
    for msg in messages {
        message_encode(tag, msg, buf);
    }
}

/// Merge an item into a repeated nested message field.
pub fn message_merge_repeated<M: Message + Default, B: RestateBuf>(
    wire_type: WireType,
    messages: &mut Vec<M>,
    buf: &mut B,
    ctx: DecodeContext,
) -> Result<(), DecodeError> {
    check_wire_type(WireType::LengthDelimited, wire_type)?;
    let mut msg = M::default();
    message_merge(WireType::LengthDelimited, &mut msg, buf, ctx)?;
    messages.push(msg);
    Ok(())
}

/// Encoded length of a repeated nested message field.
pub fn message_encoded_len_repeated<M: Message>(tag: u32, messages: &[M]) -> usize {
    key_len(tag) * messages.len()
        + messages
            .iter()
            .map(|m| {
                let len = m.encoded_len();
                len + encoded_len_varint(len as u64)
            })
            .sum::<usize>()
}

// =====================================================================
// Message trait
// =====================================================================

/// Our custom prost-shaped message trait. Same shape as `prost::Message`
/// but with `RestateBufMut` / `RestateBuf` bounds so we control the
/// dispatch on host-backed payload fields.
///
/// Generated by `restate_wire_derive::Message` from prost-style struct
/// definitions. The trait is intentionally narrow: it doesn't try to be a
/// general-purpose protobuf API — it covers exactly what shared-core
/// needs to encode/decode its protocol messages.
pub trait Message: Send + Sync {
    /// Encode the message body (no length prefix, no header) into `buf`.
    fn encode_raw<B: RestateBufMut>(&self, buf: &mut B);

    /// Length of `encode_raw`'s output, in bytes. For `Buffer::Host`
    /// fields, the length is read straight off [`HostBufferHandle::len`].
    fn encoded_len(&self) -> usize;

    /// Merge one wire field into `self`. Called by the generated
    /// `Default + decode` loop; not typically called directly.
    fn merge_field<B: RestateBuf>(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut B,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError>;

    /// Reset all fields to their default values.
    fn clear(&mut self);

    /// Decode a message body from `buf`.
    ///
    /// Default impl drives the prost-shaped per-field merge loop. Most
    /// callers prefer this; bespoke implementors can override if they
    /// need to wrap the loop (rare).
    fn decode<B: RestateBuf>(mut buf: B) -> Result<Self, DecodeError>
    where
        Self: Default,
    {
        let mut msg = Self::default();
        let ctx = DecodeContext::default();
        while buf.has_remaining() {
            let (tag, wire_type) = decode_key(&mut buf)?;
            msg.merge_field(tag, wire_type, &mut buf, ctx.clone())?;
        }
        Ok(msg)
    }
}
