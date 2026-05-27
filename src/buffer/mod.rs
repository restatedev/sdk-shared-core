// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Buffer abstractions shared by the public API and the wire-encoding
//! sub-traits.
//!
//! Two related concerns live here:
//!
//! 1. The [`Buffer`] enum (plus the [`HostBufferRegistry`] machinery it relies
//!    on) is the canonical "user payload byte sequence" type carried at every
//!    payload-bearing slot of the public `VM` trait, and the output type the
//!    embedder drains from `take_output_next`.
//!
//! 2. The [`RestateBuf`] / [`RestateBufMut`] sub-traits and their host-aware
//!    impls ([`BufferReader`], [`BufferWriter`]) plug into the prost-shaped
//!    encode/decode machinery in `crate::proto` so that `Buffer::Host` values
//!    survive a round trip through the wire layer without being copied into
//!    shared-core's address space. The protocol `Decoder` owns one
//!    [`BufferReader`] (`Buf + RestateBuf`); each message body is decoded
//!    off a length-bounded `Take<&mut BufferReader>`. The protocol `Output`
//!    owns one [`BufferWriter`] (`BufMut + RestateBufMut`); the encoder
//!    writes 8-byte headers and message bodies into it directly.
//!
//! The `restate_wire_derive::Message` proc-macro emits paths into the
//! `crate::proto` module (which re-exports the relevant items from here),
//! keeping the macro output a single namespace.
//!
//! # Refcounting
//!
//! [`HostBufferHandle`] is **refcounted** by the SDK-side
//! [`HostBufferRegistry`]. The handle is non-`Copy`: `Clone` calls
//! [`HostBufferRegistry::retain`], `Drop` calls
//! [`HostBufferRegistry::release`]. The SDK frees the underlying buffer
//! when the refcount drops to zero. Shared-core never needs to think
//! about lifetimes — it just clones handles into the places that need
//! them and lets `Drop` reclaim.

#[cfg(test)]
use ::bytes::BytesMut;
use ::bytes::{Buf, BufMut, Bytes};
#[cfg(test)]
use std::sync::Arc;

mod host;
mod reader;
mod writer;

// Re-export so the public API surface stays under `crate::buffer::*` —
// the submodules are pure code organisation.
#[cfg(test)]
pub use host::InMemoryHostBufferRegistry;
pub use host::{HostBufferHandle, HostBufferRegistry, NopHostBufferRegistry};
pub use reader::BufferReader;
pub use writer::BufferWriter;

// =====================================================================
// Buffer
// =====================================================================

/// A user payload byte sequence.
///
/// Carried at every payload-bearing slot of the public `VM` trait, and the
/// output type the embedder drains from `take_output_next`. `InMemory` is
/// owned by shared-core; `Host` references a buffer owned by the embedder
/// via [`HostBufferHandle`].
#[derive(Clone, Debug)]
pub enum Buffer {
    InMemory(Bytes),
    Host(HostBufferHandle),
}

impl Default for Buffer {
    /// An empty `InMemory` buffer. Used by the derive-macro codegen as the
    /// reset / default value for `bytes = "buffer"` fields.
    fn default() -> Self {
        Buffer::InMemory(Bytes::new())
    }
}

impl Buffer {
    /// Length in bytes.
    pub fn len(&self) -> usize {
        match self {
            Buffer::InMemory(b) => b.len(),
            Buffer::Host(h) => h.len() as usize,
        }
    }

    /// True if zero-length.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl PartialEq for Buffer {
    /// Byte-equality. Required for replay: a wire-decoded expected
    /// payload may come back as `Buffer::Host(sub_view)` (zero-copy
    /// decode fast path), while the actual payload could be a freshly
    /// registered host buffer or an `InMemory` value.
    ///
    /// - `InMemory` vs `InMemory`: direct `Bytes` comparison.
    /// - `Host` vs `Host`: view-identity short-circuit, then
    ///   [`HostBufferRegistry::eq`] for byte comparison.
    /// - Cross-variant: materialise the host bytes via `read_into` and
    ///   compare with the inline bytes. The slow path — only fires when
    ///   one side is host-backed and the other is inline-only, and a
    ///   `vec![0u8; len]` allocation is the cost of being correct.
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        match (self, other) {
            (Buffer::InMemory(a), Buffer::InMemory(b)) => a == b,
            (Buffer::Host(a), Buffer::Host(b)) => a.byte_eq(b),
            (Buffer::InMemory(b), Buffer::Host(h)) | (Buffer::Host(h), Buffer::InMemory(b)) => {
                h.byte_eq_inline(b)
            }
        }
    }
}

impl Eq for Buffer {}

impl std::hash::Hash for Buffer {
    /// Hash the bytes — required so byte-equal `Buffer`s share a hash
    /// (Eq/Hash contract). For `Buffer::Host` this materialises through
    /// the registry; `Buffer` is not used as a collection key in
    /// shared-core so the cost is theoretical, but the contract still
    /// has to hold.
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Buffer::InMemory(b) => b.hash(state),
            Buffer::Host(h) => {
                let len = h.len() as usize;
                let mut tmp = vec![0u8; len];
                if len > 0 {
                    h.read_into(0, len, &mut tmp);
                }
                tmp.hash(state);
            }
        }
    }
}

impl From<Bytes> for Buffer {
    fn from(b: Bytes) -> Self {
        Buffer::InMemory(b)
    }
}

impl From<Vec<u8>> for Buffer {
    fn from(v: Vec<u8>) -> Self {
        Buffer::InMemory(Bytes::from(v))
    }
}

impl From<&'static [u8]> for Buffer {
    fn from(b: &'static [u8]) -> Self {
        Buffer::InMemory(Bytes::from_static(b))
    }
}

impl From<&'static str> for Buffer {
    fn from(s: &'static str) -> Self {
        Buffer::InMemory(Bytes::from_static(s.as_bytes()))
    }
}

impl From<HostBufferHandle> for Buffer {
    fn from(h: HostBufferHandle) -> Self {
        Buffer::Host(h)
    }
}

// =====================================================================
// Encode side
// =====================================================================

/// A `BufMut` that may also accept payload buffers as `Buffer` values.
///
/// `push_buffer` lets each implementor decide how to land an `InMemory`
/// vs a `Host` body. Inline-only impls (`Vec<u8>`, `BytesMut`) copy
/// `InMemory` bytes and panic on `Host`. Host-aware impls (`BufferList`)
/// flush their scratch and push the buffer onto a queue, preserving the
/// `Host` reference for the embedder.
///
/// Length-prefix emission stays at the call site (see [`crate::proto`]),
/// using [`Buffer::len`] — this trait carries no registry plumbing.
pub trait RestateBufMut: BufMut {
    /// Land `buffer` at the current write position. The implementor
    /// chooses the policy per variant (copy into scratch, push onto a
    /// queue, panic if unsupported, etc.).
    fn push_buffer(&mut self, buffer: Buffer);
}

// =====================================================================
// Decode side
// =====================================================================

/// A `Buf` that may yield host-resident payload references when consumed.
///
/// Implementors expose a fast-path query (`try_take_host_slice`) that lets
/// decoders detect host-backed wire bytes at the field level without
/// runtime downcasting. Inline-only sources (`Bytes`, `&[u8]`) return
/// `None`, and the decoder falls back to standard `Buf::copy_to_bytes`.
pub trait RestateBuf: Buf {
    /// If the next `len` bytes of this source are a single contiguous
    /// host-buffer span, consume them and return the handle. Otherwise
    /// return `None` and leave the cursor in place — the caller falls
    /// back to copying bytes via standard `Buf` reads.
    ///
    /// `len` is the encoded field length (the prost decoder has already
    /// consumed the length varint at this point).
    fn try_take_host_slice(&mut self, len: usize) -> Option<HostBufferHandle>;
}

impl RestateBuf for Bytes {
    fn try_take_host_slice(&mut self, _len: usize) -> Option<HostBufferHandle> {
        None
    }
}

impl RestateBuf for &[u8] {
    fn try_take_host_slice(&mut self, _len: usize) -> Option<HostBufferHandle> {
        None
    }
}

/// Delegates `try_take_host_slice` through a `&mut` reference. Needed so
/// adapter wrappers like [`bytes::buf::Take`] can hold `&mut B` and still
/// expose the host-fast-path.
impl<B: RestateBuf + ?Sized> RestateBuf for &mut B {
    fn try_take_host_slice(&mut self, len: usize) -> Option<HostBufferHandle> {
        (**self).try_take_host_slice(len)
    }
}

/// Bounded view over a `RestateBuf`. The `Decoder` hands prost a
/// `Take<&mut BufferReader>` whose limit equals the current message body
/// length — this impl propagates the host fast-path through that bound.
impl<B: RestateBuf> RestateBuf for ::bytes::buf::Take<B> {
    fn try_take_host_slice(&mut self, len: usize) -> Option<HostBufferHandle> {
        if len > self.limit() {
            return None;
        }
        let h = self.get_mut().try_take_host_slice(len)?;
        let new_limit = self.limit() - len;
        self.set_limit(new_limit);
        Some(h)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::Message;

    impl Buffer {
        /// True for `InMemory` buffers.
        pub fn is_in_memory(&self) -> bool {
            matches!(self, Buffer::InMemory(_))
        }

        /// True for `Host` buffers.
        pub fn is_host(&self) -> bool {
            matches!(self, Buffer::Host(_))
        }

        /// Assert this buffer is in memory, and return it. For testing only.
        pub fn expect_in_memory(self) -> Bytes {
            match self {
                Buffer::InMemory(b) => b,
                Buffer::Host(_) => panic!(
                    "host-backed Buffer reached a code path that does not yet \
                 support host buffers; this is a shared-core bug"
                ),
            }
        }
    }

    /// Used only for testing
    impl RestateBufMut for Vec<u8> {
        fn push_buffer(&mut self, buffer: Buffer) {
            match buffer {
                Buffer::InMemory(b) => self.put(b.clone()),
                Buffer::Host(_) => panic!(
                    "RestateBufMut::push_buffer called on Vec<u8> with a Buffer::Host; \
                 only host-aware buffers (e.g. BufferWriter) support host buffers. \
                 This is a shared-core bug."
                ),
            }
        }
    }

    /// Used only for testing
    impl RestateBufMut for BytesMut {
        fn push_buffer(&mut self, buffer: Buffer) {
            match buffer {
                Buffer::InMemory(b) => self.put(b.clone()),
                Buffer::Host(_) => panic!(
                    "RestateBufMut::push_buffer called on BytesMut with a Buffer::Host; \
                 only host-aware buffers (e.g. BufferWriter) support host buffers. \
                 This is a shared-core bug."
                ),
            }
        }
    }

    /// Make a registry + a refcounted handle wrapping `bytes`.
    fn registered(bytes: &[u8]) -> (Arc<InMemoryHostBufferRegistry>, HostBufferHandle) {
        let registry = Arc::new(InMemoryHostBufferRegistry::new());
        let handle = registry.clone().make_handle(bytes);
        (registry, handle)
    }

    // #[test]
    // fn vec_u8_panics_on_push_buffer_host() {
    //     let (_reg, handle) = registered(b"");
    //     let mut buf: Vec<u8> = Vec::new();
    //     let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
    //         buf.push_buffer(Buffer::Host(handle));
    //     }));
    //     assert!(r.is_err(), "Vec<u8>::push_buffer should panic on Host");
    // }
    //
    // #[test]
    // fn vec_u8_accepts_in_memory_via_push_buffer() {
    //     let mut buf: Vec<u8> = Vec::new();
    //     buf.push_buffer(Buffer::InMemory(Bytes::from_static(b"hello")));
    //     assert_eq!(&buf[..], b"hello");
    // }

    #[test]
    fn bytes_yields_no_host_slice() {
        let mut b = Bytes::from_static(b"hello");
        assert_eq!(b.try_take_host_slice(5), None);
        assert_eq!(b.remaining(), 5, "cursor should not advance on None");
    }

    // -----------------------------------------------------------------
    // Refcount lifecycle tests
    // -----------------------------------------------------------------

    #[test]
    fn handle_drop_releases_underlying_buffer() {
        let (registry, handle) = registered(b"hi");
        assert_eq!(registry.live_buffers(), 1);
        drop(handle);
        assert_eq!(registry.live_buffers(), 0, "drop must release buffer");
    }

    #[test]
    fn handle_clone_bumps_refcount() {
        let (registry, handle) = registered(b"hi");
        let cloned = handle.clone();
        drop(handle);
        // Clone keeps it alive.
        assert_eq!(registry.live_buffers(), 1);
        drop(cloned);
        assert_eq!(registry.live_buffers(), 0);
    }

    #[test]
    fn sub_view_bumps_refcount_and_drops_independently() {
        let (registry, parent) = registered(b"abcdef");
        let sub = parent.sub_view(2, 3);
        assert_eq!(sub.id(), parent.id());
        assert_eq!(sub.offset(), 2);
        assert_eq!(sub.len(), 3);
        drop(parent);
        // Sub-view keeps the underlying buffer alive.
        assert_eq!(registry.live_buffers(), 1);
        let mut dst = vec![0u8; 3];
        sub.read_into(0, 3, &mut dst);
        assert_eq!(&dst[..], b"cde");
        drop(sub);
        assert_eq!(registry.live_buffers(), 0);
    }

    // -----------------------------------------------------------------
    // BufferWriter tests
    // -----------------------------------------------------------------

    fn drain_buffers(buf: &mut BufferWriter) -> Vec<Buffer> {
        let mut out = Vec::new();
        while let Some(f) = buf.pop_front() {
            out.push(f);
        }
        out
    }

    fn concat_buffers_with_registry(bufs: &[Buffer], registry: &dyn HostBufferRegistry) -> Vec<u8> {
        let mut out = Vec::new();
        for f in bufs {
            match f {
                Buffer::InMemory(b) => out.extend_from_slice(b),
                Buffer::Host(h) => {
                    let len = h.len() as usize;
                    let start = out.len();
                    out.resize(start + len, 0);
                    registry.read_into(h, 0, len, &mut out[start..]);
                }
            }
        }
        out
    }

    #[test]
    fn buffer_writer_pure_inline() {
        let mut buf = BufferWriter::new();
        buf.put_slice(b"hello world");
        let bufs = drain_buffers(&mut buf);
        assert_eq!(bufs.len(), 1);
        match &bufs[0] {
            Buffer::InMemory(b) => assert_eq!(&b[..], b"hello world"),
            Buffer::Host(_) => panic!("expected InMemory"),
        }
        assert!(buf.is_empty());
    }

    #[test]
    fn buffer_writer_mixed_inline_and_host() {
        let (_reg, h1) = registered(b"first");
        let (_reg2, h2) = registered(b"second");
        let h1_id = h1.id();
        let h2_id = h2.id();
        let mut buf = BufferWriter::new();
        buf.put_slice(b"AAA");
        buf.push_buffer(Buffer::Host(h1));
        buf.put_slice(b"BBB");
        buf.push_buffer(Buffer::Host(h2));
        let bufs = drain_buffers(&mut buf);
        assert_eq!(bufs.len(), 4);
        match &bufs[0] {
            Buffer::InMemory(b) => assert_eq!(&b[..], b"AAA"),
            _ => panic!("expected InMemory AAA"),
        }
        match &bufs[1] {
            Buffer::Host(h) => assert_eq!(h.id(), h1_id),
            _ => panic!("expected Host h1"),
        }
        match &bufs[2] {
            Buffer::InMemory(b) => assert_eq!(&b[..], b"BBB"),
            _ => panic!("expected InMemory BBB"),
        }
        match &bufs[3] {
            Buffer::Host(h) => assert_eq!(h.id(), h2_id),
            _ => panic!("expected Host h2"),
        }
    }

    #[test]
    fn buffer_writer_push_host_with_empty_scratch_emits_no_leading_inline() {
        let (_reg, handle) = registered(b"data");
        let id = handle.id();
        let mut buf = BufferWriter::new();
        // Start with push_buffer(Host) while scratch is empty — no leading
        // Buffer::InMemory(empty) should be emitted.
        buf.push_buffer(Buffer::Host(handle));
        buf.put_slice(b"trailing");
        let bufs = drain_buffers(&mut buf);
        assert_eq!(bufs.len(), 2);
        match &bufs[0] {
            Buffer::Host(h) => assert_eq!(h.id(), id),
            _ => panic!("expected Host first"),
        }
        match &bufs[1] {
            Buffer::InMemory(b) => assert_eq!(&b[..], b"trailing"),
            _ => panic!("expected InMemory trailing"),
        }
    }

    #[test]
    fn buffer_writer_drain_iter() {
        let (_reg, handle) = registered(b"x");
        let mut buf = BufferWriter::new();
        buf.put_slice(b"a");
        buf.push_buffer(Buffer::Host(handle));
        buf.put_slice(b"b");
        let bufs: Vec<Buffer> = buf.drain().collect();
        assert_eq!(bufs.len(), 3);
        assert!(buf.is_empty());
    }

    // -----------------------------------------------------------------
    // Encode-through tests against a generated Message
    // -----------------------------------------------------------------

    #[derive(Clone, PartialEq, restate_wire_derive::Message)]
    struct TestMsg {
        #[prost(string, tag = "1")]
        name: String,
        #[prost(bytes = "buffer", tag = "2")]
        body: Buffer,
        #[prost(uint32, tag = "3")]
        trailer: u32,
    }

    #[test]
    fn buffer_writer_encode_inline_payload_byte_identical() {
        let host: Arc<dyn HostBufferRegistry> = Arc::new(InMemoryHostBufferRegistry::new());

        let msg = TestMsg {
            name: "abc".to_owned(),
            body: Buffer::InMemory(Bytes::from_static(b"payload-bytes")),
            trailer: 99,
        };

        // Encode through BufferWriter
        let mut fbuf = BufferWriter::new();
        msg.encode_raw(&mut fbuf);
        let bufs = drain_buffers(&mut fbuf);
        let buf_concat = concat_buffers_with_registry(&bufs, &*host);

        // Encode through plain BytesMut for comparison
        let mut plain: BytesMut = BytesMut::new();
        msg.encode_raw(&mut plain);

        assert_eq!(buf_concat, plain.as_ref());
        // With only InMemory payload, all buffers must be InMemory.
        assert!(bufs.iter().all(|f| matches!(f, Buffer::InMemory(_))));
    }

    #[test]
    fn buffer_writer_encode_host_payload_emits_host_buffer() {
        let in_mem = Arc::new(InMemoryHostBufferRegistry::new());
        let registry: Arc<dyn HostBufferRegistry> = in_mem.clone();
        let payload_bytes = b"host-owned-payload-bytes";
        let handle = in_mem.clone().make_handle(payload_bytes);
        let handle_id = handle.id();

        let msg = TestMsg {
            name: "abc".to_owned(),
            body: Buffer::Host(handle),
            trailer: 99,
        };

        // Encode through BufferWriter
        let mut fbuf = BufferWriter::new();
        msg.encode_raw(&mut fbuf);
        let bufs = drain_buffers(&mut fbuf);

        // There must be exactly one Host buffer, carrying our handle's id.
        let host_ids: Vec<u32> = bufs
            .iter()
            .filter_map(|f| match f {
                Buffer::Host(h) => Some(h.id()),
                _ => None,
            })
            .collect();
        assert_eq!(host_ids, vec![handle_id]);

        // Concat (resolving Host via registry) must equal a fully inline
        // encoding of the same logical message.
        let buf_concat = concat_buffers_with_registry(&bufs, &*registry);

        // Inline-equivalent reference: rebuild the message with the
        // payload materialised as InMemory(bytes).
        let inline_msg = TestMsg {
            name: msg.name.clone(),
            body: Buffer::InMemory(Bytes::copy_from_slice(payload_bytes)),
            trailer: msg.trailer,
        };
        let mut reference: BytesMut = BytesMut::new();
        inline_msg.encode_raw(&mut reference);

        assert_eq!(buf_concat, reference.as_ref());

        // And the same against a parallel prost::Message struct (proves
        // byte identity at the protobuf wire level).
        #[derive(Clone, prost::Message)]
        struct ProstTestMsg {
            #[prost(string, tag = "1")]
            name: String,
            #[prost(bytes = "bytes", tag = "2")]
            body: Bytes,
            #[prost(uint32, tag = "3")]
            trailer: u32,
        }
        let prost_msg = ProstTestMsg {
            name: msg.name.clone(),
            body: Bytes::copy_from_slice(payload_bytes),
            trailer: msg.trailer,
        };
        let mut prost_buf: Vec<u8> = Vec::new();
        prost::Message::encode_raw(&prost_msg, &mut prost_buf);
        assert_eq!(buf_concat, prost_buf);
    }

    // -----------------------------------------------------------------
    // BufferReader tests
    // -----------------------------------------------------------------

    #[test]
    fn buffer_reader_buf_reads_match_materialised_bytes() {
        let (_reg, handle) = registered(b"abcdefghij");
        let mut src = BufferReader::new();
        src.push(Buffer::Host(handle));

        assert_eq!(src.remaining(), 10);
        assert_eq!(src.chunk(), b"abcdefghij");

        src.advance(3);
        assert_eq!(src.remaining(), 7);
        assert_eq!(src.chunk(), b"defghij");

        let rest = src.copy_to_bytes(src.remaining());
        assert_eq!(&rest[..], b"defghij");
        assert_eq!(src.remaining(), 0);
    }

    #[test]
    fn buffer_reader_in_memory_chunk_returns_segment_bytes_directly() {
        let mut src = BufferReader::new();
        src.push(Bytes::from_static(b"hello world"));
        assert_eq!(src.chunk(), b"hello world");
        src.advance(6);
        assert_eq!(src.chunk(), b"world");
    }

    #[test]
    fn buffer_reader_fast_path_whole_untouched_body() {
        let (_reg, handle) = registered(b"whole-body-zero-copy");
        let parent_id = handle.id();

        let mut src = BufferReader::new();
        src.push(Buffer::Host(handle));
        let taken = src
            .try_take_host_slice(20)
            .expect("sub-view must be minted when len fits remaining");
        assert_eq!(taken.id(), parent_id);
        assert_eq!(taken.offset(), 0);
        assert_eq!(taken.len(), 20);
        assert_eq!(src.remaining(), 0, "view must be drained");
    }

    #[test]
    fn buffer_reader_sub_view_after_advance() {
        let (_reg, handle) = registered(b"abcdef");
        let parent_id = handle.id();

        let mut src = BufferReader::new();
        src.push(Buffer::Host(handle));
        src.advance(2);
        let sub = src.try_take_host_slice(3).expect("sub-view minted");
        assert_eq!(sub.id(), parent_id);
        assert_eq!(sub.offset(), 2);
        assert_eq!(sub.len(), 3);
        assert_eq!(src.remaining(), 1, "cursor advanced past the sub-view");
    }

    #[test]
    fn buffer_reader_sub_view_when_len_less_than_view() {
        let (_reg, handle) = registered(b"abcdef");
        let parent_id = handle.id();

        let mut src = BufferReader::new();
        src.push(Buffer::Host(handle));
        let sub = src.try_take_host_slice(3).expect("sub-view minted");
        assert_eq!(sub.id(), parent_id);
        assert_eq!(sub.offset(), 0);
        assert_eq!(sub.len(), 3);
        assert_eq!(src.remaining(), 3);
    }

    #[test]
    fn buffer_reader_try_take_returns_none_when_head_is_inline() {
        let mut src = BufferReader::new();
        src.push(Bytes::from_static(b"abcdef"));
        assert_eq!(src.try_take_host_slice(3), None);
        assert_eq!(src.remaining(), 6, "cursor must not move on None");
    }

    #[test]
    fn buffer_reader_try_take_returns_none_when_len_exceeds_head_remaining() {
        let (_reg, handle) = registered(b"abcdef");
        let mut src = BufferReader::new();
        src.push(Buffer::Host(handle));
        assert_eq!(src.try_take_host_slice(7), None);
        assert_eq!(src.remaining(), 6, "cursor must not move on None");
    }

    #[test]
    fn buffer_reader_spans_multiple_segments_in_order() {
        let (_reg, h) = registered(b"world");
        let mut src = BufferReader::new();
        src.push(Bytes::from_static(b"hello "));
        src.push(Buffer::Host(h));
        src.push(Bytes::from_static(b"!"));
        assert_eq!(src.remaining(), 12);

        // Chunk view starts at the head segment.
        assert_eq!(src.chunk(), b"hello ");
        src.advance(6);
        // Head transitions to Host; scratch must have been (re)filled.
        assert_eq!(src.chunk(), b"world");
        src.advance(5);
        assert_eq!(src.chunk(), b"!");
        src.advance(1);
        assert_eq!(src.remaining(), 0);
        assert_eq!(src.chunk(), b"");
    }

    #[test]
    fn buffer_reader_cross_segment_advance_lands_in_middle_of_next_segment() {
        let (_reg, h) = registered(b"WORLD");
        let mut src = BufferReader::new();
        src.push(Bytes::from_static(b"hello"));
        src.push(Buffer::Host(h));
        // advance(7) consumes all 5 of "hello", then 2 bytes into "WORLD".
        src.advance(7);
        assert_eq!(src.remaining(), 3);
        assert_eq!(src.chunk(), b"RLD");
    }

    #[test]
    fn buffer_reader_copy_to_bytes_crosses_segment() {
        let (_reg, h) = registered(b"world");
        let mut src = BufferReader::new();
        src.push(Bytes::from_static(b"hello "));
        src.push(Buffer::Host(h));
        let out = src.copy_to_bytes(11);
        assert_eq!(&out[..], b"hello world");
        assert_eq!(src.remaining(), 0);
    }

    #[test]
    fn buffer_reader_chunk_invariant_under_scratch_boundary_advance() {
        // Build a host head larger than SCRATCH_SIZE so the scratch window
        // is a proper subset of the head's remaining bytes. After every
        // cursor-moving op, `chunk()` must remain non-empty while
        // `remaining() > 0`.
        let payload: Vec<u8> = (0..(super::reader::SCRATCH_SIZE * 3) as u32)
            .map(|i| (i & 0xff) as u8)
            .collect();
        let (_reg, h) = registered(&payload);
        let mut src = BufferReader::new();
        src.push(Buffer::Host(h));

        // Walk in 100-byte hops, asserting the invariant at each step.
        let mut consumed = 0usize;
        while src.remaining() > 0 {
            assert!(
                !src.chunk().is_empty(),
                "Buf::chunk must be non-empty when remaining() > 0 (consumed = {})",
                consumed,
            );
            let step = 100.min(src.remaining());
            // Sanity: chunk bytes must equal payload[consumed..consumed+chunk.len()].
            let chunk_len = src.chunk().len();
            let expected = &payload[consumed..consumed + chunk_len];
            assert_eq!(
                src.chunk(),
                expected,
                "chunk bytes mismatch at offset {consumed}"
            );
            src.advance(step);
            consumed += step;
        }
        assert_eq!(consumed, payload.len());
        assert_eq!(src.chunk(), b"");
    }

    #[test]
    fn buffer_reader_decodes_generated_message_round_trip_with_sub_view() {
        let in_mem = Arc::new(InMemoryHostBufferRegistry::new());

        let msg = TestMsg {
            name: "abc".to_owned(),
            body: Buffer::InMemory(Bytes::from_static(b"payload-bytes")),
            trailer: 99,
        };

        // Encode through plain BytesMut.
        let mut wire: BytesMut = BytesMut::new();
        msg.encode_raw(&mut wire);

        // Register the wire bytes with the host registry, build a
        // BufferReader around them, and decode.
        let body_handle = in_mem.clone().make_handle(wire.as_ref());
        let body_id = body_handle.id();
        let mut src = BufferReader::new();
        src.push(Buffer::Host(body_handle));
        let decoded = TestMsg::decode(&mut src).expect("decode through BufferReader must succeed");

        assert_eq!(decoded.name, msg.name);
        assert_eq!(decoded.trailer, msg.trailer);
        // The buffer field comes back as a Buffer::Host sub-view of the
        // input wire-bytes handle — proving the decode side is zero-copy.
        match &decoded.body {
            Buffer::Host(sub) => {
                assert_eq!(sub.id(), body_id);
                assert_ne!(sub.offset(), 0, "sub-view should sit past tag+length");
                assert_eq!(sub.len() as usize, b"payload-bytes".len());

                // Verify the sub-view actually points at "payload-bytes".
                let mut dst = vec![0u8; sub.len() as usize];
                sub.read_into(0, sub.len() as usize, &mut dst);
                assert_eq!(&dst[..], b"payload-bytes");
            }
            Buffer::InMemory(_) => panic!(
                "expected a Buffer::Host sub-view; BufferReader decode must \
                 be zero-copy for `bytes = \"buffer\"` fields"
            ),
        }
    }

    // -----------------------------------------------------------------
    // Buffer equality tests (replay correctness)
    // -----------------------------------------------------------------

    #[test]
    fn buffer_eq_host_host_with_different_ids_compares_bytes() {
        // Two separate registrations of the same bytes — different ids,
        // identical content. Identity-only equality would say they're
        // unequal; byte-equality (via the registry) must say equal.
        let registry = Arc::new(InMemoryHostBufferRegistry::new());
        let a = registry.clone().make_handle(b"payload-bytes");
        let b = registry.clone().make_handle(b"payload-bytes");
        assert_ne!(a.id(), b.id());
        assert_eq!(Buffer::Host(a), Buffer::Host(b));
    }

    #[test]
    fn buffer_eq_host_host_different_bytes() {
        let registry = Arc::new(InMemoryHostBufferRegistry::new());
        let a = registry.clone().make_handle(b"payload-bytes");
        let b = registry.clone().make_handle(b"payload-OTHER");
        assert_ne!(Buffer::Host(a), Buffer::Host(b));
    }

    #[test]
    fn buffer_eq_host_inmemory_matches_when_bytes_equal() {
        // The very-bad case: a wire-decoded payload arrived as Host
        // (sub_view), the actual replay payload is InMemory bytes.
        // Materialise + compare must say equal.
        let (_reg, host) = registered(b"payload-bytes");
        let inline = Buffer::InMemory(Bytes::from_static(b"payload-bytes"));
        assert_eq!(Buffer::Host(host.clone()), inline);
        assert_eq!(inline, Buffer::Host(host));
    }

    #[test]
    fn buffer_eq_host_inmemory_different_bytes() {
        let (_reg, host) = registered(b"payload-bytes");
        let inline = Buffer::InMemory(Bytes::from_static(b"different-len-bytes"));
        assert_ne!(Buffer::Host(host.clone()), inline);
    }

    #[test]
    fn buffer_eq_sub_view_against_inline_bytes() {
        // Decoder fast path: the sub_view points at a sub-range of the
        // wire-bytes buffer. Replay compares against an InMemory(Bytes)
        // that came from an inline-only encode. Must report equal.
        let (_reg, parent) = registered(b"AAApayload-bytesZZZ");
        let sub = parent.sub_view(3, 13); // "payload-bytes"
        let inline = Buffer::InMemory(Bytes::from_static(b"payload-bytes"));
        assert_eq!(Buffer::Host(sub), inline);
    }

    #[test]
    fn buffer_eq_view_identity_short_circuits() {
        // Two clones of the same handle (same id, offset, len) must
        // compare equal via the identity short-circuit without consulting
        // the registry. Hard to assert "no registry call" directly, but
        // exercising the path keeps the short-circuit in regression
        // tests.
        let (_reg, h) = registered(b"abc");
        let cloned = h.clone();
        assert_eq!(Buffer::Host(h), Buffer::Host(cloned));
    }
}
