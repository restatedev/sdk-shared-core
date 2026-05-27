// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! [`BufferWriter`] — the host-aware encode buffer owned by
//! `vm::context::Output`. See module docs in [`crate::buffer`].

use super::{Buffer, RestateBufMut};
use bytes::{BufMut, BytesMut};
use std::collections::VecDeque;

/// A `RestateBufMut` that fans incoming writes out into a `VecDeque<Buffer>`.
///
/// Owned long-lived by `vm::context::Output` — the encoder writes the
/// 8-byte message header and the prost-encoded body into the same
/// writer, and `take_next` pops one `Buffer` at a time off the front for
/// the HTTP layer to drain.
///
/// Inline scalar/control bytes accumulate into a `BytesMut` scratch.
/// [`push_buffer`](RestateBufMut::push_buffer) dispatches on the variant:
/// `InMemory` bytes copy into scratch (matching the `put_slice` path
/// for inline bytes), while `Host` flushes the current scratch as a
/// `Buffer::InMemory` (zero-copy via `BytesMut::split()`) and pushes the
/// `Buffer::Host(handle)` after it. The result is a sequence equivalent to
/// "inline-bytes... host-buffer... inline-bytes... host-buffer..." that the
/// embedder can splice onto the wire one buffer at a time.
pub struct BufferWriter {
    /// Scratch buffer for inline bytes accumulating between
    /// `push_buffer(Host)` flushes.
    scratch: BytesMut,
    /// Completed buffers, in emission order.
    buffers: VecDeque<Buffer>,
}

impl Default for BufferWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl BufferWriter {
    /// Construct an empty `BufferWriter`.
    pub fn new() -> Self {
        Self {
            scratch: BytesMut::new(),
            buffers: VecDeque::new(),
        }
    }

    /// Construct an empty `BufferWriter` with a pre-allocated inline
    /// scratch capacity (useful when the upper bound on inline bytes is
    /// known up-front).
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            scratch: BytesMut::with_capacity(cap),
            buffers: VecDeque::new(),
        }
    }

    /// Pop one buffer from the front of the queue.
    ///
    /// On the first call after writes, if the scratch is non-empty, it is
    /// flushed as a trailing `Buffer::InMemory` first — otherwise the last
    /// bytes written would be stuck in scratch forever. Subsequent calls
    /// just pop from the queue.
    pub fn pop_front(&mut self) -> Option<Buffer> {
        if !self.scratch.is_empty() {
            let inline = self.scratch.split().freeze();
            self.buffers.push_back(Buffer::InMemory(inline));
        }
        self.buffers.pop_front()
    }

    /// Drain all buffers as an iterator. Convenience wrapper around
    /// repeated [`Self::pop_front`] calls.
    pub fn drain(&mut self) -> impl Iterator<Item = Buffer> + '_ {
        // Flush scratch eagerly so the iterator sees every buffer.
        if !self.scratch.is_empty() {
            let inline = self.scratch.split().freeze();
            self.buffers.push_back(Buffer::InMemory(inline));
        }
        self.buffers.drain(..)
    }

    /// True iff there are no pending buffers AND the scratch is empty.
    pub fn is_empty(&self) -> bool {
        self.buffers.is_empty() && self.scratch.is_empty()
    }
}

// SAFETY: All BufMut methods either delegate to the underlying `BytesMut`
// (which itself implements `unsafe impl BufMut` and upholds the invariants),
// or return `usize::MAX` from `remaining_mut` — matching the standard
// pattern used by `Vec<u8>` for an unbounded-growth buffer.
unsafe impl BufMut for BufferWriter {
    fn remaining_mut(&self) -> usize {
        // The scratch can grow unboundedly via `BytesMut`. Standard
        // practice (matches `Vec<u8>::remaining_mut`) is to return
        // `usize::MAX`.
        usize::MAX
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        // SAFETY: forwarded; caller upholds BufMut::advance_mut invariants
        // for the underlying BytesMut.
        unsafe { self.scratch.advance_mut(cnt) }
    }

    fn chunk_mut(&mut self) -> &mut ::bytes::buf::UninitSlice {
        self.scratch.chunk_mut()
    }
}

impl RestateBufMut for BufferWriter {
    fn push_buffer(&mut self, buffer: Buffer) {
        match buffer {
            Buffer::InMemory(b) => {
                // Match the `put_slice` route: inline bytes coalesce into
                // scratch so small payloads don't fragment the wire stream.
                self.scratch.put_slice(&b);
            }
            Buffer::Host(handle) => {
                if !self.scratch.is_empty() {
                    // `split()` takes the current scratch contents and leaves
                    // the BytesMut empty (potentially reusing the same capacity
                    // for subsequent writes). Zero-copy.
                    let inline = self.scratch.split().freeze();
                    self.buffers.push_back(Buffer::InMemory(inline));
                }
                self.buffers.push_back(Buffer::Host(handle));
            }
        }
    }
}
