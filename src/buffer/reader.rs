// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! [`BufferReader`] — the host-aware decode source owned by the
//! protocol `Decoder`. See module docs in [`crate::buffer`].

use super::{Buffer, HostBufferHandle, RestateBuf, Segment};
use bytes::{Buf, Bytes};
use std::collections::VecDeque;

/// Scratch window size for `BufferReader` over a `Buffer::Host` head.
/// Well above the 10-byte varint floor, so prost's `decode_varint` always
/// hits its fast path on a single `chunk()` call. One inline `[u8; 256]`
/// per reader — no per-fill heap allocation.
///
/// `pub(super)` so the parent module's tests can drive the scratch
/// boundary directly.
pub(super) const SCRATCH_SIZE: usize = 256;

/// A long-lived host-aware byte source. Owned by the protocol
/// `Decoder` — the protocol layer appends buffers via [`Self::push`] and
/// reads message headers + bodies through the `Buf` + `RestateBuf`
/// impls. Each message body is passed to prost as a
/// `bytes::buf::Take<&mut BufferReader>` bounded to the encoded body
/// length.
///
/// # Streaming behaviour
///
/// `BufferReader` holds a queue of `Buffer` segments. Each segment
/// carries its own cursor: `Buffer::InMemory` uses `Bytes::split_to` /
/// `Bytes`'s `Buf::advance`; `Buffer::Host` uses
/// [`HostBufferHandle::advance`]. As the decoder consumes bytes, the
/// front segment shrinks in place and is popped when exhausted.
///
/// When the prost decoder encounters a `bytes = "buffer"` field and
/// calls [`RestateBuf::try_take_host_slice`] with the field length, the
/// fast path mints a **sub-view** of the head host segment (via
/// [`HostBufferHandle::sub_view`]) and advances past it — zero copy, no
/// scratch fill. The fast path is only available when the field fits
/// entirely within the current head host segment.
///
/// # `Buf::chunk` invariant
///
/// `bytes::Buf` requires `chunk() empty IFF remaining() == 0`. prost's
/// `decode_varint` reads `let bytes = buf.chunk()` and errors out on
/// `bytes.is_empty()` *before* ever falling through to
/// `decode_varint_slow`; if our `chunk()` returns `&[]` while
/// `remaining() > 0`, varint decode breaks.
///
/// We satisfy this by maintaining: when the queue front is
/// `Buffer::InMemory`, `chunk()` returns its bytes directly; when the
/// front is `Buffer::Host` with bytes remaining, `chunk()` returns a
/// scratch window that mirrors the head's first
/// `min(SCRATCH_SIZE, head.len())` bytes. The scratch is eagerly
/// (re)filled by every cursor-moving op (`push` when the queue
/// transitions empty→non-empty, plus `advance` / `copy_to_bytes` /
/// `try_take_host_slice`).
pub struct BufferReader {
    /// Segments at the front of the queue; each carries its own cursor
    /// via `Bytes::split_to` / `HostBufferHandle::advance`. No empty
    /// segments are kept at the front — popped after every
    /// cursor-moving op.
    segments: VecDeque<Buffer>,
    /// Inline scratch covering the next `scratch_end - scratch_start`
    /// bytes of the current head when the head is `Buffer::Host`. No
    /// heap allocation per fill. Invariant: `scratch_end == 0` iff the
    /// scratch is invalid (head is `InMemory` or queue is empty).
    /// Eagerly (re)filled by every cursor-moving op so the `Buf::chunk`
    /// invariant above holds.
    scratch: [u8; SCRATCH_SIZE],
    scratch_start: u16,
    scratch_end: u16,
}

impl Default for BufferReader {
    fn default() -> Self {
        Self::new()
    }
}

impl BufferReader {
    /// Construct an empty `BufferReader`.
    pub fn new() -> Self {
        Self {
            segments: VecDeque::new(),
            scratch: [0u8; SCRATCH_SIZE],
            scratch_start: 0,
            scratch_end: 0,
        }
    }

    /// Append a buffer to the back of the queue. Empty buffers are
    /// silently dropped. If the queue was empty and the new head is a
    /// `Buffer::Host`, the scratch is filled immediately so the
    /// `Buf::chunk` invariant holds before the next read.
    pub fn push(&mut self, buf: impl Into<Buffer>) {
        let buf: Buffer = buf.into();
        if buf.is_empty() {
            return;
        }
        let was_empty = self.segments.is_empty();
        self.segments.push_back(buf);
        if was_empty {
            self.fill_scratch();
        }
    }

    /// Total bytes remaining across all segments.
    pub fn total_remaining(&self) -> usize {
        self.segments.iter().map(|b| b.len()).sum()
    }

    /// True if no bytes remain.
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// If the current head is a `Buffer::Host` with bytes remaining,
    /// fill the scratch with its first `min(SCRATCH_SIZE, head.len())`
    /// bytes. No-op when head is `InMemory` (its bytes are exposed
    /// directly via `Buf::chunk`) or the queue is empty.
    ///
    /// Callers should reset `scratch_start = scratch_end = 0` *before*
    /// calling this if they've invalidated the previous scratch window.
    fn fill_scratch(&mut self) {
        if let Some(Buffer::Host(h)) = self.segments.front() {
            let remaining = h.len() as usize;
            if remaining == 0 {
                return;
            }
            let fill = remaining.min(SCRATCH_SIZE);
            h.read_into(0, fill, &mut self.scratch[..fill]);
            self.scratch_start = 0;
            self.scratch_end = fill as u16;
        }
    }
}

impl Buf for BufferReader {
    fn remaining(&self) -> usize {
        self.total_remaining()
    }

    fn chunk(&self) -> &[u8] {
        match self.segments.front() {
            Some(Buffer::InMemory(b)) => &b[..],
            Some(Buffer::Host(_)) => {
                &self.scratch[self.scratch_start as usize..self.scratch_end as usize]
            }
            None => &[],
        }
    }

    fn advance(&mut self, mut cnt: usize) {
        while cnt > 0 {
            let head = self
                .segments
                .front_mut()
                .expect("BufferReader::advance past end of buffer");
            let head_len = head.len();
            if head_len > cnt {
                // Stay within the current head.
                match head {
                    Buffer::InMemory(b) => {
                        // `Bytes::advance` shrinks from the front in place.
                        b.advance(cnt);
                    }
                    Buffer::Host(h) => {
                        h.advance(cnt as u32);
                    }
                }
                // Sync the scratch window.
                let scratch_window = (self.scratch_end - self.scratch_start) as usize;
                if scratch_window > cnt {
                    // Still within the existing scratch window.
                    self.scratch_start += cnt as u16;
                } else {
                    // Scratch exhausted; invalidate and refill from the
                    // new head position.
                    self.scratch_start = 0;
                    self.scratch_end = 0;
                    self.fill_scratch();
                }
                return;
            }
            // Consume entire head and continue.
            cnt -= head_len;
            self.segments.pop_front();
            self.scratch_start = 0;
            self.scratch_end = 0;
        }
        // Multi-segment advance landed on a fresh head; refill scratch
        // if the new head is Host.
        if self.scratch_end == 0 {
            self.fill_scratch();
        }
    }

    fn copy_to_bytes(&mut self, len: usize) -> Bytes {
        if len == 0 {
            return Bytes::new();
        }
        // Single-segment fast paths.
        if let Some(head) = self.segments.front_mut() {
            if head.len() >= len {
                let out = match head {
                    Buffer::InMemory(b) => b.split_to(len),
                    Buffer::Host(h) => {
                        let mut dst = vec![0u8; len];
                        h.read_into(0, len, &mut dst);
                        h.advance(len as u32);
                        Bytes::from(dst)
                    }
                };
                if head.is_empty() {
                    self.segments.pop_front();
                }
                self.scratch_start = 0;
                self.scratch_end = 0;
                self.fill_scratch();
                return out;
            }
        }
        // Cross-segment fallback via the generic `Buf` machinery.
        let mut dst = vec![0u8; len];
        self.copy_to_slice(&mut dst);
        Bytes::from(dst)
    }
}

impl RestateBuf for BufferReader {
    /// Consume the next `len` bytes as a single (possibly multi-segment)
    /// host handle.
    ///
    /// Coalesces across consecutive `Buffer::Host` segments at the front
    /// of the queue. Returns `None` if:
    /// - `len` is zero,
    /// - the head segment is `Buffer::InMemory` (can't satisfy a Host
    ///   request from inline bytes), or
    /// - the front `Buffer::Host` segments don't add up to `len` before
    ///   an `InMemory` segment intervenes (or the queue runs out).
    fn try_take_host_slice(&mut self, len: usize) -> Option<HostBufferHandle> {
        let len_u32 = u32::try_from(len).ok()?;
        if len_u32 == 0 {
            return None;
        }

        // Phase 1: confirm we can satisfy `len` from a contiguous run
        // of `Buffer::Host` segments at the front of the queue.
        let mut accumulated = 0u32;
        for buf in self.segments.iter() {
            let Buffer::Host(h) = buf else {
                return None;
            };
            accumulated = accumulated.saturating_add(h.len());
            if accumulated >= len_u32 {
                break;
            }
        }
        if accumulated < len_u32 {
            return None;
        }

        // Snapshot the registry from the head — every host segment in
        // the same `BufferReader` shares the same registry by
        // construction, so the head's reference is canonical.
        let registry = match self.segments.front()? {
            Buffer::Host(h) => h.registry(),
            // Phase 1 already guaranteed Host at the head.
            _ => unreachable!(),
        };

        // Phase 2: drain `len_u32` bytes by repeatedly minting
        // sub-views of the head and transferring their segments into
        // `taken`. `sub_view` retains; we `mem::forget` so the
        // refcount-shares end up owned by the resulting handle (built
        // via `from_segments_no_retain`).
        let mut taken: Vec<Segment> = Vec::new();
        let mut remaining = len_u32;
        while remaining > 0 {
            let head = self.segments.front_mut()?;
            let Buffer::Host(h) = head else {
                // Phase 1 guarantees this can't happen, but stay
                // defensive — a hostile interleaving would otherwise
                // panic deeper.
                return None;
            };
            let take = h.len().min(remaining);
            let sub = h.sub_view(0, take);
            taken.extend_from_slice(sub.segments());
            std::mem::forget(sub);
            h.advance(take);
            if h.is_empty() {
                self.segments.pop_front();
            }
            remaining -= take;
        }

        self.scratch_start = 0;
        self.scratch_end = 0;
        self.fill_scratch();
        Some(HostBufferHandle::from_segments_no_retain(registry, taken))
    }
}
