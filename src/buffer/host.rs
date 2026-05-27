// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Host buffer registry machinery — the refcounted `HostBufferHandle`,
//! its [`HostBufferRegistry`] trait, and the default panicking /
//! test-only registry implementations.
//!
//! Opt-in plumbing that lets payload bytes live in embedder memory
//! rather than being copied into shared-core's address space. See
//! `RFC-host-owned-payloads.md` for the design rationale.
//!
//! Everything here is re-exported from the parent [`crate::buffer`]
//! module so the public API surface stays in one place; this submodule
//! is pure code organization.

#[cfg(test)]
use bytes::Bytes;
use std::sync::Arc;

/// One contiguous view into a registered host buffer.
///
/// A [`HostBufferHandle`] is either a single `Segment` (the common
/// case — input chunk, decoder sub-view of a single chunk) or a vector
/// of segments (when the decoder coalesces a body that spans multiple
/// `Buffer::Host` segments). Per-segment retain/release semantics carry
/// across both cases.
#[derive(Copy, Clone, Debug)]
pub struct Segment {
    pub id: u32,
    pub offset: u32,
    pub len: u32,
}

/// Refcounted handle to a byte buffer (or sub-range thereof) owned by the
/// embedding host.
///
/// The handle is non-`Copy`. `Clone` bumps the SDK-side refcount via
/// [`HostBufferRegistry::retain`] for every segment; `Drop` decrements
/// via [`HostBufferRegistry::release`] for every segment. The SDK frees
/// the underlying buffer when the refcount for an id reaches zero.
///
/// # View semantics
///
/// A handle is a *view* into one or more registered buffers. Each
/// segment carries `(id, offset, len)` — the byte range
/// `[offset .. offset + len]` within `id`. Two clones share segments
/// 1-for-1 but each carries an independent refcount-share per segment.
///
/// [`HostBufferHandle::sub_view`] mints a fresh view of a sub-range of
/// `self`'s window, bumping the per-segment refcount for each segment
/// it touches. [`HostBufferHandle::advance`] shrinks the window from
/// the left, releasing any segments fully drained from the front.
///
/// # Buffer immutability invariant
///
/// Bytes behind a handle MUST be content-immutable for as long as any
/// handle to them lives — length, byte contents, and identity all stable
/// until the last refcount-share is released.
///
/// # WASM ABI
///
/// Single-segment handles round-trip across the WASM ABI as
/// `(id, offset, len)` tuples via [`HostBufferHandle::from_parts`].
/// Multi-segment handles round-trip as a list of segments via
/// [`HostBufferHandle::from_segments_no_retain`]. Neither constructor
/// bumps refcounts — the caller is responsible for the initial
/// refcount-shares.
pub struct HostBufferHandle {
    storage: HandleStorage,
    registry: Arc<dyn HostBufferRegistry>,
}

#[derive(Clone, Debug)]
enum HandleStorage {
    Single(Segment),
    Multi {
        segments: Box<[Segment]>,
        total_len: u32,
    },
}

impl HostBufferHandle {
    /// Construct a single-segment handle from raw `(id, offset, len)`
    /// and a registry reference WITHOUT bumping the refcount. The
    /// caller is responsible for having already established (or
    /// transferred) a refcount-share.
    ///
    /// Used by SDK WASM bridges to reconstruct handles from wire
    /// `(id, offset, len)` tuples — the host's `register` call already
    /// established the refcount-share that transfers in.
    pub fn from_parts(
        registry: Arc<dyn HostBufferRegistry>,
        id: u32,
        offset: u32,
        len: u32,
    ) -> Self {
        Self {
            storage: HandleStorage::Single(Segment { id, offset, len }),
            registry,
        }
    }

    /// Construct a handle from an ordered list of segments WITHOUT
    /// bumping any refcount. Caller has already established
    /// refcount-shares for every segment (e.g. via per-segment
    /// `sub_view` during decoder coalescing, or via per-segment
    /// `register` on the embedder side).
    ///
    /// Collapses to `Single` when `segments.len() == 1`. Panics on
    /// empty input — a zero-segment handle is not a valid view.
    pub fn from_segments_no_retain(
        registry: Arc<dyn HostBufferRegistry>,
        segments: Vec<Segment>,
    ) -> Self {
        assert!(
            !segments.is_empty(),
            "from_segments_no_retain requires at least one segment"
        );
        if segments.len() == 1 {
            return Self {
                storage: HandleStorage::Single(segments[0]),
                registry,
            };
        }
        let total_len = segments
            .iter()
            .map(|s| s.len)
            .try_fold(0u32, u32::checked_add)
            .expect("multi-segment handle total length overflows u32");
        Self {
            storage: HandleStorage::Multi {
                segments: segments.into_boxed_slice(),
                total_len,
            },
            registry,
        }
    }

    /// The segments making up this view, in order. Single-segment
    /// handles yield a one-element slice; multi-segment handles yield
    /// the underlying segment list. ABI-boundary code uses
    /// `segments().len()` to fork between the `Host` and `HostMulti`
    /// shapes.
    pub fn segments(&self) -> &[Segment] {
        match &self.storage {
            HandleStorage::Single(s) => std::slice::from_ref(s),
            HandleStorage::Multi { segments, .. } => segments,
        }
    }

    /// The registry this handle dispatches against. Used by the decoder
    /// to mint a fresh multi-segment handle without rederiving the
    /// registry reference.
    pub(crate) fn registry(&self) -> Arc<dyn HostBufferRegistry> {
        self.registry.clone()
    }

    /// Total byte length of this view across all its segments.
    pub fn len(&self) -> u32 {
        match &self.storage {
            HandleStorage::Single(s) => s.len,
            HandleStorage::Multi { total_len, .. } => *total_len,
        }
    }

    /// True if this view is zero-length.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Mint a sub-view at `[view_offset .. view_offset + len]` within
    /// the current view. Bumps the refcount for every segment the
    /// sub-view touches; the returned handle owns independent
    /// refcount-shares.
    pub(crate) fn sub_view(&self, view_offset: u32, len: u32) -> Self {
        debug_assert!(
            view_offset
                .checked_add(len)
                .map(|e| e <= self.len())
                .unwrap_or(false),
            "sub_view out of bounds: view_offset={} len={} self.len={}",
            view_offset,
            len,
            self.len(),
        );
        let mut out: Vec<Segment> = Vec::new();
        let mut remaining = len;
        let mut skip = view_offset;
        for seg in self.segments() {
            if remaining == 0 {
                break;
            }
            if skip >= seg.len {
                skip -= seg.len;
                continue;
            }
            let take = (seg.len - skip).min(remaining);
            self.registry.retain(seg.id);
            out.push(Segment {
                id: seg.id,
                offset: seg.offset + skip,
                len: take,
            });
            remaining -= take;
            skip = 0;
        }
        Self::from_segments_no_retain(self.registry.clone(), out)
    }

    /// Shrink this view from the left by `n` bytes. Releases any
    /// segments fully drained from the front. Does NOT touch the
    /// refcount for segments that are merely trimmed.
    pub(crate) fn advance(&mut self, n: u32) {
        debug_assert!(n <= self.len(), "advance past end of view");
        if n == 0 {
            return;
        }
        match &mut self.storage {
            HandleStorage::Single(s) => {
                s.offset += n;
                s.len -= n;
            }
            HandleStorage::Multi {
                segments,
                total_len,
            } => {
                let mut skip = n;
                let mut first_kept = 0usize;
                for (i, seg) in segments.iter_mut().enumerate() {
                    if skip == 0 {
                        break;
                    }
                    if skip >= seg.len {
                        self.registry.release(seg.id);
                        skip -= seg.len;
                        first_kept = i + 1;
                    } else {
                        seg.offset += skip;
                        seg.len -= skip;
                        skip = 0;
                    }
                }
                *total_len -= n;
                if first_kept == segments.len() {
                    // Whole view drained; storage stays Multi with an
                    // empty inner slice. The caller should drop us
                    // anyway — we're now empty.
                    debug_assert_eq!(*total_len, 0);
                    self.storage = HandleStorage::Multi {
                        segments: Vec::new().into_boxed_slice(),
                        total_len: 0,
                    };
                    return;
                }
                if first_kept > 0 {
                    let kept: Vec<Segment> = segments[first_kept..].to_vec();
                    let new_total = *total_len;
                    if kept.len() == 1 {
                        self.storage = HandleStorage::Single(kept[0]);
                    } else {
                        self.storage = HandleStorage::Multi {
                            segments: kept.into_boxed_slice(),
                            total_len: new_total,
                        };
                    }
                }
            }
        }
    }

    /// Read `len` bytes from `[view_offset ..]` of this view into
    /// `dst`. Dispatches per-segment through
    /// [`HostBufferRegistry::read_into`].
    pub fn read_into(&self, view_offset: usize, len: usize, dst: &mut [u8]) {
        assert_eq!(dst.len(), len, "dst.len() must equal len");
        let mut remaining = len;
        let mut skip = view_offset;
        let mut dst_off = 0;
        for seg in self.segments() {
            if remaining == 0 {
                break;
            }
            let seg_len = seg.len as usize;
            if skip >= seg_len {
                skip -= seg_len;
                continue;
            }
            let read_off = seg.offset as usize + skip;
            let take = (seg_len - skip).min(remaining);
            self.registry.read_into(
                seg.id,
                read_off as u32,
                take as u32,
                &mut dst[dst_off..dst_off + take],
            );
            dst_off += take;
            remaining -= take;
            skip = 0;
        }
    }

    /// True iff `self` and `other` reference byte-equal content.
    ///
    /// Fast paths:
    /// - Length mismatch — `false`.
    /// - Both single-segment with identical `(id, offset)` — `true`.
    /// - Both single-segment otherwise — dispatch through
    ///   [`HostBufferRegistry::eq`] for a direct registry compare.
    ///
    /// Slow path: at least one side multi-segment. Materialise both
    /// views via `read_into` into temp buffers and compare. The
    /// materialise allocation is the cost of being correct across a
    /// cross-segment boundary; sub-views from the decoder's coalesce
    /// path go this way only when the SDK does a replay equality
    /// against an unrelated host buffer.
    pub fn byte_eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        if let (HandleStorage::Single(a), HandleStorage::Single(b)) =
            (&self.storage, &other.storage)
        {
            if a.id == b.id && a.offset == b.offset {
                return true;
            }
            return self
                .registry
                .eq(a.id, a.offset, a.len, b.id, b.offset, b.len);
        }
        let len = self.len() as usize;
        if len == 0 {
            return true;
        }
        let mut a_buf = vec![0u8; len];
        let mut b_buf = vec![0u8; len];
        self.read_into(0, len, &mut a_buf);
        other.read_into(0, len, &mut b_buf);
        a_buf == b_buf
    }

    /// True iff this handle's bytes equal `other`. Materialises the
    /// host bytes via [`Self::read_into`] (which dispatches
    /// per-segment) and compares.
    pub fn byte_eq_inline(&self, other: &[u8]) -> bool {
        if self.len() as usize != other.len() {
            return false;
        }
        if other.is_empty() {
            return true;
        }
        let mut tmp = vec![0u8; other.len()];
        self.read_into(0, other.len(), &mut tmp);
        tmp == other
    }
}

impl Clone for HostBufferHandle {
    fn clone(&self) -> Self {
        for seg in self.segments() {
            self.registry.retain(seg.id);
        }
        Self {
            storage: self.storage.clone(),
            registry: self.registry.clone(),
        }
    }
}

impl Drop for HostBufferHandle {
    fn drop(&mut self) {
        for seg in self.segments() {
            self.registry.release(seg.id);
        }
    }
}

impl PartialEq for HostBufferHandle {
    /// Identity equality across all segments — same registry view
    /// shape, same `(id, offset, len)` per segment. Byte-equality uses
    /// [`Self::byte_eq`].
    fn eq(&self, other: &Self) -> bool {
        let a = self.segments();
        let b = other.segments();
        if a.len() != b.len() {
            return false;
        }
        a.iter()
            .zip(b.iter())
            .all(|(x, y)| x.id == y.id && x.offset == y.offset && x.len == y.len)
    }
}

impl Eq for HostBufferHandle {}

impl std::hash::Hash for HostBufferHandle {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for seg in self.segments() {
            seg.id.hash(state);
            seg.offset.hash(state);
            seg.len.hash(state);
        }
    }
}

impl std::fmt::Debug for HostBufferHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.storage {
            HandleStorage::Single(s) => f
                .debug_struct("HostBufferHandle")
                .field("id", &s.id)
                .field("offset", &s.offset)
                .field("len", &s.len)
                .finish(),
            HandleStorage::Multi {
                segments,
                total_len,
            } => f
                .debug_struct("HostBufferHandle")
                .field("segments", segments)
                .field("total_len", total_len)
                .finish(),
        }
    }
}

/// Registry of host-owned payload buffers.
///
/// Implementations live on the embedder side. Shared-core invokes the
/// registry only on payloads carried via the `Host` variant of the
/// [`Buffer`](super::Buffer) enum. All methods take raw
/// `(id, offset, len)` tuples — multi-segment handles dispatch through
/// the trait one segment at a time.
///
/// # Refcounting contract
///
/// - [`retain`](Self::retain) increments the refcount for `id`.
/// - [`release`](Self::release) decrements the refcount; when it
///   reaches zero the SDK MUST free the backing storage.
/// - `retain` / `release` look at `id` only — view `offset`/`len` are
///   not part of the refcounting key. Sub-views of the same id
///   contribute equally to the same refcount.
///
/// # Buffer immutability invariant
///
/// Bytes behind a registered buffer MUST be content-immutable for as
/// long as any refcount-share is outstanding.
pub trait HostBufferRegistry: Send + Sync {
    /// Byte-equality between two views (possibly into the same
    /// buffer). Implementations MUST honour each side's `offset`/`len`
    /// and compare those byte ranges. Implementations should
    /// short-circuit on length mismatch.
    fn eq(
        &self,
        a_id: u32,
        a_offset: u32,
        a_len: u32,
        b_id: u32,
        b_offset: u32,
        b_len: u32,
    ) -> bool;

    /// Copy `len` bytes from `[offset .. offset + len]` of the buffer
    /// `id` into `dst`. Caller guarantees `dst.len() == len`.
    fn read_into(&self, id: u32, offset: u32, len: u32, dst: &mut [u8]);

    /// Increment the refcount for `id`.
    fn retain(&self, id: u32);

    /// Decrement the refcount for `id`. When it reaches zero the SDK
    /// frees the underlying buffer.
    fn release(&self, id: u32);
}

/// A registry that panics on every call. The default for native embedders
/// that never construct `Host` buffer values. Invoking any method asserts
/// the invariant "shared-core never touches a registry when no host
/// buffers exist".
#[derive(Debug, Default)]
pub struct NopHostBufferRegistry;

impl HostBufferRegistry for NopHostBufferRegistry {
    fn eq(&self, _: u32, _: u32, _: u32, _: u32, _: u32, _: u32) -> bool {
        nop_registry_panic()
    }
    fn read_into(&self, _: u32, _: u32, _: u32, _: &mut [u8]) {
        nop_registry_panic()
    }
    fn retain(&self, _: u32) {
        nop_registry_panic()
    }
    fn release(&self, _: u32) {
        nop_registry_panic()
    }
}

#[inline(never)]
#[cold]
fn nop_registry_panic() -> ! {
    panic!(
        "host buffer registry was invoked but the embedder declared no host \
         buffer support; this is a shared-core or SDK bug"
    )
}

#[cfg(test)]
pub(crate) mod mocks {
    use super::*;

    impl HostBufferHandle {
        /// Underlying buffer id. Test-only introspection helper —
        /// production code reads `segments()` directly. Panics on
        /// multi-segment handles.
        pub fn id(&self) -> u32 {
            match &self.storage {
                HandleStorage::Single(s) => s.id,
                HandleStorage::Multi { .. } => {
                    panic!("HostBufferHandle::id() called on multi-segment handle")
                }
            }
        }

        /// Byte offset of this view's start within the underlying buffer.
        /// Panics on multi-segment handles (the concept does not apply).
        #[cfg(test)]
        pub fn offset(&self) -> u32 {
            match &self.storage {
                HandleStorage::Single(s) => s.offset,
                HandleStorage::Multi { .. } => {
                    panic!("HostBufferHandle::offset() called on multi-segment handle")
                }
            }
        }
    }

    /// In-memory test/dev-only host buffer registry. Backed by a `HashMap<u32,
    /// (Bytes, refcount)>`. Lets Rust unit tests exercise host-backed code
    /// paths without standing up a real WASM-bridged registry.
    #[cfg(test)]
    #[derive(Default)]
    pub struct InMemoryHostBufferRegistry {
        next_id: std::sync::atomic::AtomicU32,
        /// `id -> (bytes, refcount)`. Entries are inserted at `register` with
        /// refcount=1, mutated by `retain`/`release`, and removed when
        /// refcount drops to zero.
        buffers: std::sync::Mutex<std::collections::HashMap<u32, (Bytes, u32)>>,
    }

    #[cfg(test)]
    impl InMemoryHostBufferRegistry {
        pub fn new() -> Self {
            Self::default()
        }

        /// Number of distinct buffer ids currently alive (refcount > 0).
        /// Test introspection only.
        pub fn live_buffers(&self) -> usize {
            self.buffers.lock().unwrap().len()
        }

        /// Test-only: insert `bytes` into the registry with refcount = 1 and
        /// return a fresh handle wrapping the assigned id. Consumes one `Arc`
        /// share (use `registry.clone().make_handle(...)`).
        pub fn make_handle(self: Arc<Self>, bytes: &[u8]) -> HostBufferHandle {
            let id = self
                .next_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.buffers
                .lock()
                .unwrap()
                .insert(id, (Bytes::copy_from_slice(bytes), 1));
            let len = bytes.len() as u32;
            HostBufferHandle::from_parts(self as Arc<dyn HostBufferRegistry>, id, 0, len)
        }
    }

    #[cfg(test)]
    impl HostBufferRegistry for InMemoryHostBufferRegistry {
        fn eq(
            &self,
            a_id: u32,
            a_offset: u32,
            a_len: u32,
            b_id: u32,
            b_offset: u32,
            b_len: u32,
        ) -> bool {
            if a_len != b_len {
                return false;
            }
            let buffers = self.buffers.lock().unwrap();
            let (a_buf, _) = buffers.get(&a_id).expect("unknown host buffer id");
            let (b_buf, _) = buffers.get(&b_id).expect("unknown host buffer id");
            let a_range = a_offset as usize..(a_offset + a_len) as usize;
            let b_range = b_offset as usize..(b_offset + b_len) as usize;
            a_buf[a_range] == b_buf[b_range]
        }

        fn read_into(&self, id: u32, offset: u32, len: u32, dst: &mut [u8]) {
            assert_eq!(dst.len(), len as usize, "dst.len() must equal len");
            let buffers = self.buffers.lock().unwrap();
            let (buf, _) = buffers.get(&id).expect("unknown host buffer id");
            let start = offset as usize;
            let end = start + len as usize;
            dst.copy_from_slice(&buf[start..end]);
        }

        fn retain(&self, id: u32) {
            let mut buffers = self.buffers.lock().unwrap();
            let entry = buffers
                .get_mut(&id)
                .expect("retain on unknown host buffer id");
            entry.1 = entry
                .1
                .checked_add(1)
                .expect("host buffer refcount overflow");
        }

        fn release(&self, id: u32) {
            let mut buffers = self.buffers.lock().unwrap();
            let entry = buffers
                .get_mut(&id)
                .expect("release on unknown host buffer id (double-release?)");
            entry.1 = entry
                .1
                .checked_sub(1)
                .expect("host buffer refcount underflow");
            if entry.1 == 0 {
                buffers.remove(&id);
            }
        }
    }
}
