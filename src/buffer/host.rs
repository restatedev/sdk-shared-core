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

/// Refcounted handle to a byte buffer (or sub-range thereof) owned by the
/// embedding host.
///
/// The handle is non-`Copy`. `Clone` bumps the SDK-side refcount via
/// [`HostBufferRegistry::retain`]; `Drop` decrements via
/// [`HostBufferRegistry::release`]. The SDK frees the underlying buffer
/// when its refcount reaches zero.
///
/// # View semantics
///
/// A handle is a *view* into the underlying buffer: bytes
/// `[offset .. offset + len]`. Two clones share the same id but each
/// carries an independent refcount-share.
///
/// [`HostBufferHandle::sub_view`] mints a fresh view at a sub-range of
/// `self`'s window, bumping the refcount. [`HostBufferHandle::advance`]
/// shrinks the window from the left without touching the refcount —
/// used by streaming decoders that consume bytes incrementally.
///
/// # Buffer immutability invariant
///
/// Bytes behind a handle MUST be content-immutable for as long as any
/// handle to them lives — length, byte contents, and identity all stable
/// until the last refcount-share is released.
///
/// # WASM ABI
///
/// The WASM ABI MUST round-trip `id`, `offset`, and `len`. Construction
/// across the boundary uses [`HostBufferHandle::from_parts`] (which does
/// NOT call `retain` — the caller is responsible for the initial
/// refcount having been established).
pub struct HostBufferHandle {
    id: u32,
    offset: u32,
    len: u32,
    registry: Arc<dyn HostBufferRegistry>,
}

impl HostBufferHandle {
    /// Construct a handle from raw `(id, offset, len)` and a registry
    /// reference WITHOUT bumping the refcount. The caller is responsible
    /// for having already established (or transferred) a refcount-share.
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
            id,
            offset,
            len,
            registry,
        }
    }

    /// Underlying buffer id. Sub-views of the same buffer share an id.
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Byte offset of this view's start within the underlying buffer.
    pub fn offset(&self) -> u32 {
        self.offset
    }

    /// Byte length of this view.
    pub fn len(&self) -> u32 {
        self.len
    }

    /// True if this view is zero-length.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Mint a sub-view at `[self.offset + view_offset .. self.offset +
    /// view_offset + len]` within the underlying buffer. Bumps the
    /// refcount; the returned sub-view owns an independent share.
    pub(crate) fn sub_view(&self, view_offset: u32, len: u32) -> Self {
        debug_assert!(
            view_offset
                .checked_add(len)
                .map(|e| e <= self.len)
                .unwrap_or(false),
            "sub_view out of bounds: view_offset={} len={} self.len={}",
            view_offset,
            len,
            self.len,
        );
        self.registry.retain(self);
        Self {
            id: self.id,
            offset: self.offset + view_offset,
            len,
            registry: self.registry.clone(),
        }
    }

    /// Shrink this view from the left by `n` bytes (offset += n, len -= n).
    /// Does NOT touch the refcount — the handle still owns its share of
    /// the underlying buffer; the window just got smaller.
    pub(crate) fn advance(&mut self, n: u32) {
        debug_assert!(n <= self.len, "advance past end of view");
        self.offset += n;
        self.len -= n;
    }

    /// Read `len` bytes from `[self.offset + view_offset ..]` of the
    /// underlying buffer into `dst`. Convenience wrapper around
    /// [`HostBufferRegistry::read_into`].
    pub fn read_into(&self, view_offset: usize, len: usize, dst: &mut [u8]) {
        self.registry.read_into(self, view_offset, len, dst);
    }

    /// True iff `self` and `other` reference the same bytes. Fast-path
    /// short-circuit on view identity (same id + offset + len); otherwise
    /// dispatches through [`HostBufferRegistry::eq`] for a byte
    /// comparison — required by replay equality when a wire-decoded
    /// expected payload comes back as a host sub-view but the actual
    /// payload was produced by a different registration.
    pub fn byte_eq(&self, other: &Self) -> bool {
        if self.len != other.len {
            return false;
        }
        // Same view: same id and offset (length is already equal).
        if self.id == other.id && self.offset == other.offset {
            return true;
        }
        self.registry.eq(self, other)
    }

    /// True iff this handle's bytes equal `other`. Materialises the
    /// host bytes via [`HostBufferRegistry::read_into`] — the slow path
    /// used when comparing a `Buffer::Host` to a `Buffer::InMemory` for
    /// replay equality.
    pub fn byte_eq_inline(&self, other: &[u8]) -> bool {
        if self.len as usize != other.len() {
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
        self.registry.retain(self);
        Self {
            id: self.id,
            offset: self.offset,
            len: self.len,
            registry: self.registry.clone(),
        }
    }
}

impl Drop for HostBufferHandle {
    fn drop(&mut self) {
        self.registry.release(self);
    }
}

impl PartialEq for HostBufferHandle {
    fn eq(&self, other: &Self) -> bool {
        // Compare by view identity only — registry Arc is implementation
        // detail.
        self.id == other.id && self.offset == other.offset && self.len == other.len
    }
}

impl Eq for HostBufferHandle {}

impl std::hash::Hash for HostBufferHandle {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.offset.hash(state);
        self.len.hash(state);
    }
}

impl std::fmt::Debug for HostBufferHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostBufferHandle")
            .field("id", &self.id)
            .field("offset", &self.offset)
            .field("len", &self.len)
            .finish()
    }
}

/// Registry of host-owned payload buffers.
///
/// Implementations live on the embedder side. Shared-core invokes the
/// registry only on payloads carried via the `Host` variant of the
/// [`Buffer`](super::Buffer) enum.
///
/// # Refcounting contract
///
/// - [`register`](Self::register) stores `bytes`, returns a fresh id, and
///   establishes refcount = 1.
/// - [`retain`](Self::retain) increments the refcount for `handle.id()`.
/// - [`release`](Self::release) decrements the refcount; when it reaches
///   zero the SDK MUST free the backing storage.
/// - `retain` / `release` look at `handle.id()` only — the view
///   `offset`/`len` are not part of the refcounting key. Sub-views of the
///   same id contribute equally to the same refcount.
///
/// # Buffer immutability invariant
///
/// Bytes behind a registered buffer MUST be content-immutable for as
/// long as any refcount-share is outstanding.
pub trait HostBufferRegistry: Send + Sync {
    /// Byte-equality between two views (possibly into the same buffer).
    /// Implementations MUST honour each handle's `offset`/`len` and
    /// compare those byte ranges. Implementations should short-circuit
    /// on length mismatch.
    fn eq(&self, a: &HostBufferHandle, b: &HostBufferHandle) -> bool;

    /// Copy a range of the host buffer into `dst`. The range is
    /// `[handle.offset() + view_offset .. handle.offset() + view_offset + len]`
    /// within the underlying buffer identified by `handle.id()`. Caller
    /// guarantees `dst.len() == len` and `view_offset + len <= handle.len()`.
    fn read_into(&self, handle: &HostBufferHandle, view_offset: usize, len: usize, dst: &mut [u8]);

    /// Increment the refcount for `handle.id()`.
    fn retain(&self, handle: &HostBufferHandle);

    /// Decrement the refcount for `handle.id()`. When it reaches zero the
    /// SDK frees the underlying buffer.
    fn release(&self, handle: &HostBufferHandle);
}

/// A registry that panics on every call. The default for native embedders
/// that never construct `Host` buffer values. Invoking any method asserts
/// the invariant "shared-core never touches a registry when no host
/// buffers exist".
#[derive(Debug, Default)]
pub struct NopHostBufferRegistry;

impl HostBufferRegistry for NopHostBufferRegistry {
    fn eq(&self, _a: &HostBufferHandle, _b: &HostBufferHandle) -> bool {
        nop_registry_panic()
    }
    fn read_into(
        &self,
        _handle: &HostBufferHandle,
        _view_offset: usize,
        _len: usize,
        _dst: &mut [u8],
    ) {
        nop_registry_panic()
    }
    fn retain(&self, _handle: &HostBufferHandle) {
        nop_registry_panic()
    }
    fn release(&self, _handle: &HostBufferHandle) {
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
    fn eq(&self, a: &HostBufferHandle, b: &HostBufferHandle) -> bool {
        if a.len() != b.len() {
            return false;
        }
        let buffers = self.buffers.lock().unwrap();
        let (a_buf, _) = buffers.get(&a.id()).expect("unknown host buffer handle");
        let (b_buf, _) = buffers.get(&b.id()).expect("unknown host buffer handle");
        let a_range = a.offset() as usize..(a.offset() + a.len()) as usize;
        let b_range = b.offset() as usize..(b.offset() + b.len()) as usize;
        a_buf[a_range] == b_buf[b_range]
    }

    fn read_into(&self, handle: &HostBufferHandle, view_offset: usize, len: usize, dst: &mut [u8]) {
        assert_eq!(dst.len(), len, "dst.len() must equal len");
        let buffers = self.buffers.lock().unwrap();
        let (buf, _) = buffers
            .get(&handle.id())
            .expect("unknown host buffer handle");
        let start = handle.offset() as usize + view_offset;
        dst.copy_from_slice(&buf[start..start + len]);
    }

    fn retain(&self, handle: &HostBufferHandle) {
        let mut buffers = self.buffers.lock().unwrap();
        let entry = buffers
            .get_mut(&handle.id())
            .expect("retain on unknown host buffer handle");
        entry.1 = entry
            .1
            .checked_add(1)
            .expect("host buffer refcount overflow");
    }

    fn release(&self, handle: &HostBufferHandle) {
        let mut buffers = self.buffers.lock().unwrap();
        let entry = buffers
            .get_mut(&handle.id())
            .expect("release on unknown host buffer handle (double-release?)");
        entry.1 = entry
            .1
            .checked_sub(1)
            .expect("host buffer refcount underflow");
        if entry.1 == 0 {
            buffers.remove(&handle.id());
        }
    }
}
