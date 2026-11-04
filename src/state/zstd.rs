//! zstd frames, with one compression and one decompression context per
//! thread: making a context costs several times what coding a 4 KiB page
//! with it does (36 µs against 5 µs to compress one against its previous
//! image, 19 µs against 1 µs to decompress it).
//!
//! A frame's dictionary is a raw prefix: content the frame refers back to,
//! such as the page a tail page or a patch is compressed against. (zstd's dictionary calls would read one
//! that starts with its dictionary magic as a trained dictionary.)
use std::{cell::RefCell, io};
use zstd_safe::{CCtx, CParameter, DCtx, ResetDirective};

/// The level of every frame: pages, frames, entries and manifests.
const LEVEL: i32 = 3;
/// A scratch buffer kept past a call: larger ones, of a large record, are
/// freed after it.
const KEPT_SCRATCH: usize = 64 * 1024;

thread_local! {
    static COMPRESS: RefCell<CCtx<'static>> = RefCell::new(CCtx::create());
    static DECOMPRESS: RefCell<DCtx<'static>> = RefCell::new(DCtx::create());
    /// Frames are compressed here, then copied out at their size: the
    /// worst-case bound a frame needs room for is far above what it takes.
    static SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

fn error(code: zstd_safe::ErrorCode) -> io::Error {
    io::Error::other(zstd_safe::get_error_name(code))
}

/// Extend `prefix`'s lifetime to a context's: the callers below reset the
/// context before they return, which drops the prefix.
fn referenced(prefix: &[u8]) -> &'static [u8] {
    // SAFETY: the context references the prefix only until its reset, which
    // every path through `compress` and `decompress` reaches, and nothing
    // reads the prefix after it.
    unsafe { std::mem::transmute::<&[u8], &'static [u8]>(prefix) }
}

/// `input` as one frame against `prefix` (empty for none), with a
/// checksum: `f` gets the frame.
pub(crate) fn compress<T>(
    input: &[u8],
    prefix: &[u8],
    f: impl FnOnce(&[u8]) -> T,
) -> io::Result<T> {
    COMPRESS.with_borrow_mut(|cctx| {
        SCRATCH.with_borrow_mut(|frame| {
            frame.clear();
            frame.reserve(zstd_safe::compress_bound(input.len()));
            let compressed = (cctx.set_parameter(CParameter::CompressionLevel(LEVEL)))
                .and_then(|_| cctx.set_parameter(CParameter::ChecksumFlag(true)))
                .and_then(|_| cctx.ref_prefix(referenced(prefix)))
                .and_then(|_| cctx.compress2(frame, input));
            if cctx.reset(ResetDirective::SessionAndParameters).is_err() {
                *cctx = CCtx::create();
            }
            compressed.map_err(error)?;
            let value = f(frame);
            if frame.capacity() > KEPT_SCRATCH {
                *frame = Vec::new();
            }
            Ok(value)
        })
    })
}

/// Decompress the frame `input`, made against `prefix`, into `out`, which
/// holds as many bytes as its capacity allows: a larger frame fails.
pub(crate) fn decompress(out: &mut Vec<u8>, input: &[u8], prefix: &[u8]) -> io::Result<()> {
    DECOMPRESS.with_borrow_mut(|dctx| {
        let decompressed =
            (dctx.ref_prefix(referenced(prefix))).and_then(|_| dctx.decompress(out, input));
        if dctx.reset(ResetDirective::SessionAndParameters).is_err() {
            *dctx = DCtx::create();
        }
        decompressed.map(drop).map_err(error)
    })
}

/// Decompress the frame `input`, made against `prefix`, to the size its
/// header gives, of at most `max` bytes.
pub(crate) fn decompress_sized(input: &[u8], prefix: &[u8], max: usize) -> io::Result<Vec<u8>> {
    let size = zstd_safe::get_frame_content_size(input)
        .ok()
        .flatten()
        .filter(|&size| size <= max as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid frame size"))?;
    let mut out = Vec::with_capacity(size as usize);
    decompress(&mut out, input, prefix)?;
    match out.len() as u64 == size {
        true => Ok(out),
        false => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid frame size",
        )),
    }
}
