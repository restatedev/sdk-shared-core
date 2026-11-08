//! What is stored, and how it is encoded: the head, the tail entries and
//! the checkpoints' manifests in Restate, and the checkpoints' packs.
//!
//! | Store   | Key                                            | Holds                                                                               |
//! | ------- | ---------------------------------------------- | ----------------------------------------------------------------------------------- |
//! | Restate | `v22/head`                                     | The commit point: the version, the checkpoint and the one before, the tail's extent |
//! | Restate | `v22/tail/<version>-<nonce>/<part>`            | The mutation the flush of `version` made ([`Entry`]), in parts of up to 1 MiB       |
//! | Restate | `v22/manifest/<version>-<nonce>/<part>`        | A checkpoint's [`Manifest`], in parts of 64 KiB                                     |
//! | Restate | `v22/pack/l0/<version>-<nonce>/<part>`         | A pack in L0: a checkpoint's frames and values, in parts of 64 KiB                  |
//! | S3      | `l1/<version>-<nonce>`                         | A pack in L1: what a checkpoint moved out of L0, with its own frames and values     |
//!
//! A file is a row of *frames* of [`crate::state::StateOptions::frame_pages`]
//! pages, each a zstd frame of its own, so that a page is read with one
//! ranged get of its frame. A checkpoint's manifest says where each frame
//! and large record is: in its own pack, or in an older one. It is in Restate
//! wherever the packs are, so that an open reads it with the tail.
//!
//! Every key but the head's names one value for ever: a pack, a manifest and
//! a tail entry each carry a random nonce in their names, so that what a
//! flush that is lost wrote is never overwritten by the next writer's, but
//! left for garbage collection. A tail entry's nonce is its writer's, which
//! the head records ([`Run`]); a writer puts at most one entry at a version
//! under a nonce. Heads written before tail
//! entries had nonces (`SH22`) name theirs `v22/tail/<version>/<part>`, which
//! a retry overwrites; they are read as a run without a nonce.
use crate::state::{zstd, PAGE};
use bytes::Bytes;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    io,
    ops::Range,
};

/// The head's key: the commit point. An open reads it first, so a host whose
/// store can send keys ahead of the first read (Restate's eager state)
/// should send this one.
pub(crate) const HEAD_KEY: &str = "v22/head";
pub(crate) const TAIL_PREFIX: &str = "v22/tail/";
/// Where L1's packs are, in their store.
pub(crate) const PACK_PREFIX: &str = "l1/";
/// Where L0's packs are, in Restate ([`crate::state::blobs::KvBlobs`]).
pub(crate) const L0_PREFIX: &str = "l0/";
const HEAD_MAGIC: &[u8; 4] = b"SH23";
/// A head written before tail entries had nonces: read, never written.
const LEGACY_HEAD_MAGIC: &[u8; 4] = b"SH22";
const ENTRY_MAGIC: &[u8; 4] = b"SE22";
const MANIFEST_MAGIC: &[u8; 4] = b"SM22";
/// Bytes of a tail entry's part.
pub(crate) const PART_BYTES: usize = 1 << 20;

/// Bytes of a record's value.
pub(crate) const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;
/// Bytes of a record's or file's name.
pub(crate) const MAX_NAME_BYTES: usize = 255;
/// Bytes of a paged file: 10 GiB.
pub(crate) const MAX_FILE_BYTES: u64 = 10 << 30;
/// Most pages a frame holds: 1 MiB.
pub(crate) const MAX_FRAME_PAGES: u64 = 256;
/// Files and records a state may name.
pub(crate) const MAX_NAMES: usize = 1 << 16;
/// Largest version, far from overflow.
const MAX_VERSION: u64 = 1 << 62;
/// Largest decoded tail entry or manifest.
pub(crate) const MAX_DECODED: usize = 1 << 30;

fn too_large(what: &str, bytes: usize) -> io::Error {
    io::Error::other(format!("{what} of {bytes} bytes exceeds {MAX_DECODED}"))
}

/// The key of part `part` of the tail entry of `version`, flushed with
/// `nonce`: `v22/tail/<version>-<nonce>/<part>`, or, for a nonce of 0, an
/// entry written before nonces, `v22/tail/<version>/<part>`.
pub(crate) fn tail_key(version: u64, nonce: u64, part: u32) -> String {
    match nonce {
        0 => format!("{TAIL_PREFIX}{version}/{part}"),
        nonce => format!("{TAIL_PREFIX}{version}-{nonce:016x}/{part}"),
    }
}

/// What a tail entry's key names (see [`tail_key`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TailKey {
    pub version: u64,
    /// 0 for an entry written before nonces, whose key a retry reuses.
    pub nonce: u64,
    pub part: u32,
}

/// What the tail entry's key `key` names.
pub(crate) fn parse_tail_key(key: &str) -> Option<TailKey> {
    let (name, part) = key.strip_prefix(TAIL_PREFIX)?.split_once('/')?;
    let (version, nonce) = match name.split_once('-') {
        Some((version, nonce)) => (
            version,
            u64::from_str_radix(nonce, 16).ok().filter(|&n| n != 0)?,
        ),
        None => (name, 0),
    };
    Some(TailKey {
        version: version.parse().ok()?,
        nonce,
        part: part.parse().ok()?,
    })
}

/// Whether `key` names one value for ever: any key but the head's, and the
/// tail entries' written before nonces (see the module documentation).
pub(crate) fn immutable(key: &str) -> bool {
    key != HEAD_KEY && parse_tail_key(key).is_none_or(|tail| tail.nonce != 0)
}

pub(crate) fn put_varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        out.push(n as u8 | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

pub(crate) fn invalid(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("invalid {what}"))
}

/// Reads varints and byte strings, failing on any overrun.
pub(crate) struct Reading<'a> {
    bytes: &'a [u8],
    at: usize,
    /// What it reads, for errors.
    what: &'static str,
}

impl<'a> Reading<'a> {
    pub(super) fn new(bytes: &'a [u8], what: &'static str) -> Self {
        Self { bytes, at: 0, what }
    }

    pub(super) fn invalid(&self) -> io::Error {
        invalid(self.what)
    }

    pub(super) fn varint(&mut self) -> io::Result<u64> {
        let mut n = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = *self.bytes.get(self.at).ok_or_else(|| self.invalid())?;
            self.at += 1;
            n |= u64::from(byte & 0x7f)
                .checked_shl(shift)
                .ok_or_else(|| self.invalid())?;
            if byte & 0x80 == 0 {
                return match shift < 63 || byte <= 1 {
                    true => Ok(n),
                    false => Err(self.invalid()),
                };
            }
        }
        Err(self.invalid())
    }

    /// Eight bytes, little-endian.
    pub(super) fn fixed_u64(&mut self) -> io::Result<u64> {
        let end = self.at + 8;
        let bytes = self.bytes.get(self.at..end).ok_or_else(|| self.invalid())?;
        self.at = end;
        Ok(u64::from_le_bytes(bytes.try_into().expect("eight bytes")))
    }

    /// A varint of at most `max`.
    pub(super) fn bounded(&mut self, max: u64) -> io::Result<u64> {
        Some(self.varint()?)
            .filter(|&n| n <= max)
            .ok_or_else(|| self.invalid())
    }

    /// A count of items of at least a byte each.
    fn count(&mut self) -> io::Result<usize> {
        let left = (self.bytes.len() - self.at) as u64;
        Ok(self.bounded(left)? as usize)
    }

    pub(super) fn bytes(&mut self) -> io::Result<&'a [u8]> {
        let len = self.varint()? as usize;
        let end = (self.at.checked_add(len))
            .filter(|&end| end <= self.bytes.len())
            .ok_or_else(|| self.invalid())?;
        let bytes = &self.bytes[self.at..end];
        self.at = end;
        Ok(bytes)
    }

    fn name(&mut self) -> io::Result<String> {
        let name = self.bytes()?;
        if name.len() > MAX_NAME_BYTES {
            return Err(self.invalid());
        }
        String::from_utf8(name.to_vec()).map_err(|_| self.invalid())
    }

    /// Fail unless every byte was read.
    pub(super) fn end(&self) -> io::Result<()> {
        match self.at == self.bytes.len() {
            true => Ok(()),
            false => Err(self.invalid()),
        }
    }
}

/// Where a pack is kept: L0, in Restate, or L1, in S3.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Tier {
    L0,
    L1,
}

impl Tier {
    fn prefix(self) -> &'static str {
        match self {
            Self::L0 => L0_PREFIX,
            Self::L1 => PACK_PREFIX,
        }
    }

    fn decode(n: u64) -> Option<Self> {
        match n {
            0 => Some(Self::L0),
            1 => Some(Self::L1),
            _ => None,
        }
    }
}

/// A pack's name: the version it checkpoints, a random nonce, and its tier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PackId {
    pub version: u64,
    pub nonce: u64,
    pub tier: Tier,
}

impl PackId {
    /// Its name: `<version>-<nonce>`, its manifest's.
    pub(crate) fn name(self) -> String {
        format!("{:016x}-{:016x}", self.version, self.nonce)
    }

    /// Its pack's key, in its tier's store: `l0/<name>` or `l1/<name>`.
    pub(crate) fn key(self) -> String {
        format!("{}{}", self.tier.prefix(), self.name())
    }

    /// The pack id named `name`, in `tier`.
    pub(crate) fn parse_name(name: &str, tier: Tier) -> Option<Self> {
        let (version, nonce) = name.split_once('-')?;
        Some(Self {
            version: u64::from_str_radix(version, 16).ok()?,
            nonce: u64::from_str_radix(nonce, 16).ok()?,
            tier,
        })
    }

    /// The version a manifest named `name` (`<version>-<nonce>`) holds.
    pub(crate) fn manifest_version(name: &str) -> Option<u64> {
        u64::from_str_radix(name.split_once('-')?.0, 16).ok()
    }

    /// The pack id whose pack's key is `key`.
    pub(crate) fn parse(key: &str) -> Option<Self> {
        [Tier::L0, Tier::L1]
            .into_iter()
            .find_map(|tier| Self::parse_name(key.strip_prefix(tier.prefix())?, tier))
    }
}

/// A checkpoint, as a head names it: its pack, and the bytes of its
/// manifest, which is in Restate under the pack's name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Checkpoint {
    pub pack: PackId,
    pub manifest: u64,
}

impl Checkpoint {
    /// The version whose state it holds.
    pub(crate) fn version(&self) -> u64 {
        self.pack.version
    }

    fn encode(this: Option<&Self>, out: &mut Vec<u8>) {
        let Some(this) = this else {
            return out.push(0);
        };
        out.push(1);
        for n in [
            this.pack.version,
            this.pack.nonce,
            this.pack.tier as u64,
            this.manifest,
        ] {
            put_varint(out, n);
        }
    }

    fn decode(r: &mut Reading) -> io::Result<Option<Self>> {
        match r.varint()? {
            0 => Ok(None),
            1 => Ok(Some(Self {
                pack: PackId {
                    version: r.varint()?,
                    nonce: r.varint()?,
                    tier: Tier::decode(r.varint()?).ok_or_else(|| r.invalid())?,
                },
                // Its compressed body, of at most what a decode takes.
                manifest: r.bounded(MAX_DECODED as u64)?,
            })),
            _ => Err(r.invalid()),
        }
    }
}

/// The tail entries from version `first` on (to the next run's first) were
/// written with one random `nonce`, which their keys carry; 0 for entries
/// written before nonces (see [`tail_key`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Run {
    pub first: u64,
    pub nonce: u64,
}

/// The commit point.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Head {
    /// The database's identity, drawn when it was made: a namespace emptied
    /// and made again has another, so that no cache takes one for the other.
    pub id: u64,
    /// Pages per frame, set when the database was made.
    pub frame_pages: u64,
    /// The flushes that changed something, ever.
    pub version: u64,
    pub checkpoint: Option<Checkpoint>,
    /// The checkpoint before, which readers that opened on it may still read.
    pub previous: Option<Checkpoint>,
    /// The first version whose tail entry is kept.
    pub tail_first: u64,
    /// The parts of each tail entry kept, from `tail_first` to `version`.
    pub parts: Vec<u32>,
    /// The nonces of the tail entries kept, by run: the first from
    /// `tail_first`, each from a later version; none if no entry is kept.
    pub runs: Vec<Run>,
    /// Bytes of the tail entries since the checkpoint.
    pub tail_bytes: u64,
}

impl Head {
    /// The head of an empty database.
    pub(crate) fn new(id: u64, frame_pages: u64) -> Self {
        Self {
            id,
            frame_pages,
            version: 0,
            checkpoint: None,
            previous: None,
            tail_first: 1,
            parts: Vec::new(),
            runs: Vec::new(),
            tail_bytes: 0,
        }
    }

    pub(crate) fn frame_bytes(&self) -> u64 {
        self.frame_pages * PAGE
    }

    /// The version of the head's checkpoint; 0 without one.
    pub(crate) fn checkpointed(&self) -> u64 {
        self.checkpoint.map_or(0, |c| c.version())
    }

    pub(crate) fn encode(&self) -> Bytes {
        let mut out = HEAD_MAGIC.to_vec();
        out.extend_from_slice(&self.id.to_le_bytes());
        put_varint(&mut out, self.frame_pages);
        put_varint(&mut out, self.version);
        Checkpoint::encode(self.checkpoint.as_ref(), &mut out);
        Checkpoint::encode(self.previous.as_ref(), &mut out);
        put_varint(&mut out, self.tail_first);
        put_varint(&mut out, self.tail_bytes);
        for &parts in &self.parts {
            put_varint(&mut out, parts.into());
        }
        put_varint(&mut out, self.runs.len() as u64);
        for run in &self.runs {
            put_varint(&mut out, run.first);
            out.extend_from_slice(&run.nonce.to_le_bytes());
        }
        out.into()
    }

    pub(crate) fn decode(bytes: &[u8]) -> io::Result<Self> {
        let (legacy, body) = match bytes.strip_prefix(HEAD_MAGIC) {
            Some(body) => (false, body),
            None => (
                true,
                (bytes.strip_prefix(LEGACY_HEAD_MAGIC)).ok_or_else(|| invalid("head"))?,
            ),
        };
        let (id, body) = body.split_first_chunk().ok_or_else(|| invalid("head"))?;
        let mut r = Reading::new(body, "head");
        let frame_pages = r.bounded(MAX_FRAME_PAGES)?;
        let version = r.bounded(MAX_VERSION)?;
        let checkpoint = Checkpoint::decode(&mut r)?;
        let previous = Checkpoint::decode(&mut r)?;
        let tail_first = r.varint()?;
        let tail_bytes = r.varint()?;
        let count = (version + 1)
            .checked_sub(tail_first)
            .filter(|&count| count <= body.len() as u64)
            .ok_or_else(|| r.invalid())?;
        let parts = (0..count)
            .map(|_| u32::try_from(r.varint()?).map_err(|_| r.invalid()))
            .collect::<io::Result<_>>()?;
        let runs = match legacy {
            // One run, of entries without a nonce.
            true => match count {
                0 => Vec::new(),
                _ => vec![Run {
                    first: tail_first,
                    nonce: 0,
                }],
            },
            false => {
                let runs = r.bounded(count)?;
                (0..runs)
                    .map(|_| {
                        let first = r.varint()?;
                        let nonce = r.fixed_u64()?;
                        Ok(Run { first, nonce })
                    })
                    .collect::<io::Result<Vec<Run>>>()?
            }
        };
        r.end()?;
        // The runs cover the tail kept, from its first entry, in order.
        let covered = match runs.first() {
            None => count == 0,
            Some(run) => {
                run.first == tail_first
                    && runs.windows(2).all(|pair| pair[0].first < pair[1].first)
                    && runs.last().is_some_and(|run| run.first <= version)
            }
        };
        let at = |c: Option<Checkpoint>| c.map_or(0, |c| c.version());
        let ordered = at(previous) <= at(checkpoint) && at(checkpoint) <= version;
        let kept = previous.is_none() || checkpoint.is_some();
        // The tail kept starts past the previous checkpoint, which covers
        // what went before it.
        let tail = tail_first == at(previous) + 1;
        if frame_pages == 0 || version == 0 || !ordered || !kept || !tail || !covered {
            return Err(invalid("head"));
        }
        Ok(Self {
            id: u64::from_le_bytes(*id),
            frame_pages,
            version,
            checkpoint,
            previous,
            tail_first,
            parts,
            runs,
            tail_bytes,
        })
    }

    /// The parts of version `version`'s tail entry.
    pub(crate) fn parts_of(&self, version: u64) -> u32 {
        (version.checked_sub(self.tail_first))
            .and_then(|at| self.parts.get(at as usize))
            .copied()
            .unwrap_or(0)
    }

    /// The nonce of version `version`'s tail entry, if the head keeps it (0
    /// for an entry written before nonces).
    pub(crate) fn nonce_of(&self, version: u64) -> Option<u64> {
        if version < self.tail_first || version > self.version {
            return None;
        }
        (self.runs.iter().rev())
            .find(|run| run.first <= version)
            .map(|run| run.nonce)
    }

    /// The keys of the parts of version `version`'s tail entry.
    pub(crate) fn entry_keys(&self, version: u64) -> Vec<String> {
        let Some(nonce) = self.nonce_of(version) else {
            return Vec::new();
        };
        (0..self.parts_of(version))
            .map(|part| tail_key(version, nonce, part))
            .collect()
    }

    /// The head's own tail entry, at its version, was written with `nonce`:
    /// a run of its own, unless the entry before it has that nonce too.
    pub(crate) fn written_by(&mut self, nonce: u64) {
        if self.runs.last().map(|run| run.nonce) != Some(nonce) {
            self.runs.push(Run {
                first: self.version,
                nonce,
            });
        }
    }

    /// Drop the runs of entries before `tail_first`, which no longer kept:
    /// the first run kept starts there.
    pub(crate) fn trim_runs(&mut self) {
        if self.tail_first > self.version {
            self.runs.clear();
            return;
        }
        let first = (self.runs.iter())
            .rposition(|run| run.first <= self.tail_first)
            .unwrap_or(0);
        self.runs.drain(..first);
        if let Some(run) = self.runs.first_mut() {
            run.first = self.tail_first;
        }
    }
}

/// What a flush changed in a paged file: its size after, and the pages it
/// wrote, each a zstd frame against the page
/// before it (see FORMAT.md), or `None` for a hole.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct FileChange {
    pub size: u64,
    pub pages: Vec<(u64, Option<Bytes>)>,
}

/// A tail entry: what one flush changed.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Entry {
    pub records: Vec<(String, Bytes)>,
    pub files: Vec<(String, FileChange)>,
}

impl Entry {
    /// The entry, in parts of at most [`PART_BYTES`]; an error past the
    /// 1 GiB a decode takes.
    pub(super) fn encode(&self) -> io::Result<Vec<Bytes>> {
        let mut out = ENTRY_MAGIC.to_vec();
        put_varint(&mut out, self.records.len() as u64);
        for (name, value) in &self.records {
            put_bytes(&mut out, name.as_bytes());
            put_bytes(&mut out, value);
        }
        put_varint(&mut out, self.files.len() as u64);
        for (name, file) in &self.files {
            put_bytes(&mut out, name.as_bytes());
            put_varint(&mut out, file.size / 512);
            put_varint(&mut out, file.pages.len() as u64);
            let mut next = 0;
            for (page, frame) in &file.pages {
                put_varint(&mut out, page - next);
                next = page + 1;
                // A zstd frame is never empty: empty is a hole.
                put_bytes(&mut out, frame.as_deref().unwrap_or_default());
            }
        }
        if out.len() > MAX_DECODED {
            return Err(too_large("a tail entry", out.len()));
        }
        let out = Bytes::from(out);
        Ok((0..out.len())
            .step_by(PART_BYTES)
            .map(|at| out.slice(at..(at + PART_BYTES).min(out.len())))
            .collect())
    }

    /// The entry of `parts`, in order.
    pub(super) fn decode(parts: &[Bytes]) -> io::Result<Self> {
        let joined;
        let bytes = match parts {
            [one] => one,
            _ => {
                joined = Bytes::from(parts.concat());
                &joined
            }
        };
        if bytes.len() > MAX_DECODED {
            return Err(invalid("tail entry"));
        }
        let body = (bytes.strip_prefix(ENTRY_MAGIC)).ok_or_else(|| invalid("tail entry"))?;
        let mut r = Reading::new(body, "tail entry");
        let value = |r: &mut Reading| -> io::Result<Bytes> {
            let value = r.bytes()?;
            match value.len() <= MAX_RECORD_BYTES {
                true => Ok(bytes.slice_ref(value)),
                false => Err(r.invalid()),
            }
        };
        let mut entry = Entry::default();
        for _ in 0..r.count()? {
            let name = r.name()?;
            entry.records.push((name, value(&mut r)?));
        }
        for _ in 0..r.count()? {
            let name = r.name()?;
            let size = r.bounded(MAX_FILE_BYTES / 512)? * 512;
            let mut pages = Vec::new();
            let mut next = 0u64;
            for _ in 0..r.count()? {
                let page = next
                    .checked_add(r.varint()?)
                    .filter(|&page| page < MAX_FILE_BYTES / PAGE)
                    .ok_or_else(|| r.invalid())?;
                next = page + 1;
                let frame = r.bytes()?;
                let frame = (!frame.is_empty()).then(|| bytes.slice_ref(frame));
                pages.push((page, frame));
            }
            entry.files.push((name, FileChange { size, pages }));
        }
        r.end()?;
        Ok(entry)
    }
}

/// Where something a checkpoint stored is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct Location {
    pub pack: PackId,
    pub offset: u64,
    pub len: u64,
}

impl Location {
    pub(crate) fn range(&self) -> Range<u64> {
        self.offset..self.offset + self.len
    }
}

/// A record's value as a manifest keeps it: in the manifest,
/// or a zstd frame of its own in a pack.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Value {
    Inline(Bytes),
    Stored(Location),
}

/// A paged file in a checkpoint: its size, where each of its frames is
/// (`None`: zeros), and its patches: pages
/// changed since their frame was written, each a zstd frame against the
/// page as its frame holds it, by page.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct FileIndex {
    pub size: u64,
    pub frames: Vec<Option<Location>>,
    pub patches: BTreeMap<u64, Location>,
}

/// A checkpoint's state: every file and record, and where what it
/// stored is; and the bytes each pack it names stored, for compaction.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Manifest {
    pub files: BTreeMap<String, FileIndex>,
    pub records: BTreeMap<String, Value>,
    pub sizes: BTreeMap<PackId, u64>,
}

impl Manifest {
    /// Every location it names.
    pub(crate) fn locations(&self) -> impl Iterator<Item = &Location> {
        let frames = (self.files.values())
            .flat_map(|file| file.frames.iter().flatten().chain(file.patches.values()));
        let values = (self.records.values()).filter_map(|value| match value {
            Value::Stored(at) => Some(at),
            Value::Inline(_) => None,
        });
        frames.chain(values)
    }

    /// Every location it names, to change.
    #[cfg(any(test, feature = "__bench"))]
    pub(crate) fn locations_mut(&mut self) -> impl Iterator<Item = &mut Location> {
        let frames = (self.files.values_mut()).flat_map(|file| {
            file.frames
                .iter_mut()
                .flatten()
                .chain(file.patches.values_mut())
        });
        let values = (self.records.values_mut()).filter_map(|value| match value {
            Value::Stored(at) => Some(at),
            Value::Inline(_) => None,
        });
        frames.chain(values)
    }

    /// Point what it names at `moved` to where it moved.
    pub(crate) fn relocate(&mut self, moved: &HashMap<Location, Location>) {
        let mut relocate = |at: &mut Location| {
            if let Some(to) = moved.get(at) {
                *at = *to;
            }
        };
        for file in self.files.values_mut() {
            file.frames.iter_mut().flatten().for_each(&mut relocate);
            file.patches.values_mut().for_each(&mut relocate);
        }
        for value in self.records.values_mut() {
            if let Value::Stored(at) = value {
                relocate(at);
            }
        }
    }

    /// The packs it names.
    pub(crate) fn packs(&self) -> BTreeSet<PackId> {
        self.locations().map(|at| at.pack).collect()
    }

    /// The manifest; an error past the 1 GiB a decode takes.
    pub(crate) fn encode(&self) -> io::Result<Bytes> {
        let packs: Vec<PackId> = self.sizes.keys().copied().collect();
        let index: HashMap<PackId, usize> =
            packs.iter().enumerate().map(|(i, &p)| (p, i)).collect();
        let mut body = Vec::new();
        put_varint(&mut body, packs.len() as u64);
        for pack in &packs {
            put_varint(&mut body, pack.version);
            put_varint(&mut body, pack.nonce);
            put_varint(&mut body, pack.tier as u64);
            put_varint(&mut body, self.sizes[pack]);
        }
        // A location as its pack's index plus one (0 is none), and its
        // offset as a delta from the end of the one before, zigzagged:
        // frames written in a row cost a few bytes each.
        let mut end = 0u64;
        let mut location = |body: &mut Vec<u8>, at: Option<&Location>| {
            let Some(at) = at else {
                return body.push(0);
            };
            put_varint(body, index[&at.pack] as u64 + 1);
            let delta = at.offset.wrapping_sub(end) as i64;
            put_varint(body, ((delta << 1) ^ (delta >> 63)) as u64);
            put_varint(body, at.len);
            end = at.offset + at.len;
        };
        let mut value = |body: &mut Vec<u8>, value: &Value| match value {
            Value::Inline(bytes) => {
                body.push(0);
                put_bytes(body, bytes);
            }
            Value::Stored(at) => {
                body.push(1);
                location(body, Some(at));
            }
        };
        put_varint(&mut body, self.records.len() as u64);
        for (name, record) in &self.records {
            put_bytes(&mut body, name.as_bytes());
            value(&mut body, record);
        }
        put_varint(&mut body, self.files.len() as u64);
        for (name, file) in &self.files {
            put_bytes(&mut body, name.as_bytes());
            put_varint(&mut body, file.size / 512);
            put_varint(&mut body, file.frames.len() as u64);
            for frame in &file.frames {
                location(&mut body, frame.as_ref());
            }
            put_varint(&mut body, file.patches.len() as u64);
            let mut next = 0;
            for (&page, at) in &file.patches {
                put_varint(&mut body, page - next);
                next = page + 1;
                location(&mut body, Some(at));
            }
        }
        if body.len() > MAX_DECODED {
            return Err(too_large("a manifest", body.len()));
        }
        let mut out = MANIFEST_MAGIC.to_vec();
        zstd::compress(&body, &[], |frame| out.extend_from_slice(frame))
            .expect("compressing in memory");
        Ok(out.into())
    }

    pub(crate) fn decode(bytes: &[u8]) -> io::Result<Self> {
        let frame = (bytes.strip_prefix(MANIFEST_MAGIC)).ok_or_else(|| invalid("manifest"))?;
        let body = zstd::decompress_sized(frame, &[], MAX_DECODED)?;
        let mut r = Reading::new(&body, "manifest");
        let mut manifest = Manifest::default();
        let mut packs = Vec::new();
        for _ in 0..r.count()? {
            let pack = PackId {
                version: r.varint()?,
                nonce: r.varint()?,
                tier: Tier::decode(r.varint()?).ok_or_else(|| r.invalid())?,
            };
            manifest.sizes.insert(pack, r.varint()?);
            packs.push(pack);
        }
        let sizes = manifest.sizes.clone();
        let mut end = 0u64;
        let mut location = |r: &mut Reading| -> io::Result<Option<Location>> {
            let Some(pack) = r.varint()?.checked_sub(1) else {
                return Ok(None);
            };
            let pack = *packs.get(pack as usize).ok_or_else(|| r.invalid())?;
            let z = r.varint()?;
            let delta = (z >> 1) as i64 ^ -((z & 1) as i64);
            let offset = end.wrapping_add(delta as u64);
            let len = r.varint()?;
            // Within its pack, whose size the manifest holds.
            end = (offset.checked_add(len))
                .filter(|&end| end <= sizes[&pack])
                .ok_or_else(|| r.invalid())?;
            Ok(Some(Location { pack, offset, len }))
        };
        let mut value = |r: &mut Reading| -> io::Result<Value> {
            match r.varint()? {
                0 => Ok(Value::Inline(Bytes::copy_from_slice(r.bytes()?))),
                1 => Ok(Value::Stored(location(r)?.ok_or_else(|| r.invalid())?)),
                _ => Err(r.invalid()),
            }
        };
        for _ in 0..r.count()? {
            let name = r.name()?;
            let record = value(&mut r)?;
            manifest.records.insert(name, record);
        }
        for _ in 0..r.count()? {
            let name = r.name()?;
            let size = r.bounded(MAX_FILE_BYTES / 512)? * 512;
            let frames = (0..r.count()?)
                .map(|_| location(&mut r))
                .collect::<io::Result<_>>()?;
            let mut patches = BTreeMap::new();
            let mut next = 0u64;
            for _ in 0..r.count()? {
                let page = next
                    .checked_add(r.varint()?)
                    .filter(|&page| page < size.div_ceil(PAGE))
                    .ok_or_else(|| r.invalid())?;
                next = page + 1;
                patches.insert(page, location(&mut r)?.ok_or_else(|| r.invalid())?);
            }
            let file = FileIndex {
                size,
                frames,
                patches,
            };
            manifest.files.insert(name, file);
        }
        r.end()?;
        let names = manifest.files.len() + manifest.records.len();
        if names > MAX_NAMES {
            return Err(invalid("manifest"));
        }
        Ok(manifest)
    }
}

#[cfg(test)]
#[path = "tests/unit/format.rs"]
mod tests;
