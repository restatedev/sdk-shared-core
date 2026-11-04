//! Shared by the simulation tests: a seeded random source, a session of a state, and a reference model of its content.
#![allow(dead_code)]
use crate::state::{
    kv::KvStore, simulation::Setup, File, FlushReport, Mutation, Page, Result, State, PAGE_SIZE,
};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Arc;

/// A deterministic random source (splitmix64).
pub(super) struct Rng(u64);

impl Rng {
    pub(super) fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub(super) fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A number below `n` (0 when `n` is 0).
    pub(super) fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next() % n
        }
    }

    /// True `percent` times in a hundred.
    pub(super) fn percent(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }

    pub(super) fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len() as u64) as usize]
    }

    pub(super) fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.next() as u8).collect()
    }
}

/// A page numbered `n` in its first eight bytes, zeros after: compressible.
pub(super) fn page(n: u64) -> Page {
    let mut page = vec![0; PAGE_SIZE];
    page[..8].copy_from_slice(&n.to_le_bytes());
    page.into()
}

/// A page of mixed entropy: random bytes for a random part of it.
pub(super) fn varied(rng: &mut Rng) -> Page {
    let mut page = page(rng.next()).to_vec();
    let noisy = rng.below(PAGE_SIZE as u64) as usize;
    let at = rng.below((PAGE_SIZE - noisy) as u64 + 1) as usize;
    page[at..at + noisy].copy_from_slice(&rng.bytes(noisy));
    page.into()
}

/// The records a random change sets.
pub(super) const RECORDS: [&str; 2] = ["outbox", "schema"];

/// A change to a state (in a mutation) and to its model alike.
#[derive(Clone, Debug)]
pub(super) enum Op {
    /// Set a record.
    Record(&'static str, Bytes),
    /// Write a page of the file, or a hole.
    Write(u64, Option<Page>),
    /// Set the file's length.
    SetLen(u64),
}

impl Op {
    /// A random change: records small and over an inline budget of a few
    /// hundred bytes, pages written, holes, and lengths shrunk and grown.
    pub(super) fn random(rng: &mut Rng) -> Self {
        let len = |rng: &mut Rng| match rng.below(4) {
            0 => 0,
            1 => rng.below(40) as usize,
            2 => 100 + rng.below(400) as usize,
            _ => 1000 + rng.below(3000) as usize,
        };
        match rng.below(10) {
            0..=1 => {
                let len = len(rng);
                // Named by its length, which draws nothing more.
                Self::Record(RECORDS[len % RECORDS.len()], rng.bytes(len).into())
            }
            2 => Self::SetLen(512 * rng.below(8 * 20)),
            3 => Self::Write(rng.below(20), None),
            _ => Self::Write(rng.below(20), Some(varied(rng))),
        }
    }
}

/// A state and a handle on its file.
pub(super) struct Session {
    pub state: State,
    file: File,
}

impl Session {
    /// Open the state.
    pub(super) async fn open(store: Arc<dyn KvStore>, setup: impl Into<Setup>) -> Result<Self> {
        Ok(Self::of(setup.into().open(store).await?))
    }

    /// Open a reader beside the owner (see [`crate::state::Open::reader`]).
    pub(super) async fn reader(store: Arc<dyn KvStore>, setup: impl Into<Setup>) -> Result<Self> {
        Ok(Self::of(setup.into().open(store).reader().await?))
    }

    fn of(state: State) -> Self {
        let file = state.file();
        Self { state, file }
    }

    pub(super) fn file(&self) -> &File {
        &self.file
    }

    /// Make the change `op` in `mutation`, a mutation of this state.
    pub(super) fn apply(&self, mutation: &mut Mutation, op: &Op) -> Result<()> {
        match op {
            Op::Record(name, value) => mutation.set_record(name, value.clone()),
            Op::Write(index, image) => mutation.write(*index, image.clone()),
            Op::SetLen(len) => mutation.set_len(*len),
        }
    }

    /// Flush `ops`, in a mutation of their own.
    pub(super) async fn flush(&self, ops: &[Op]) -> Result<FlushReport> {
        let mut mutation = self.state.mutation();
        for op in ops {
            self.apply(&mut mutation, op)?;
        }
        mutation.flush().await
    }
}

/// What a state should hold, as last flushed: its records, and its file's
/// length and pages; an absent page is a hole.
#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct Model {
    records: BTreeMap<&'static str, Bytes>,
    len: u64,
    pages: BTreeMap<u64, Page>,
}

const PAGE: u64 = PAGE_SIZE as u64;

impl Model {
    pub(super) fn apply(&mut self, op: &Op) {
        match op {
            Op::Record(name, value) => {
                self.records.insert(name, value.clone());
            }
            Op::Write(index, image) => {
                self.len = self.len.max((index + 1) * PAGE);
                match image {
                    Some(image) => self.pages.insert(*index, image.clone()),
                    None => self.pages.remove(index),
                };
            }
            Op::SetLen(len) => {
                let kept = len.div_ceil(PAGE);
                self.pages.retain(|&index, _| index < kept);
                self.len = *len;
            }
        }
    }

    pub(super) fn applied(&self, ops: &[Op]) -> Self {
        let mut model = self.clone();
        for op in ops {
            model.apply(op);
        }
        model
    }

    /// What reading page `index` of the file returns.
    pub(super) fn page(&self, index: u64) -> Option<Page> {
        if index * PAGE >= self.len {
            return None;
        }
        self.pages.get(&index).cloned()
    }

    pub(super) fn len(&self) -> u64 {
        self.len
    }

    /// The record `name`: empty if never set.
    pub(super) fn record(&self, name: &str) -> Bytes {
        self.records.get(name).cloned().unwrap_or_default()
    }

    /// How `session` differs from the model; `None` when it reads the same.
    /// A session reads its state as last flushed, so compare after a flush.
    pub(super) async fn differs(&self, session: &Session) -> Result<Option<String>> {
        for name in RECORDS {
            let held = session.state.record(name);
            if held != self.record(name) {
                return Ok(Some(format!("record {name:?}: {} bytes", held.len())));
            }
        }
        let file = session.file();
        if file.len() != self.len {
            return Ok(Some(format!("{} bytes, not {}", file.len(), self.len)));
        }
        for index in 0..=self.len.div_ceil(PAGE) {
            let (held, expected) = (file.read(index).await?, self.page(index));
            if held != expected {
                let start = |page: Option<Page>| page.map(|page| page[..4].to_vec());
                let (held, expected) = (start(held), start(expected));
                return Ok(Some(format!(
                    "page {index} starts {held:02x?}, not {expected:02x?}"
                )));
            }
        }
        Ok(None)
    }

    /// Whether `session` reads as the model.
    pub(super) async fn matches(&self, session: &Session) -> Result<bool> {
        Ok(self.differs(session).await?.is_none())
    }
}
