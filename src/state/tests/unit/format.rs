use super::*;

fn checkpoint(version: u64) -> Checkpoint {
    Checkpoint {
        pack: PackId {
            version,
            nonce: u64::MAX - version,
            tier: if version.is_multiple_of(2) {
                Tier::L0
            } else {
                Tier::L1
            },
        },
        manifest: 77,
    }
}

#[test]
fn a_head_decodes_as_encoded_and_not_truncated() {
    let head = Head {
        version: 9,
        checkpoint: Some(checkpoint(6)),
        previous: Some(checkpoint(2)),
        tail_first: 3,
        parts: vec![1, 0, 2, 1, 1, 0, 1],
        runs: vec![
            Run {
                first: 3,
                nonce: 0xaaaa,
            },
            Run {
                first: 6,
                nonce: 0xbbbb,
            },
        ],
        tail_bytes: 4096,
        ..Head::new(0x1234_5678_9abc, 16)
    };
    let bytes = head.encode();
    assert_eq!(Head::decode(&bytes).unwrap(), head);
    for len in 0..bytes.len() {
        assert!(Head::decode(&bytes[..len]).is_err(), "truncated to {len}");
    }
    // A checkpoint past the head's version, out of order, or a tail of the
    // wrong extent.
    let ahead = Head {
        checkpoint: Some(checkpoint(10)),
        ..head.clone()
    };
    assert!(Head::decode(&ahead.encode()).is_err());
    let swapped = Head {
        checkpoint: Some(checkpoint(2)),
        previous: Some(checkpoint(6)),
        ..head.clone()
    };
    assert!(Head::decode(&swapped.encode()).is_err());
    let short = Head {
        parts: vec![1],
        ..head.clone()
    };
    assert!(Head::decode(&short.encode()).is_err());
    // Runs that do not cover the tail kept, in order, from its first entry.
    for runs in [
        vec![],
        vec![Run { first: 4, nonce: 1 }],
        vec![Run { first: 3, nonce: 1 }, Run { first: 3, nonce: 2 }],
        vec![
            Run { first: 3, nonce: 1 },
            Run {
                first: 10,
                nonce: 2,
            },
        ],
    ] {
        let wrong = Head {
            runs: runs.clone(),
            ..head.clone()
        };
        assert!(Head::decode(&wrong.encode()).is_err(), "{runs:?}");
    }
    assert_eq!(head.entry_keys(9), [tail_key(9, 0xbbbb, 0)]);
    assert!(head.entry_keys(8).is_empty());
    assert_eq!(
        head.entry_keys(5),
        [tail_key(5, 0xaaaa, 0), tail_key(5, 0xaaaa, 1)]
    );
    assert!(head.entry_keys(2).is_empty() && head.entry_keys(10).is_empty());
}

/// A head written before tail entries had nonces (`SH22`, without runs)
/// reads as one run of entries without a nonce, named as they were.
#[test]
fn a_head_written_before_nonces_names_its_entries_as_they_were() {
    let head = Head {
        version: 5,
        checkpoint: Some(checkpoint(4)),
        previous: Some(checkpoint(3)),
        tail_first: 4,
        parts: vec![1, 2],
        runs: vec![Run { first: 4, nonce: 0 }],
        tail_bytes: 300,
        ..Head::new(7, 16)
    };
    // Its bytes: the magic of then, and no runs (an empty list is one byte).
    let encoded = Head {
        runs: Vec::new(),
        ..head.clone()
    }
    .encode();
    let legacy = [b"SH22".as_slice(), &encoded[4..encoded.len() - 1]].concat();
    assert_eq!(Head::decode(&legacy).unwrap(), head);
    assert_eq!(head.entry_keys(5), ["v22/tail/5/0", "v22/tail/5/1"]);
    // An empty tail (checkpointed at its version) has no run.
    let empty = Head {
        version: 3,
        checkpoint: Some(checkpoint(3)),
        previous: Some(checkpoint(3)),
        tail_first: 4,
        parts: Vec::new(),
        runs: Vec::new(),
        ..head.clone()
    };
    let encoded = empty.encode();
    let legacy = [b"SH22".as_slice(), &encoded[4..encoded.len() - 1]].concat();
    assert_eq!(Head::decode(&legacy).unwrap(), empty);
    assert_eq!(Head::decode(&encoded).unwrap(), empty);
}

/// A writer's flushes share a run; another writer's, or a nonce drawn
/// again, start one; a checkpoint that drops the entries before it drops
/// their runs.
#[test]
fn runs_follow_the_nonces_and_the_tail_kept() {
    let mut head = Head {
        version: 1,
        parts: vec![1],
        runs: vec![Run {
            first: 1,
            nonce: 0xa,
        }],
        ..Head::new(7, 16)
    };
    for (version, nonce) in [(2, 0xa), (3, 0xa), (4, 0xb), (5, 0xc), (6, 0xc)] {
        head.version = version;
        head.parts.push(1);
        head.written_by(nonce);
    }
    let firsts = |head: &Head| {
        head.runs
            .iter()
            .map(|run| (run.first, run.nonce))
            .collect::<Vec<_>>()
    };
    assert_eq!(firsts(&head), [(1, 0xa), (4, 0xb), (5, 0xc)]);
    assert_eq!(Head::decode(&head.encode()).unwrap(), head);
    // The entries before 4 go.
    head.parts.drain(..3);
    head.tail_first = 4;
    head.trim_runs();
    assert_eq!(firsts(&head), [(4, 0xb), (5, 0xc)]);
    // And then those before 6: the run of 5 is kept, from 6.
    head.parts.drain(..2);
    head.tail_first = 6;
    head.trim_runs();
    assert_eq!(firsts(&head), [(6, 0xc)]);
    // As a head names them: past the previous checkpoint.
    head.previous = Some(checkpoint(5));
    head.checkpoint = Some(checkpoint(6));
    assert_eq!(Head::decode(&head.encode()).unwrap(), head);
}

fn location(pack: PackId, offset: u64, len: u64) -> Location {
    Location { pack, offset, len }
}

#[test]
fn a_manifest_decodes_as_encoded_and_not_truncated() {
    let [old, new] = [checkpoint(3).pack, checkpoint(8).pack];
    let mut manifest = Manifest::default();
    manifest.files.insert(
        "db".into(),
        FileIndex {
            size: 3 * 16 * PAGE - 512,
            frames: vec![
                Some(location(new, 0, 900)),
                None,
                Some(location(old, 1 << 30, 70)),
            ],
            patches: BTreeMap::from([(5, location(new, 900, 40)), (40, location(old, 3, 9))]),
        },
    );
    manifest.files.insert("empty".into(), FileIndex::default());
    manifest
        .records
        .insert("small".into(), Value::Inline(Bytes::from_static(b"hi")));
    manifest
        .records
        .insert("large".into(), Value::Stored(location(new, 900, 5000)));
    manifest.sizes = BTreeMap::from([(old, 1 << 31), (new, 5900)]);
    let bytes = manifest.encode().unwrap();
    assert_eq!(Manifest::decode(&bytes).unwrap(), manifest);
    assert_eq!(manifest.packs(), BTreeSet::from([old, new]));
    assert_eq!(manifest.locations().count(), 5);
    for len in 0..bytes.len() {
        assert!(
            Manifest::decode(&bytes[..len]).is_err(),
            "truncated to {len}"
        );
    }
    let empty = Manifest::default();
    assert_eq!(Manifest::decode(&empty.encode().unwrap()).unwrap(), empty);
}

#[test]
fn a_tail_entry_splits_into_parts_and_reads_back() {
    let entry = Entry {
        records: vec![
            ("progress".into(), Bytes::from_static(b"step 3")),
            ("outbox".into(), Bytes::from(vec![9; 3 << 20])),
        ],
        files: vec![(
            "db".into(),
            FileChange {
                size: 9 * PAGE,
                pages: vec![(0, Some(Bytes::from(vec![1; 100]))), (8, None)],
            },
        )],
    };
    let parts = entry.encode().unwrap();
    assert_eq!(parts.len(), 4);
    assert!(parts.iter().all(|part| part.len() <= PART_BYTES));
    assert_eq!(Entry::decode(&parts).unwrap(), entry);
    let one = Entry::default().encode().unwrap();
    assert_eq!(Entry::decode(&one).unwrap(), Entry::default());
    assert!(Entry::decode(&parts[..3]).is_err());
}

#[test]
fn keys_parse_as_made() {
    let pack = checkpoint(12).pack;
    assert_eq!(PackId::parse(&pack.key()), Some(pack));
    assert_eq!(PackId::parse("checkpoint/zz-1"), None);
    let tail = |version, nonce, part| {
        Some(TailKey {
            version,
            nonce,
            part,
        })
    };
    assert_eq!(parse_tail_key(&tail_key(7, 0xabc, 2)), tail(7, 0xabc, 2));
    assert_eq!(parse_tail_key(&tail_key(7, 0, 2)), tail(7, 0, 2));
    assert_eq!(parse_tail_key("v22/tail/7-0000000000000000/2"), None);
    assert_eq!(parse_tail_key(HEAD_KEY), None);
    // Every key names one value for ever, but the head's and the tail
    // entries' written before nonces.
    assert!(!immutable(HEAD_KEY));
    assert!(!immutable(&tail_key(7, 0, 2)));
    assert!(immutable(&tail_key(7, 0xabc, 2)));
    assert!(immutable("v22/pack/l0/00000000000000aa-00000000000000bb/0"));
    assert!(immutable(
        "v22/manifest/00000000000000aa-00000000000000bb/0"
    ));
}
