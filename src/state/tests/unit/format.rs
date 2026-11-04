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
    assert_eq!(head.entry_keys(9), [tail_key(9, 0)]);
    assert!(head.entry_keys(8).is_empty());
    assert_eq!(head.entry_keys(5), [tail_key(5, 0), tail_key(5, 1)]);
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
    assert_eq!(parse_tail_key(&tail_key(7, 2)), Some((7, 2)));
    assert_eq!(parse_tail_key(HEAD_KEY), None);
}
