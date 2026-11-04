use super::*;

#[test]
fn synced_writes_survive_a_crash() -> io::Result<()> {
    futures::executor::block_on(async {
        let store = MemoryKv::default();
        for value in [vec![1; 4096], vec![2; 37]] {
            store.put("key", value.clone().into())?;
            store.sync().await?;
            store.crash();
            assert_eq!(store.get("key").await?.as_deref(), Some(&value[..]));
        }
        store.delete("key")?;
        store.sync().await?;
        store.crash();
        assert!(store.get("key").await?.is_none());
        Ok(())
    })
}

/// Reads see every write issued; a crash keeps a prefix of the unsynced ones.
#[test]
fn a_crash_keeps_a_prefix_of_the_unsynced_writes() -> io::Result<()> {
    futures::executor::block_on(async {
        for kept in 0..=3 {
            let store = MemoryKv::default();
            store.put("a", vec![1].into())?;
            store.put("b", vec![1].into())?;
            store.sync().await?;
            store.put("a", vec![2].into())?;
            store.delete("b")?;
            store.put("c", vec![2].into())?;
            assert_eq!(store.get("a").await?.as_deref(), Some(&[2][..]));
            assert!(store.get("b").await?.is_none());
            assert_eq!(store.unsynced().len(), 3);
            store.crash_keeping(kept);
            let a = store.get("a").await?.map(Vec::from);
            let b = store.get("b").await?.map(Vec::from);
            let c = store.get("c").await?.map(Vec::from);
            assert_eq!(a, Some(if kept >= 1 { vec![2] } else { vec![1] }));
            assert_eq!(b, if kept >= 2 { None } else { Some(vec![1]) });
            assert_eq!(c, (kept >= 3).then(|| vec![2]));
            assert!(store.unsynced().is_empty());
        }
        Ok(())
    })
}
