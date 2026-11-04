use super::*;

fn page(byte: u8) -> Page {
    vec![byte; PAGE as usize].into()
}

/// `image`, as the change after `before`.
fn change(image: &Page, before: &Page) -> Option<Bytes> {
    Some(encode_page(image, before).unwrap())
}

#[test]
fn a_chain_applies_its_changes_in_order() -> io::Result<()> {
    let (hole, checkpointed) = (page(0), page(1));
    let mut chain = Chain::default();
    assert_eq!(chain.fold(checkpointed.clone(), &hole)?, checkpointed);
    let mut before = checkpointed.clone();
    for byte in 2..6 {
        let mut image = before.to_vec();
        image[byte as usize * 100] = byte;
        let image = Page::from(image);
        chain.push(change(&image, &before));
        assert_eq!(chain.fold(checkpointed.clone(), &hole)?, image);
        before = image;
    }
    assert!(!chain.starts_over());
    Ok(())
}

#[test]
fn a_hole_starts_a_chain_over() -> io::Result<()> {
    let (hole, checkpointed) = (page(0), page(1));
    let mut chain = Chain::default();
    chain.push(change(&page(2), &checkpointed));
    chain.push(None);
    assert!(chain.starts_over());
    assert_eq!(chain.0.len(), 1, "what came before the hole is dropped");
    assert_eq!(chain.fold(checkpointed.clone(), &hole)?, hole);
    chain.push(change(&page(3), &hole));
    assert_eq!(chain.fold(checkpointed, &hole)?, page(3));
    Ok(())
}

/// A shrink drops the chains past it and cuts the checkpoint's pages there;
/// a page written after it builds on a hole, not the checkpoint's page.
#[test]
fn a_file_cut_builds_on_holes_past_the_cut() -> io::Result<()> {
    let (hole, checkpointed) = (page(0), page(1));
    let mut file = FileState {
        size: 8 * PAGE,
        ..FileState::default()
    };
    let changed = |size, pages: Vec<(u64, Option<Bytes>)>| FileChange { size, pages };
    file.apply(&changed(
        8 * PAGE,
        vec![(6, change(&page(6), &checkpointed))],
    ));
    assert!(file.on_checkpoint(6));
    assert_eq!(file.image(6, checkpointed.clone(), &hole)?, page(6));
    file.apply(&changed(4 * PAGE, vec![]));
    assert!(!file.pages.contains_key(&6));
    assert!(!file.on_checkpoint(6) && file.on_checkpoint(3));
    assert_eq!(file.image(6, checkpointed.clone(), &hole)?, hole);
    file.apply(&changed(8 * PAGE, vec![(6, change(&page(7), &hole))]));
    assert_eq!(file.image(6, checkpointed.clone(), &hole)?, page(7));
    assert_eq!(file.image(3, checkpointed.clone(), &hole)?, checkpointed);
    Ok(())
}
