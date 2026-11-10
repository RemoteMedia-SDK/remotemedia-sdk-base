use super::*;
use remotemedia_types::{BufferBudget, CopyPolicy, RuntimeData};

#[test]
fn channel_chunks_split_retains_ranges_and_single_delivery_keeps_budget() {
    let budget = BufferBudget::new(1, 16);
    let RuntimeData::Binary(data) = budget
        .admit(RuntimeData::Binary(vec![3; 16].into()))
        .unwrap()
    else {
        unreachable!()
    };
    let pointer = data.as_ptr() as usize;
    let splitter = DataChannelSplitter::with_max_chunk_size("s", "m", 4);
    let chunks = splitter.split_owned(Bytes::from_owner(data)).unwrap();
    for (i, chunk) in chunks.iter().enumerate() {
        assert_eq!(chunk.data.as_ptr() as usize, pointer + 4 * i);
    }
    let single = ChunkInfo::new("s", "one", 0, Some(1), true, chunks[1].data.clone());
    drop(chunks);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut reassembler = DataChannelReassembler::new().with_output(tx);
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "single-chunk");
    let output = reassembler
        .feed_chunk_with_context(&single, &strict)
        .unwrap()
        .unwrap();
    let queued = rx.try_recv().unwrap();
    assert_eq!(output.as_ptr() as usize, pointer + 4);
    assert_eq!(queued.as_ptr(), output.as_ptr());
    drop((single, output, reassembler));
    assert_eq!(budget.usage().owners, 1);
    drop(queued);
    assert_eq!(budget.usage().owners, 0);
    assert!(strict
        .report()
        .entries
        .iter()
        .all(|(_, c)| c.completed == 0 && c.rejected == 0));
}

#[test]
fn channel_chunks_all_orders_preserve_prefixes_and_count_only_final_assembly() {
    for order in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        for known_total in [false, true] {
            let splitter = DataChannelSplitter::with_max_chunk_size("s", "m", 2);
            let mut chunks = splitter.split_owned(Bytes::from_static(b"abcdef")).unwrap();
            if !known_total {
                for chunk in &mut chunks {
                    chunk.total_chunks = None;
                }
            }
            let copies = CopyContext::new(CopyPolicy::AllowCopies, "assembly");
            let (tx, mut rx) = mpsc::unbounded_channel();
            let mut reassembler = DataChannelReassembler::new().with_output(tx);
            for (received, index) in order.into_iter().enumerate() {
                let output = reassembler
                    .feed_chunk_with_context(&chunks[index], &copies)
                    .unwrap();
                if received < 2 {
                    assert!(output.is_none());
                    assert_eq!(reassembler.memory_used(), (received + 1) * 2);
                    let retained =
                        &reassembler.pending.values().next().unwrap().chunks[&(index as u32)];
                    assert_eq!(retained.as_ptr(), chunks[index].data.as_ptr());
                } else {
                    let output = output.unwrap();
                    assert_eq!(&output[..], b"abcdef");
                    assert_eq!(rx.try_recv().unwrap().as_ptr(), output.as_ptr());
                    assert_eq!(
                        (reassembler.memory_used(), reassembler.pending_count()),
                        (0, 0)
                    );
                }
            }
            let c = copies
                .report()
                .entries
                .into_iter()
                .find(|(r, _)| *r == CopyReason::VecMaterialization)
                .unwrap()
                .1;
            assert_eq!((c.completed, c.copied_bytes, c.failed), (1, 6, 0));
        }
    }
}

#[test]
fn channel_chunks_strict_assembly_rejection_discards_pending_owners() {
    let budget = BufferBudget::new(1, 6);
    let RuntimeData::Binary(data) = budget
        .admit(RuntimeData::Binary(vec![1; 6].into()))
        .unwrap()
    else {
        unreachable!()
    };
    let chunks = DataChannelSplitter::with_max_chunk_size("s", "m", 2)
        .split_owned(Bytes::from_owner(data))
        .unwrap();
    let mut reassembler = DataChannelReassembler::new();
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "assembly-strict");
    for chunk in &chunks[..2] {
        assert!(reassembler
            .feed_chunk_with_context(chunk, &strict)
            .unwrap()
            .is_none());
    }
    assert_eq!(
        reassembler
            .feed_chunk_with_context(&chunks[2], &strict)
            .unwrap_err()
            .bytes,
        6
    );
    assert_eq!(
        (reassembler.memory_used(), reassembler.pending_count()),
        (0, 0)
    );
    drop(chunks);
    assert_eq!(budget.usage().owners, 0);
}

#[test]
fn channel_chunks_limits_duplicates_and_conflicting_terminals_preserve_accounting() {
    let mut r = DataChannelReassembler::new().with_max_memory(4);
    let first = ChunkInfo::new("s", "m", 0, Some(2), false, vec![1, 2]);
    assert!(r.feed_chunk(&first).is_none());
    assert!(r.feed_chunk(&first).is_none());
    assert_eq!(r.memory_used(), 2);
    for invalid in [
        ChunkInfo::new("s", "m", 1, Some(3), true, vec![3]),
        ChunkInfo::new("s", "m", u32::MAX, None, true, vec![]),
        ChunkInfo::new("s", "other", 0, Some(1), true, vec![0; 5]),
        ChunkInfo::new("s", "other", 0, Some(0), false, vec![]),
        ChunkInfo::new("s", "other", 0, Some(4097), false, vec![]),
    ] {
        assert!(r.feed_chunk(&invalid).is_none());
        assert_eq!(r.memory_used(), 2);
    }
    assert_eq!(
        &r.feed_chunk(&ChunkInfo::new("s", "m", 1, Some(2), true, vec![3, 4]))
            .unwrap()[..],
        &[1, 2, 3, 4]
    );
    assert_eq!(r.memory_used(), 0);
    assert!(r.feed_chunk(&first).is_none());
    let next = ChunkInfo::new("s", "next", 0, Some(2), false, vec![8; 3]);
    assert!(r.feed_chunk(&next).is_none());
    assert_eq!((r.memory_used(), r.pending_count()), (3, 1));
    assert!(r.pending.contains_key(&("s".into(), "next".into())));
}

#[test]
fn channel_chunks_existing_message_at_capacity_and_expiration_release_owners() {
    let budget = BufferBudget::new(1, 4);
    let RuntimeData::Binary(data) = budget
        .admit(RuntimeData::Binary(vec![7; 4].into()))
        .unwrap()
    else {
        unreachable!()
    };
    let chunks = DataChannelSplitter::with_max_chunk_size("s", "m", 2)
        .split_owned(Bytes::from_owner(data))
        .unwrap();
    let mut r = DataChannelReassembler::new();
    r.max_pending = 1;
    assert!(r.feed_chunk(&chunks[0]).is_none());
    assert_eq!(&r.feed_chunk(&chunks[1]).unwrap()[..], &[7; 4]);
    assert_eq!((r.pending_count(), r.memory_used()), (0, 0));
    assert!(r.feed_chunk(&chunks[0]).is_none());
    drop(chunks);
    assert_eq!(budget.usage().owners, 1);
    r.pending.values_mut().next().unwrap().last_updated =
        Instant::now() - DEFAULT_REASSEMBLY_TIMEOUT - Duration::from_secs(1);
    assert_eq!(r.evict_expired(), 1);
    assert_eq!((r.pending_count(), r.memory_used()), (0, 0));
    assert_eq!(budget.usage().owners, 0);
}

#[test]
fn channel_chunks_borrowed_split_counts_or_rejects_after_configuration_checks() {
    let splitter = DataChannelSplitter::with_max_chunk_size("s", "m", 2);
    let allow = CopyContext::new(CopyPolicy::AllowCopies, "split");
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "split-strict");
    assert_eq!(
        splitter
            .split_with_context(b"abcdef", &allow)
            .unwrap()
            .len(),
        3
    );
    assert!(splitter.split_with_context(b"abcdef", &strict).is_err());
    let invalid = DataChannelSplitter::with_max_chunk_size("s", "m", 0);
    assert!(invalid.split_with_context(b"abc", &allow).is_err());
    assert!(DataChannelSplitter::with_max_chunk_size("s", "m", 1)
        .split_owned(Bytes::from(vec![0; 4097]))
        .is_err());
    let c = allow
        .report()
        .entries
        .into_iter()
        .find(|(r, _)| *r == CopyReason::BorrowedInput)
        .unwrap()
        .1;
    assert_eq!((c.completed, c.copied_bytes), (1, 6));
}
