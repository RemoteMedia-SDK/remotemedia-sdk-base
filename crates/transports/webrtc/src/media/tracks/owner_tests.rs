use super::*;
use remotemedia_types::{BufferBudget, BufferBudgetUsage, SharedBytes};

#[test]
fn video_wrapper_and_sample_slices_keep_the_owner_and_budget_until_last_drop() {
    let budget = BufferBudget::new(1, 64);
    let data = budget
        .admit(RuntimeData::Binary(vec![7; 48].into()))
        .unwrap();
    let RuntimeData::Binary(data) = data else {
        unreachable!()
    };
    let pointer = data.as_ptr();
    let frame = VideoFrame {
        width: 4,
        height: 4,
        format: VideoFormat::RGB24,
        data,
        timestamp_us: 1234,
        is_keyframe: true,
    };
    let clone = frame.clone();
    assert_eq!(clone.data.as_ptr(), pointer);
    let RuntimeData::Video {
        pixel_data,
        timestamp_us,
        is_keyframe,
        ..
    } = clone.as_runtime_data()
    else {
        unreachable!()
    };
    assert_eq!(pixel_data.as_ptr(), pointer);
    assert_eq!(timestamp_us, 1234);
    assert!(is_keyframe);
    let timestamp = std::time::SystemTime::UNIX_EPOCH + Duration::from_micros(timestamp_us);
    let sample = sample_from_owner(pixel_data, Duration::from_millis(20), timestamp);
    assert_eq!(sample.data.as_ptr(), pointer);
    assert_eq!(sample.timestamp, timestamp);
    let slice = sample.data.slice(8..16);
    assert_eq!(slice.as_ptr(), pointer.wrapping_add(8));
    drop((frame, clone, sample));
    assert_eq!(
        budget.usage(),
        BufferBudgetUsage {
            owners: 1,
            bytes: 48
        }
    );
    assert_eq!(&slice[..], &[7; 8]);
    drop(slice);
    assert_eq!(budget.usage(), BufferBudgetUsage::default());
}

#[test]
fn a_sample_retains_empty_subranges_without_detaching_the_backing_owner() {
    let budget = BufferBudget::new(1, 64);
    let RuntimeData::Binary(data) = budget
        .admit(RuntimeData::Binary(vec![1; 64].into()))
        .unwrap()
    else {
        unreachable!()
    };
    let empty: SharedBytes = data.slice(20..20).unwrap();
    drop(data);
    let sample = sample_from_owner(empty, Duration::ZERO, std::time::SystemTime::UNIX_EPOCH);
    assert!(sample.data.is_empty());
    assert_eq!(budget.usage().owners, 1);
    drop(sample);
    assert_eq!(budget.usage().owners, 0);
}
