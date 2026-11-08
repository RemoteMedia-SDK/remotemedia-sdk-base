use super::*;
use remotemedia_types::{
    BoundOutputAllocator, BufferAllocationError, BufferBudget, BufferLoan, HeapOutputAllocator,
    OutputAllocator,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Default)]
struct ObservedAllocator(AtomicUsize);

impl OutputAllocator for ObservedAllocator {
    fn allocate_bytes(
        &self,
        len: usize,
    ) -> std::result::Result<BufferLoan<u8>, BufferAllocationError> {
        HeapOutputAllocator.allocate_bytes(len)
    }
    fn allocate_audio(
        &self,
        len: usize,
    ) -> std::result::Result<BufferLoan<f32>, BufferAllocationError> {
        let loan = HeapOutputAllocator.allocate_audio(len)?;
        self.0.store(loan.as_ptr() as usize, Ordering::SeqCst);
        Ok(loan)
    }
}

#[test]
fn opus_selected_output_matches_legacy_for_rates_channels_and_durations() {
    let allocator = ObservedAllocator::default();
    for &sample_rate in OPUS_SUPPORTED_SAMPLE_RATES {
        for channels in [1, 2] {
            let config = AudioEncoderConfig {
                sample_rate,
                channels,
                ..Default::default()
            };
            let mut encoder = AudioEncoder::new(config.clone()).unwrap();
            let mut reference = AudioDecoder::new(config.clone()).unwrap();
            let mut selected = AudioDecoder::new(config).unwrap();
            for duration_us in [2500, 5000, 10000, 20000, 40000, 60000] {
                let count = sample_rate as usize * duration_us / 1_000_000 * channels as usize;
                let input: Vec<f32> = (0..count).map(|i| (i as f32 * 0.03).sin() * 0.2).collect();
                let packet = encoder.encode(&input).unwrap();
                let expected = reference.decode(&packet).unwrap();
                let actual = selected.decode_with_allocator(&packet, &allocator).unwrap();
                assert_eq!(actual.len(), count);
                assert_eq!(actual.as_slice(), expected);
                assert_eq!(actual.as_ptr() as usize, allocator.0.load(Ordering::SeqCst));
            }
        }
    }
}

#[test]
fn opus_selected_output_credit_survives_views_and_failure_does_not_advance_decoder() {
    let config = AudioEncoderConfig::default();
    let packet = AudioEncoder::new(config.clone())
        .unwrap()
        .encode(&vec![0.25; 960])
        .unwrap();
    let budget = BufferBudget::new(1, 960 * 4);
    let provider = Arc::new(ObservedAllocator::default());
    let allocator = BoundOutputAllocator::new(provider.clone()).with_budget(budget.clone());
    let mut decoder = AudioDecoder::new(config.clone()).unwrap();
    let mut reference = AudioDecoder::new(config).unwrap();
    let first = decoder.decode_with_allocator(&packet, &allocator).unwrap();
    assert_eq!(first.as_slice(), reference.decode(&packet).unwrap());
    assert_eq!(first.as_ptr() as usize, provider.0.load(Ordering::SeqCst));
    let retained = first.slice(4..8).unwrap();
    drop(first);
    assert_eq!(budget.usage().bytes, 960 * 4);
    assert!(decoder
        .decode_with_allocator(&packet, &allocator)
        .unwrap_err()
        .to_string()
        .contains("allocation"));
    assert_eq!(budget.usage().owners, 1);
    std::thread::spawn(move || drop(retained)).join().unwrap();
    assert_eq!(budget.usage().owners, 0);
    let retry = decoder.decode_with_allocator(&packet, &allocator).unwrap();
    assert_eq!(retry.as_slice(), reference.decode(&packet).unwrap());
    drop(retry);
    assert_eq!(budget.usage().bytes, 0);
}

#[test]
fn opus_selected_output_errors_release_credit_and_loss_concealment_is_bounded() {
    let budget = BufferBudget::new(1, 5760 * 4);
    let allocator =
        BoundOutputAllocator::new(Arc::new(HeapOutputAllocator)).with_budget(budget.clone());
    let mut decoder = AudioDecoder::new(AudioEncoderConfig::default()).unwrap();
    // A two-frame VBR header with no frame lengths passes the duration query but
    // fails decoding, exercising cleanup after the output loan was allocated.
    assert!(decoder.decode_with_allocator(&[0x02], &allocator).is_err());
    assert_eq!(budget.usage().owners, 0);
    assert!(decoder
        .decode_with_allocator(&[0x03, 0xff], &allocator)
        .is_err());
    assert_eq!(budget.usage().bytes, 0);
    let output = decoder.decode_with_allocator(&[], &allocator).unwrap();
    let expected = AudioDecoder::new(AudioEncoderConfig::default())
        .unwrap()
        .decode(&[])
        .unwrap();
    assert_eq!(output.as_slice(), expected);
    assert_eq!(output.len(), 5760);
    drop(output);
    assert_eq!(budget.usage().owners, 0);
}
