use super::*;
use remotemedia_types::{CopyContext, CopyPolicy, CopyReason};
use std::sync::{atomic::Ordering, mpsc};
use std::time::{Duration, Instant};

enum Command {
    Render(usize, mpsc::Sender<Vec<f32>>),
    Fail,
    Panic,
    Stop,
}
struct FakeStream {
    commands: mpsc::Sender<Command>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Drop for FakeStream {
    fn drop(&mut self) {
        let _ = self.commands.send(Command::Stop);
        let _ = self.thread.take().unwrap().join();
    }
}

pub(crate) struct TestDevice(mpsc::Sender<Command>);
impl TestDevice {
    pub fn render(&self, samples: usize) -> Vec<f32> {
        let (tx, rx) = mpsc::channel();
        self.0.send(Command::Render(samples, tx)).unwrap();
        rx.recv_timeout(Duration::from_secs(5)).unwrap()
    }
}

pub(crate) fn test_playback(copies: CopyContext) -> (AudioPlayback, TestDevice) {
    test_playback_on(copies, None)
}

/// `map`: `(packet channels, device channels)` rendered through `map_frame`.
fn test_playback_on(copies: CopyContext, map: Option<(u16, u16)>) -> (AudioPlayback, TestDevice) {
    let (tx, rx) = mpsc::channel();
    let commands = tx.clone();
    let config = PlaybackConfig {
        channels: map.map_or(1, |(channels, _)| channels),
        ..Default::default()
    };
    let playback = AudioPlayback::start_backend(config, copies, move |_, mut callback| {
        if let Some((channels, device_channels)) = map {
            callback.map_channels(channels, device_channels);
        }
        let thread = std::thread::spawn(move || {
            while let Ok(command) = rx.recv() {
                match command {
                    Command::Render(samples, reply) => {
                        let mut output = vec![0.; samples];
                        crate::audio_io::capture::tests::allocations::callback(|| {
                            callback.render(&mut output)
                        });
                        reply.send(output).unwrap();
                    }
                    Command::Fail => callback.error.store(true, Ordering::Release),
                    Command::Panic => panic!("simulated callback panic"),
                    Command::Stop => break,
                }
            }
            crate::audio_io::capture::tests::allocations::callback(|| drop(callback));
        });
        Ok((
            FakeStream {
                commands,
                thread: Some(thread),
            },
            "test output".into(),
        ))
    })
    .unwrap();
    (playback, TestDevice(tx))
}

fn copies() -> CopyContext {
    CopyContext::new(CopyPolicy::AllowCopies, "speaker-test")
}

#[test]
fn packets_map_onto_a_device_with_other_channels_without_callback_allocations() {
    // Mono goes to front left/right of a 4-channel device; the rest is silent.
    let (mut playback, device) = test_playback_on(copies(), Some((1, 4)));
    playback
        .queue_shared(vec![0.5f32, 0.25].into(), &copies())
        .unwrap();
    assert_eq!(
        device.render(12),
        [0.5, 0.5, 0., 0., 0.25, 0.25, 0., 0., 0., 0., 0., 0.]
    );
    // Stereo onto 3 channels: copied in order, the third silent.
    let (mut stereo, device) = test_playback_on(copies(), Some((2, 3)));
    stereo
        .queue_shared(vec![0.1f32, 0.2, 0.3, 0.4].into(), &copies())
        .unwrap();
    assert_eq!(device.render(6), [0.1, 0.2, 0., 0.3, 0.4, 0.]);
    playback.stop().unwrap();
    stereo.stop().unwrap();
}

#[test]
fn shared_playback_returns_owners_and_exact_partial_accounting_on_join() {
    let copies = copies();
    let (mut playback, device) = test_playback(copies.clone());
    playback
        .queue_shared(vec![1., 2., 3., 4.].into(), &copies)
        .unwrap();
    assert_eq!(device.render(2), vec![1., 2.]);
    playback.stop().unwrap();
    assert_eq!(playback.position(), 2);
    assert!(playback.is_complete());
    let report = copies.report();
    assert_eq!(
        report.entries[CopyReason::DeviceTransfer as usize]
            .1
            .copied_bytes,
        8
    );
    assert_eq!(
        report.entries[CopyReason::DeviceTransfer as usize]
            .1
            .in_flight,
        0
    );
    assert_eq!(
        report.entries[CopyReason::BorrowedInput as usize]
            .1
            .copied_bytes,
        0
    );
    assert!(playback.queue_shared(vec![9.].into(), &copies).is_err());
}

#[tokio::test]
async fn borrowed_compatibility_counts_staging_and_finish_drains() {
    let copies = copies();
    let (mut playback, device) = test_playback(copies.clone());
    playback.try_queue(&[1., 2., 3.]).unwrap();
    assert_eq!(device.render(4), vec![1., 2., 3., 0.]);
    playback.finish(Duration::from_secs(5)).await.unwrap();
    for reason in [CopyReason::BorrowedInput, CopyReason::DeviceTransfer] {
        assert_eq!(copies.report().entries[reason as usize].1.copied_bytes, 12);
    }
}

#[tokio::test]
async fn timeout_still_joins_and_accounts_unplayed_packets_without_copying() {
    let copies = copies();
    let (mut playback, _device) = test_playback(copies.clone());
    playback.queue_shared(vec![1.; 8].into(), &copies).unwrap();
    assert!(playback
        .finish(Duration::ZERO)
        .await
        .unwrap_err()
        .to_string()
        .contains("timed out"));
    assert!(playback.is_complete());
    assert_eq!(
        copies.report().entries[CopyReason::DeviceTransfer as usize]
            .1
            .copied_bytes,
        0
    );
    assert_eq!(
        copies.report().entries[CopyReason::DeviceTransfer as usize]
            .1
            .in_flight,
        0
    );
}

#[test]
fn startup_failure_and_panic_are_joined_and_reported() {
    let error = AudioPlayback::start_backend::<()>(PlaybackConfig::default(), copies(), |_, _| {
        anyhow::bail!("backend failure")
    });
    assert!(error.err().unwrap().to_string().contains("backend failure"));
    let panic = AudioPlayback::start_backend::<()>(PlaybackConfig::default(), copies(), |_, _| {
        panic!("backend panic")
    });
    assert!(panic
        .err()
        .unwrap()
        .to_string()
        .contains("control thread exited"));
}

#[test]
fn strict_policy_rejects_before_backend_creation_and_per_packet_admission() {
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "strict-speaker");
    assert!(AudioPlayback::start_backend::<()>(
        PlaybackConfig::default(),
        strict.clone(),
        |_, _| panic!("must not open")
    )
    .is_err());
    let (mut playback, device) = test_playback(copies());
    assert!(playback.queue_shared(vec![1.; 4].into(), &strict).is_err());
    assert_eq!(device.render(4), vec![0.; 4]);
    playback.stop().unwrap();
    assert_eq!(
        strict.report().entries[CopyReason::DeviceTransfer as usize]
            .1
            .rejected_bytes,
        16
    );
}

#[test]
fn callback_failure_or_unwind_stops_admission_and_recovers_pending_packets() {
    for command in [Command::Fail, Command::Panic] {
        let copies = copies();
        let (mut playback, device) = test_playback(copies.clone());
        playback.queue_shared(vec![1.; 8].into(), &copies).unwrap();
        device.0.send(command).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while playback.check_running().is_ok() {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(playback.stop().is_err());
        assert!(playback.is_complete());
        assert_eq!(
            copies.report().entries[CopyReason::DeviceTransfer as usize]
                .1
                .in_flight,
            0
        );
    }
}

#[tokio::test]
#[ignore = "requires an available physical output device; writes silence only"]
async fn physical_playback_silence() {
    assert_eq!(
        std::env::var("REMOTEMEDIA_PLAYBACK_SMOKE").as_deref(),
        Ok("1")
    );
    let copies = copies();
    let device = get_host(None).unwrap().default_output_device().unwrap();
    let layout = device.default_output_config().unwrap();
    assert_eq!(layout.sample_format(), cpal::SampleFormat::F32);
    let samples = (layout.sample_rate().0 / 20) as usize * usize::from(layout.channels());
    let mut playback = AudioPlayback::start_with_context(
        PlaybackConfig {
            sample_rate: layout.sample_rate().0,
            channels: layout.channels(),
            ..Default::default()
        },
        copies.clone(),
    )
    .unwrap();
    playback
        .queue_shared(vec![0.; samples].into(), &copies)
        .unwrap();
    playback.finish(Duration::from_secs(5)).await.unwrap();
    let counts = copies.report().entries[CopyReason::DeviceTransfer as usize].1;
    assert_eq!(counts.copied_bytes, samples as u64 * 4);
    assert_eq!(counts.in_flight, 0);
    assert_eq!(playback.position(), samples);
    assert!(copies
        .report()
        .entries
        .iter()
        .all(|(reason, counts)| *reason == CopyReason::DeviceTransfer || counts.copied_bytes == 0));
    println!(
        "device={}, rate={}, channels={}, rendered_samples={}, device_transfer_bytes={}",
        playback.device_name(),
        layout.sample_rate().0,
        layout.channels(),
        samples,
        counts.copied_bytes
    );
}
