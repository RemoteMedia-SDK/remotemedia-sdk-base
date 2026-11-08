use super::*;
use crate::channels::{DataChannelDecodeError, DataChannelMessage};
use crate::config::DataChannelMode;
use crate::media::audio::AudioEncoderConfig;
use crate::media::tracks::AudioTrack;
use bytes::Bytes;
use futures_util::FutureExt;
use remotemedia_types::{BufferBudget, CopyCounts, CopyPolicy, CopyReason};
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use webrtc::data_channel::RTCDataChannel;
use webrtc::rtp_transceiver::rtp_codec::RTPCodecType;

struct LiveTransport {
    transport: WebRtcTransport,
    receiver: PeerConnection,
    remote: Arc<RTCDataChannel>,
    wire: mpsc::UnboundedReceiver<Bytes>,
    rtp: mpsc::UnboundedReceiver<(RTPCodecType, Bytes)>,
    audio: Arc<AudioTrack>,
    readers: Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl LiveTransport {
    async fn new(copies: CopyContext) -> Self {
        let config = WebRtcTransportConfig {
            stun_servers: vec![],
            turn_servers: vec![],
            ..Default::default()
        };
        // The transport's public validation requires a STUN entry. It is never
        // started here; manually attached peers use the local-only config above.
        let transport =
            WebRtcTransport::new_with_copy_context(WebRtcTransportConfig::default(), copies)
                .unwrap();
        let sender = Arc::new(PeerConnection::new("remote".into(), &config).await.unwrap());
        let receiver = PeerConnection::new("source".into(), &config).await.unwrap();
        let audio = sender
            .add_audio_track(AudioEncoderConfig {
                sample_rate: 48000,
                channels: 2,
                ..Default::default()
            })
            .await
            .unwrap();
        #[cfg(feature = "video")]
        sender
            .add_video_track(crate::media::video::VideoEncoderConfig {
                width: 64,
                height: 64,
                ..Default::default()
            })
            .await
            .unwrap();
        sender
            .add_data_channel(DataChannelMode::Reliable)
            .await
            .unwrap();
        let (channels, mut channel_rx) = mpsc::unbounded_channel();
        let (wire_tx, wire) = mpsc::unbounded_channel();
        receiver
            .peer_connection()
            .on_data_channel(Box::new(move |channel| {
                let channels = channels.clone();
                let wire_tx = wire_tx.clone();
                Box::pin(async move {
                    channel.on_message(Box::new(move |message| {
                        let tx = wire_tx.clone();
                        Box::pin(async move {
                            let _ = tx.send(message.data);
                        })
                    }));
                    channels.send(channel).unwrap();
                })
            }));
        let (rtp_tx, rtp) = mpsc::unbounded_channel();
        let readers = Arc::new(std::sync::Mutex::new(Vec::new()));
        let track_readers = readers.clone();
        receiver
            .on_track(move |track, _, _| {
                let tx = rtp_tx.clone();
                let readers = track_readers.clone();
                Box::pin(async move {
                    // webrtc-rs holds its callback mutex until this future ends.
                    // Each read loop must run independently to permit a second track.
                    let task = tokio::spawn(async move {
                        while let Ok((packet, _)) = track.read_rtp().await {
                            if tx.send((track.kind(), packet.payload)).is_err() {
                                break;
                            }
                        }
                    });
                    readers.lock().unwrap().push(task);
                })
            })
            .await;
        transport
            .peer_manager
            .add_peer("remote".into(), sender.clone())
            .await
            .unwrap();
        crate::test_support::negotiate(sender.peer_connection(), receiver.peer_connection()).await;
        while !sender.is_data_channel_open().await || transport.list_peers().await.len() != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let remote = channel_rx.recv().await.unwrap();
        Self {
            transport,
            receiver,
            remote,
            wire,
            rtp,
            audio,
            readers,
        }
    }

    async fn receive_media(&mut self, kind: RTPCodecType) {
        let (actual, bytes) = self.rtp.recv().await.unwrap();
        assert_eq!(actual, kind);
        assert!(!bytes.is_empty());
    }

    async fn close(self) {
        self.audio.shutdown().await.unwrap();
        self.transport.shutdown().await.unwrap();
        self.receiver.close().await.unwrap();
        let readers = std::mem::take(&mut *self.readers.lock().unwrap());
        for reader in readers {
            timeout(Duration::from_secs(5), reader)
                .await
                .unwrap()
                .unwrap();
        }
    }
}

fn counts(copies: &CopyContext, reason: CopyReason) -> CopyCounts {
    copies
        .report()
        .entries
        .into_iter()
        .find(|(r, _)| *r == reason)
        .unwrap()
        .1
}

async fn check_messages(
    live: &mut LiveTransport,
    copies: &CopyContext,
    policy: CopyPolicy,
) -> DataChannelMessage {
    let binary = DataChannelMessage::binary(vec![0, 1, 2, 255]);
    let (tx, mut rx) = mpsc::unbounded_channel();
    live.transport
        .on_peer_data_channel_message_result("remote", move |message| {
            let tx = tx.clone();
            async move {
                tx.send(message).unwrap();
            }
        })
        .await
        .unwrap();
    live.remote
        .send(&Bytes::from(binary.encode().unwrap()))
        .await
        .unwrap();
    let retained = rx.recv().await.unwrap().unwrap();
    assert_eq!(retained, binary);
    assert!(copies
        .report()
        .entries
        .iter()
        .all(|(_, c)| c.copied_bytes == 0));
    let legacy = Bytes::from_static(br#"{"type":"Binary","payload":"AAEC/w=="}"#);
    live.remote.send(&legacy).await.unwrap();
    let received = rx.recv().await.unwrap();
    if policy == CopyPolicy::RequireZeroCopy {
        assert!(matches!(
            received,
            Err(DataChannelDecodeError::CopyRejected(_))
        ));
    } else {
        assert_eq!(received.unwrap().as_binary().unwrap(), &[0, 1, 2, 255]);
    }
    live.remote
        .send(&Bytes::from_static(b"\xff"))
        .await
        .unwrap();
    assert!(matches!(
        rx.recv().await.unwrap(),
        Err(DataChannelDecodeError::Protobuf(_))
    ));

    let text = DataChannelMessage::text("metadata");
    live.transport
        .send_data_channel_message("remote", &text)
        .await
        .unwrap();
    assert_eq!(
        DataChannelMessage::decode_owned(live.wire.recv().await.unwrap()).unwrap(),
        text
    );
    let sent = live
        .transport
        .send_data_channel_message("remote", &binary)
        .await;
    let stats = live
        .transport
        .broadcast_data_channel_message(&binary, None)
        .await;
    assert_eq!(stats.total_peers, 1);
    if policy == CopyPolicy::RequireZeroCopy {
        assert!(sent.unwrap_err().to_string().contains("standalone"));
        assert_eq!((stats.sent_count, stats.failed_count), (0, 1));
        assert_eq!(
            counts(copies, CopyReason::PortableSerialization).rejected_bytes,
            8
        );
        assert!(live.wire.try_recv().is_err());
    } else {
        sent.unwrap();
        assert_eq!((stats.sent_count, stats.failed_count), (1, 0));
        for _ in 0..2 {
            assert_eq!(
                DataChannelMessage::decode_owned(live.wire.recv().await.unwrap()).unwrap(),
                binary
            );
        }
        assert_eq!(
            counts(copies, CopyReason::PortableSerialization).copied_bytes,
            8
        );
    }

    // The simple callback also honors the selected authority. Ordered metadata
    // following a rejected legacy frame proves it does not deliver that frame.
    let (tx, mut simple) = mpsc::unbounded_channel();
    live.transport
        .on_peer_data_channel_message("remote", move |message| {
            let tx = tx.clone();
            async move {
                tx.send(message).unwrap();
            }
        })
        .await
        .unwrap();
    live.remote.send(&legacy).await.unwrap();
    live.remote
        .send(&Bytes::from(text.encode().unwrap()))
        .await
        .unwrap();
    if policy == CopyPolicy::AllowCopies {
        assert_eq!(
            simple.recv().await.unwrap().as_binary().unwrap(),
            &[0, 1, 2, 255]
        );
    }
    assert_eq!(simple.recv().await.unwrap(), text);
    let c = counts(copies, CopyReason::PortableDeserialization);
    assert_eq!(
        (c.copied_bytes, c.rejected_bytes),
        if policy == CopyPolicy::AllowCopies {
            (8, 0)
        } else {
            (0, 8)
        }
    );
    retained
}

fn audio_data(samples: usize, rate: u32) -> RuntimeData {
    RuntimeData::Audio {
        samples: vec![0.25; samples].into(),
        sample_rate: rate,
        channels: 2,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    }
}

async fn check_audio(live: &mut LiveTransport, copies: &CopyContext, policy: CopyPolicy) {
    // The old send_to_peer path only encoded and discarded its returned packet.
    assert!(live.rtp.try_recv().is_err());
    live.transport
        .send_to_peer("remote", &audio_data(1920, 48000))
        .await
        .unwrap();
    live.receive_media(RTPCodecType::Audio).await;
    assert_eq!(
        live.transport
            .broadcast(&audio_data(1920, 48000))
            .await
            .unwrap()
            .sent_count,
        1
    );
    live.receive_media(RTPCodecType::Audio).await;
    live.transport
        .send_audio("remote", Arc::new(vec![0.25; 960]))
        .await
        .unwrap();
    live.receive_media(RTPCodecType::Audio).await;
    live.transport
        .broadcast_audio(Arc::new(vec![0.25; 960]), None)
        .await
        .unwrap();
    live.receive_media(RTPCodecType::Audio).await;
    assert_eq!(
        counts(copies, CopyReason::VecMaterialization).copied_bytes,
        0
    );

    let budget = BufferBudget::new(2, 3840);
    let first = budget.admit(audio_data(480, 24000)).unwrap();
    live.transport.send_to_peer("remote", &first).await.unwrap();
    drop(first);
    let second = budget.admit(audio_data(480, 24000)).unwrap();
    let result = live.transport.send_to_peer("remote", &second).await;
    drop(second);
    if policy == CopyPolicy::RequireZeroCopy {
        assert!(result.unwrap_err().to_string().contains("standalone"));
        assert_eq!(
            counts(copies, CopyReason::VecMaterialization).rejected_bytes,
            1920
        );
    } else {
        result.unwrap();
        live.receive_media(RTPCodecType::Audio).await;
        assert_eq!(
            counts(copies, CopyReason::VecMaterialization).copied_bytes,
            3840
        );
    }
    live.audio.shutdown().await.unwrap();
    assert_eq!(budget.usage().owners, 0);
}

#[cfg(feature = "video")]
async fn check_video(live: &mut LiveTransport, copies: &CopyContext, policy: CopyPolicy) {
    use crate::media::tracks::video_copy_tests::{frame, YUV_BYTES};
    use crate::media::video::{VideoFormat, VideoFrame};
    use remotemedia_core::data::video::PixelFormat;
    let data = frame(PixelFormat::Yuv420p);
    let RuntimeData::Video { pixel_data, .. } = &data else {
        unreachable!()
    };
    let video = VideoFrame {
        data: pixel_data.clone(),
        width: 64,
        height: 64,
        format: VideoFormat::I420,
        timestamp_us: 33_333,
        is_keyframe: true,
    };
    let generic = live.transport.send_to_peer("remote", &data).await;
    let direct = live.transport.send_video("remote", &video).await;
    let broadcast = live.transport.broadcast(&data).await.unwrap();
    live.transport.broadcast_video(&video, None).await.unwrap();
    if policy == CopyPolicy::RequireZeroCopy {
        assert!(generic.is_err() && direct.is_err());
        assert_eq!(broadcast.failed_count, 1);
        assert_eq!(
            counts(copies, CopyReason::ForeignPoolIngress).rejected_bytes,
            4 * YUV_BYTES
        );
    } else {
        generic.unwrap();
        direct.unwrap();
        assert_eq!(broadcast.sent_count, 1);
        live.receive_media(RTPCodecType::Video).await;
        assert_eq!(
            counts(copies, CopyReason::ForeignPoolIngress).copied_bytes,
            4 * YUV_BYTES
        );
    }
}

#[tokio::test]
async fn standalone_policy_real_peers_cover_media_messages_and_broadcasts() {
    for policy in [CopyPolicy::AllowCopies, CopyPolicy::RequireZeroCopy] {
        let copies = CopyContext::new(policy, "standalone");
        let mut live = timeout(Duration::from_secs(20), LiveTransport::new(copies.clone()))
            .await
            .unwrap();
        let work = async {
            let retained = check_messages(&mut live, &copies, policy).await;
            check_audio(&mut live, &copies, policy).await;
            #[cfg(feature = "video")]
            check_video(&mut live, &copies, policy).await;
            if policy == CopyPolicy::RequireZeroCopy {
                assert!(copies
                    .report()
                    .entries
                    .iter()
                    .all(|(_, c)| c.copied_bytes == 0));
            }
            retained
        };
        let result = std::panic::AssertUnwindSafe(timeout(Duration::from_secs(20), work))
            .catch_unwind()
            .await;
        live.close().await;
        let retained = result
            .unwrap()
            .expect("standalone transport test timed out");
        assert_eq!(retained.as_binary().unwrap(), &[0, 1, 2, 255]);
    }
}
