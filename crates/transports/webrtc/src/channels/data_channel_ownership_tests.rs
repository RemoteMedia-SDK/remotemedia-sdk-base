use super::*;
use remotemedia_types::{BufferBudget, CopyCounts, CopyPolicy, RuntimeData};
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use webrtc::api::{setting_engine::SettingEngine, APIBuilder};
use webrtc::ice::network_type::NetworkType;

async fn peer() -> RTCPeerConnection {
    let mut settings = SettingEngine::default();
    settings.set_network_types(vec![NetworkType::Udp4]);
    settings.set_include_loopback_candidate(true);
    settings.set_ip_filter(Box::new(|ip| ip.is_loopback()));
    APIBuilder::new()
        .with_setting_engine(settings)
        .build()
        .new_peer_connection(Default::default())
        .await
        .unwrap()
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

#[tokio::test]
async fn channel_owners_dispatch_preserves_pointer_budget_and_abandoned_future_lifetime() {
    let peer = peer().await;
    let channel = DataChannel::new(&peer, "owner", DataChannelMode::Reliable)
        .await
        .unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut dispatch = channel.binary_handler(move |data| {
        let tx = tx.clone();
        async move {
            tx.send(data).unwrap();
        }
    });
    let budget = BufferBudget::new(1, 64);
    let RuntimeData::Binary(data) = budget
        .admit(RuntimeData::Binary(vec![7; 64].into()))
        .unwrap()
    else {
        unreachable!()
    };
    let pointer = data.as_ptr() as usize;
    let frame = webrtc::data_channel::data_channel_message::DataChannelMessage {
        is_string: false,
        data: Bytes::from_owner(data),
    };
    let pending = dispatch(frame);
    drop(dispatch);
    channel.close().await.unwrap();
    drop(channel);
    peer.close().await.unwrap();
    assert_eq!(budget.usage().owners, 1);
    pending.await;
    let data = rx.recv().await.unwrap();
    assert_eq!(data.as_ptr() as usize, pointer);
    let retained = data.slice(9..17);
    drop(data);
    assert_eq!(retained.as_ptr() as usize, pointer + 9);
    assert_eq!(&retained[..], &[7; 8]);
    assert_eq!(budget.usage().owners, 1);
    std::thread::spawn(move || drop(retained)).join().unwrap();
    assert_eq!(budget.usage().owners, 0);
}

#[tokio::test]
async fn channel_owners_dropped_dispatch_releases_owner_without_callback() {
    let peer = peer().await;
    let channel = DataChannel::new(&peer, "cancel", DataChannelMode::Reliable)
        .await
        .unwrap();
    let mut dispatch = channel.binary_handler(|_| async { panic!("cancelled callback ran") });
    let budget = BufferBudget::new(1, 64);
    let RuntimeData::Binary(data) = budget
        .admit(RuntimeData::Binary(vec![8; 64].into()))
        .unwrap()
    else {
        unreachable!()
    };
    let pending = dispatch(
        webrtc::data_channel::data_channel_message::DataChannelMessage {
            is_string: false,
            data: Bytes::from_owner(data.slice(4..4).unwrap()),
        },
    );
    drop(data);
    assert_eq!(budget.usage().owners, 1);
    drop(pending);
    assert_eq!(budget.usage().owners, 0);
    assert_eq!(channel.messages_received().await, 0);
    channel.close().await.unwrap();
    peer.close().await.unwrap();
}

#[tokio::test]
async fn channel_owners_send_validation_and_copy_rejection_precede_native_send() {
    let peer = peer().await;
    let channel = DataChannel::new(&peer, "validation", DataChannelMode::Reliable)
        .await
        .unwrap();
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "channel-borrowed");
    assert!(channel
        .send_binary_with_context(&[1, 2, 3], &copies)
        .await
        .is_err());
    assert_eq!(counts(&copies, CopyReason::BorrowedInput).completed, 0);
    assert!(channel.send_binary(&[1, 2, 3]).await.is_err());
    assert!(channel
        .validate_binary_send(MAX_MESSAGE_SIZE + 1)
        .await
        .is_err());
    *channel.state.write().await = DataChannelState::Open;
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "channel-strict");
    let err = channel
        .send_binary_with_context(&[1, 2, 3], &strict)
        .await
        .unwrap_err();
    let Error::Other(err) = err else {
        panic!("expected copy rejection")
    };
    assert_eq!(err.downcast_ref::<CopyRejected>().unwrap().bytes, 3);
    assert_eq!(counts(&strict, CopyReason::BorrowedInput).rejected_bytes, 3);
    // The wrapper snapshot completes even if the disconnected native send fails.
    assert!(channel
        .send_binary_with_context(&[1, 2, 3], &copies)
        .await
        .is_err());
    assert_eq!(counts(&copies, CopyReason::BorrowedInput).copied_bytes, 3);
    assert_eq!(channel.messages_sent().await, 0);
    channel.close().await.unwrap();
    peer.close().await.unwrap();
}

async fn negotiate(offer: &RTCPeerConnection, answer: &RTCPeerConnection) {
    let mut gathered = offer.gathering_complete_promise().await;
    offer
        .set_local_description(offer.create_offer(None).await.unwrap())
        .await
        .unwrap();
    gathered.recv().await;
    answer
        .set_remote_description(offer.local_description().await.unwrap())
        .await
        .unwrap();
    let mut gathered = answer.gathering_complete_promise().await;
    answer
        .set_local_description(answer.create_answer(None).await.unwrap())
        .await
        .unwrap();
    gathered.recv().await;
    offer
        .set_remote_description(answer.local_description().await.unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn channel_owners_peer_entrypoints_reject_missing_channel_without_copy() {
    let peer = crate::peer::PeerConnection::new(
        "owning-channel".into(),
        &crate::config::WebRtcTransportConfig::default(),
    )
    .await
    .unwrap();
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "peer-channel");
    assert!(peer
        .send_data_channel_binary_owned(Bytes::from_static(b"owned"))
        .await
        .is_err());
    assert!(peer
        .send_data_channel_binary_with_context(b"borrowed", &copies)
        .await
        .is_err());
    assert_eq!(counts(&copies, CopyReason::BorrowedInput).completed, 0);
    peer.close().await.unwrap();
}

#[tokio::test]
async fn channel_owners_real_loopback_retains_receives_and_attributes_compatibility() {
    use futures_util::FutureExt;
    let offer = peer().await;
    let answer = peer().await;
    let sender = DataChannel::new(&offer, "loopback", DataChannelMode::Reliable)
        .await
        .unwrap();
    let (channels_tx, mut channels_rx) = mpsc::unbounded_channel();
    answer.on_data_channel(Box::new(move |rtc| {
        let tx = channels_tx.clone();
        Box::pin(async move {
            assert!(tx
                .send(DataChannel::from_rtc_channel(
                    rtc,
                    DataChannelMode::Reliable
                ))
                .is_ok());
        })
    }));
    let work = async {
        negotiate(&offer, &answer).await;
        let receiver = channels_rx.recv().await.unwrap();
        while !sender.is_open().await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let (tx, mut rx) = mpsc::unbounded_channel();
        receiver.on_binary_message_owned(move |data| {
            let tx = tx.clone();
            async move {
                tx.send(data).unwrap();
            }
        });
        let expected: Vec<u8> = (0..64).collect();
        sender
            .send_binary_owned(Bytes::from(expected.clone()))
            .await
            .unwrap();
        let retained = rx.recv().await.unwrap().slice(7..19);
        assert_eq!(&retained[..], &expected[7..19]);

        let receive = CopyContext::new(CopyPolicy::AllowCopies, "loopback-receive");
        let (tx, mut rx) = mpsc::unbounded_channel();
        receiver.on_binary_message_with_context(receive.clone(), move |data| {
            let tx = tx.clone();
            async move {
                tx.send(data).unwrap();
            }
        });
        let send = CopyContext::new(CopyPolicy::AllowCopies, "loopback-send");
        sender
            .send_binary_with_context(&expected, &send)
            .await
            .unwrap();
        assert_eq!(rx.recv().await.unwrap().unwrap(), expected);
        assert_eq!(counts(&send, CopyReason::BorrowedInput).copied_bytes, 64);
        assert_eq!(
            counts(&receive, CopyReason::VecMaterialization).copied_bytes,
            64
        );

        let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "loopback-strict-snapshot");
        let (tx, mut rx) = mpsc::unbounded_channel();
        receiver.on_binary_message_with_context(strict.clone(), move |data| {
            let tx = tx.clone();
            async move {
                tx.send(data).unwrap();
            }
        });
        sender
            .send_binary_owned(Bytes::from(expected))
            .await
            .unwrap();
        assert_eq!(rx.recv().await.unwrap().unwrap_err().bytes, 64);
        assert_eq!(
            counts(&strict, CopyReason::VecMaterialization).rejected_bytes,
            64
        );
        assert_eq!(counts(&strict, CopyReason::VecMaterialization).completed, 0);
        let (tx, mut rx) = mpsc::unbounded_channel();
        receiver.on_message(move |message| {
            let tx = tx.clone();
            async move {
                tx.send(message).unwrap();
            }
        });
        let message = DataChannelMessage::binary(vec![0, 255, 13, 7]);
        let wire_len = message.encode().unwrap().len() as u64;
        assert!(sender
            .send_message_with_context(&message, &strict)
            .await
            .is_err());
        sender
            .send_message_with_context(&message, &send)
            .await
            .unwrap();
        assert_eq!(
            counts(&send, CopyReason::PortableSerialization).copied_bytes,
            4
        );
        assert_eq!(rx.recv().await.unwrap(), message);
        assert_eq!(sender.bytes_sent().await, 192 + wire_len);
        assert_eq!(receiver.bytes_received().await, 192 + wire_len);
        assert_eq!(receiver.messages_received().await, 4);
        receiver.close().await.unwrap();
        drop(receiver);
        retained
    };
    let result = std::panic::AssertUnwindSafe(timeout(Duration::from_secs(25), work))
        .catch_unwind()
        .await;
    sender.close().await.unwrap();
    offer.close().await.unwrap();
    answer.close().await.unwrap();
    let retained = result.unwrap().expect("local WebRTC negotiation timed out");
    assert_eq!(&retained[..], &(7..19).collect::<Vec<u8>>());
}
