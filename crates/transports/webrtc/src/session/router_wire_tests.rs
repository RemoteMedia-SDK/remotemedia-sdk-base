use super::*;
use crate::channels::{DataChannelMessage, DataChannelReassembler, MAX_CHUNK_SIZE};
use crate::config::{DataChannelMode, WebRtcTransportConfig};
use crate::generated::{data_buffer::DataType, DataBuffer};
use crate::peer::PeerConnection;
use bytes::Bytes;
use futures_util::FutureExt;
use prost::Message;
use remotemedia_types::{CopyContext, CopyPolicy, CopyReason};
use tokio::time::{timeout, Duration};

async fn negotiate(sender: &PeerConnection, receiver: &PeerConnection, advertised: Option<u32>) {
    let mut gathered = sender.peer_connection().gathering_complete_promise().await;
    sender.create_offer().await.unwrap();
    gathered.recv().await;
    let offer = sender
        .peer_connection()
        .local_description()
        .await
        .unwrap()
        .sdp;
    let mut gathered = receiver
        .peer_connection()
        .gathering_complete_promise()
        .await;
    receiver.create_answer(offer).await.unwrap();
    gathered.recv().await;
    let mut answer = receiver
        .peer_connection()
        .local_description()
        .await
        .unwrap()
        .sdp;
    if let Some(limit) = advertised {
        answer = answer
            .lines()
            .filter(|line| !line.starts_with("a=max-message-size:"))
            .map(|line| format!("{line}\r\n"))
            .collect();
        answer.push_str(&format!("a=max-message-size:{limit}\r\n"));
    }
    sender.set_remote_description(answer).await.unwrap();
    assert_eq!(
        wire_output::message_size_limit(sender).await.unwrap(),
        match advertised {
            Some(n) if n != 0 => n.min(65536) as usize,
            _ => 65536,
        }
    );
    while !sender.is_data_channel_open().await {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn receive_wire(rx: &mut mpsc::UnboundedReceiver<Bytes>) -> Bytes {
    let mut reassembly = DataChannelReassembler::new();
    loop {
        let wire = rx.recv().await.unwrap();
        match DataChannelMessage::decode_owned(wire).unwrap() {
            DataChannelMessage::RuntimeData { data_buffer, .. } => return data_buffer,
            DataChannelMessage::Chunk { chunk, .. } => {
                if let Some(data) = reassembly.feed_chunk(&chunk) {
                    return data;
                }
            }
            _ => panic!("unexpected outgoing envelope"),
        }
    }
}

async fn route_cases(sender: Arc<PeerConnection>, rx: &mut mpsc::UnboundedReceiver<Bytes>) {
    let peers = Arc::new(PeerManager::new(1).unwrap());
    peers.add_peer("sender".into(), sender).await.unwrap();
    for len in [16, 32768, 65536, MAX_CHUNK_SIZE + 17] {
        let runtime = RuntimeData::Binary(vec![42; len].into());
        let reference = crate::adapters::runtime_data_to_data_buffer(&runtime).encode_to_vec();
        for policy in [CopyPolicy::RequireZeroCopy, CopyPolicy::AllowCopies] {
            let copies = CopyContext::new(policy, "router-wire");
            let router = SessionRouter::new(
                "wire".into(),
                Arc::new(Session::new("wire".into())),
                peers.clone(),
            )
            .unwrap()
            .with_copy_context(copies.clone());
            let result = router.send_to_peer("sender", &runtime).await;
            if policy == CopyPolicy::RequireZeroCopy {
                assert!(result.unwrap_err().to_string().contains("router-wire"));
                assert!(rx.try_recv().is_err());
            } else {
                result.unwrap();
                let received = receive_wire(rx).await;
                assert_eq!(received.as_ref(), reference);
                let Some(DataType::Binary(binary)) =
                    DataBuffer::decode(received).unwrap().data_type
                else {
                    panic!("binary payload lost");
                };
                assert_eq!(binary.data.as_ref(), vec![42; len]);
            }
            let report = copies.report();
            let counts = report
                .entries
                .iter()
                .find(|(r, _)| *r == CopyReason::PortableSerialization)
                .unwrap()
                .1;
            assert_eq!(
                (counts.copied_bytes, counts.rejected_bytes),
                if policy == CopyPolicy::AllowCopies {
                    ((len + reference.len()) as u64, 0)
                } else {
                    (0, len as u64)
                }
            );
            assert_eq!((counts.failed, counts.in_flight), (0, 0));
            assert!(report
                .entries
                .iter()
                .filter(|(r, _)| *r != CopyReason::PortableSerialization)
                .all(|(_, c)| c.copied_bytes == 0));
        }
    }
}

#[tokio::test]
async fn wire_output_router_real_peers_count_small_and_chunked_transfers() {
    for advertised in [None, Some(32768), Some(0)] {
        local_peer_case(advertised).await;
    }
}

async fn local_peer_case(advertised: Option<u32>) {
    let config = WebRtcTransportConfig {
        stun_servers: Vec::new(),
        turn_servers: Vec::new(),
        ..Default::default()
    };
    let sender = Arc::new(PeerConnection::new("sender".into(), &config).await.unwrap());
    let receiver = PeerConnection::new("receiver".into(), &config)
        .await
        .unwrap();
    sender
        .add_data_channel(DataChannelMode::Reliable)
        .await
        .unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    receiver
        .peer_connection()
        .on_data_channel(Box::new(move |channel| {
            let tx = tx.clone();
            Box::pin(async move {
                channel.on_message(Box::new(move |message| {
                    let tx = tx.clone();
                    Box::pin(async move {
                        tx.send(message.data).unwrap();
                    })
                }));
            })
        }));
    let work = async {
        negotiate(&sender, &receiver, advertised).await;
        route_cases(sender.clone(), &mut rx).await;
    };
    let result = std::panic::AssertUnwindSafe(timeout(Duration::from_secs(25), work))
        .catch_unwind()
        .await;
    sender.close().await.unwrap();
    receiver.close().await.unwrap();
    result.unwrap().expect("local outgoing wire test timed out");
}
