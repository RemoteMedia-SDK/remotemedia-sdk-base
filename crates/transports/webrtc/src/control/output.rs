//! Retain session authority and payload owners until final control serialization.
use super::*;
use bytes::Bytes;
use prost::encoding::{encode_key, encode_varint, encoded_len_varint, key_len, WireType};
use remotemedia_types::CopyContext;

pub(super) async fn forward_taps(
    mut rx: tokio::sync::broadcast::Receiver<RuntimeData>,
    tx: mpsc::Sender<Outbound>,
    address: CoreAddress,
    copies: CopyContext,
) {
    let addr = core_addr_to_pb(&address);
    loop {
        let data = match rx.recv().await {
            Ok(data) => data,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n))
                if address.port.as_deref() == Some("__reply__") =>
            {
                let _ = tx
                    .send(error_event(
                        ControlErrorCode::Internal,
                        format!("RPC reply subscription lagged by {n} frames"),
                        Some(address),
                    ))
                    .await;
                break;
            }
            Err(_) => break,
        };
        if tx
            .send(Outbound::tap(addr.clone(), data, &copies))
            .await
            .is_err()
        {
            break;
        }
    }
}

pub(super) async fn forward_intercepts(
    mut rx: mpsc::Receiver<CoreEvent>,
    tx: mpsc::Sender<Outbound>,
    addr: PbAddress,
    copies: CopyContext,
) {
    while let Some(event) = rx.recv().await {
        if let CoreEvent::InterceptRequest {
            correlation_id,
            data,
            ..
        } = event
        {
            if tx
                .send(Outbound::intercept(
                    addr.clone(),
                    correlation_id,
                    data,
                    &copies,
                ))
                .await
                .is_err()
            {
                break;
            }
        }
    }
}

pub(super) enum PayloadHeader {
    Tap(TapEvent),
    Intercept(PbInterceptRequest),
}

pub(super) enum Outbound {
    Metadata(ControlEvent),
    Payload {
        header: PayloadHeader,
        data: RuntimeData,
        copies: CopyContext,
    },
}

impl From<ControlEvent> for Outbound {
    fn from(event: ControlEvent) -> Self {
        Self::Metadata(event)
    }
}

impl Outbound {
    pub(super) fn tap(addr: PbAddress, data: RuntimeData, copies: &CopyContext) -> Self {
        Self::Payload {
            header: PayloadHeader::Tap(TapEvent {
                addr: Some(addr),
                data: None,
            }),
            data: data.into_shared(),
            copies: copies.clone(),
        }
    }

    pub(super) fn intercept(
        addr: PbAddress,
        correlation_id: u64,
        data: RuntimeData,
        copies: &CopyContext,
    ) -> Self {
        Self::Payload {
            header: PayloadHeader::Intercept(PbInterceptRequest {
                addr: Some(addr),
                correlation_id,
                data: None,
            }),
            data: data.into_shared(),
            copies: copies.clone(),
        }
    }

    pub(super) fn encode(&self) -> crate::Result<Bytes> {
        match self {
            Self::Metadata(event) => {
                // A future caller must not evade attribution by putting a bulk
                // protobuf event through the metadata-only conversion.
                if matches!(
                    event.event,
                    Some(PbEvent::Tap(_) | PbEvent::InterceptRequest(_))
                ) {
                    return Err(crate::Error::InvalidData(
                        "control payload requires copy authority".into(),
                    ));
                }
                Ok(event.encode_to_vec().into())
            }
            Self::Payload {
                header,
                data,
                copies,
            } => {
                let header_has_data = match header {
                    PayloadHeader::Tap(header) => header.data.is_some(),
                    PayloadHeader::Intercept(header) => header.data.is_some(),
                };
                if header_has_data {
                    return Err(crate::Error::InvalidData(
                        "control header must not contain a payload".into(),
                    ));
                }
                let prepared = crate::adapters::PreparedRuntimeData::new(data, copies)
                    .map_err(|e| crate::Error::Other(e.into()))?;
                let (event_tag, data_tag, header_len) = match header {
                    PayloadHeader::Tap(header) => (2, 2, header.encoded_len()),
                    PayloadHeader::Intercept(header) => (3, 3, header.encoded_len()),
                };
                let data_len = prepared.encoded_len();
                let inner_len =
                    header_len + key_len(data_tag) + encoded_len_varint(data_len as u64) + data_len;
                let wire_len =
                    key_len(event_tag) + encoded_len_varint(inner_len as u64) + inner_len;
                let mut wire = Vec::with_capacity(wire_len);
                encode_key(event_tag, WireType::LengthDelimited, &mut wire);
                encode_varint(inner_len as u64, &mut wire);
                match header {
                    PayloadHeader::Tap(header) => header.encode(&mut wire),
                    PayloadHeader::Intercept(header) => header.encode(&mut wire),
                }
                .expect("sized control header destination");
                encode_key(data_tag, WireType::LengthDelimited, &mut wire);
                encode_varint(data_len as u64, &mut wire);
                prepared
                    .append_to(&mut wire)
                    .map_err(|e| crate::Error::Other(e.into()))?;
                debug_assert_eq!(wire.len(), wire_len);
                Ok(wire.into())
            }
        }
    }
}

#[cfg(test)]
#[path = "output_tests.rs"]
mod tests;
