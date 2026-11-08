use super::*;
use remotemedia_types::{BufferBudget, RuntimeData};

#[test]
fn observer_bridge_conversion_and_ffi_fanout_retain_payload_owners() {
    for raw in [false, true] {
        let budget = BufferBudget::new(1, 16);
        let RuntimeData::Binary(source) = budget
            .admit(RuntimeData::Binary(vec![5; 16].into()))
            .unwrap()
        else {
            unreachable!()
        };
        let pointer = source.as_ptr();
        let bridge = if raw {
            WebRtcEventBridge::data_received("peer".into(), source, 42)
        } else {
            WebRtcEventBridge::pipeline_output("peer".into(), RuntimeData::Binary(source), 42)
        };
        let event = convert_bridge_to_ffi_event(&bridge);
        let clone = event.clone();
        drop((bridge, event));
        let bytes = match clone {
            WebRtcEvent::DataReceived(event) => {
                assert_eq!(event.timestamp, 42);
                event.data
            }
            WebRtcEvent::PipelineOutput(event) => {
                assert_eq!(event.timestamp, 42);
                let RuntimeData::Binary(bytes) = event.data else {
                    unreachable!()
                };
                bytes
            }
            _ => unreachable!(),
        };
        assert_eq!(bytes.as_ptr(), pointer);
        let retained = bytes.slice(2..5).unwrap();
        drop(bytes);
        assert_eq!(budget.usage().owners, 1);
        assert_eq!(&*retained, &[5; 3]);
        std::thread::spawn(move || drop(retained)).join().unwrap();
        assert_eq!(budget.usage().owners, 0);
    }
}
