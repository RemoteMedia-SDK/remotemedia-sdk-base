//! Task 6.4: a device tensor at the gRPC wire. Compatibility reports both the
//! DeviceTransfer to host and the PortableSerialization into protobuf; strict
//! rejects with the transfer named, before any device access or encoding.
//! Runs only with REMOTEMEDIA_CUDA_TESTS=1 and CUDA_VISIBLE_DEVICES pinned.
use remotemedia_core::data::{CopyContext, CopyPolicy, CopyReason, DlDtype, RuntimeData};
use remotemedia_device_cuda::CudaDevice;
use remotemedia_grpc::adapters::runtime_data_to_data_buffer_with_context;
use remotemedia_grpc::generated::data_buffer::DataType;

fn tensor() -> Option<RuntimeData> {
    if std::env::var("REMOTEMEDIA_CUDA_TESTS").as_deref() != Ok("1") {
        eprintln!("skipped: set REMOTEMEDIA_CUDA_TESTS=1 and CUDA_VISIBLE_DEVICES to run");
        return None;
    }
    assert!(std::env::var("CUDA_VISIBLE_DEVICES").is_ok());
    let gpu = CudaDevice::open(0).unwrap();
    let allocation = gpu.alloc(4096).unwrap();
    let stream = gpu.stream().unwrap();
    stream.fill_u32(&allocation, 0, 0xABCD, 1024).unwrap();
    let ready = stream.record().unwrap();
    Some(RuntimeData::DeviceTensor {
        buffer: allocation.into_buffer(),
        dtype: DlDtype::F32,
        shape: vec![32, 32],
        strides: None,
        byte_offset: 0,
        ready: Some(ready.into_ready()),
        metadata: None,
    })
}

fn counted(copies: &CopyContext, reason: CopyReason) -> (u64, u64, u64) {
    let counts = copies
        .report()
        .entries
        .into_iter()
        .find(|(r, _)| *r == reason)
        .unwrap()
        .1;
    (counts.completed, counts.copied_bytes, counts.rejected)
}

#[test]
fn compatibility_reports_the_transfer_and_the_serialization() {
    let Some(tensor) = tensor() else { return };
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "grpc-device");
    let buffer = runtime_data_to_data_buffer_with_context(&tensor, &copies).unwrap();
    let Some(DataType::Numpy(numpy)) = buffer.data_type else {
        panic!("a device tensor encodes as a host NumPy array")
    };
    assert_eq!(numpy.shape, vec![32, 32]);
    assert_eq!(numpy.data.len(), 4096);
    assert!(numpy
        .data
        .chunks_exact(4)
        .all(|w| u32::from_le_bytes(w.try_into().unwrap()) == 0xABCD));
    assert_eq!(counted(&copies, CopyReason::DeviceTransfer), (1, 4096, 0));
    assert_eq!(
        counted(&copies, CopyReason::PortableSerialization),
        (1, 4096, 0)
    );
}

#[test]
fn strict_rejects_the_device_transfer_by_name() {
    let Some(tensor) = tensor() else { return };
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "grpc-device-strict");
    let error = runtime_data_to_data_buffer_with_context(&tensor, &strict)
        .err()
        .expect("strict must reject");
    assert!(error.to_string().contains("DeviceTransfer"), "{error}");
    assert_eq!(counted(&strict, CopyReason::DeviceTransfer), (0, 0, 1));
    assert_eq!(
        counted(&strict, CopyReason::PortableSerialization),
        (0, 0, 0)
    );
}
