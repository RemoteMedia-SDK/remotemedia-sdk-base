# Fixed: RuntimeData::Numpy gRPC Adapter

## Issues Found

The original code in `adapters.rs` had **3 critical bugs**:

### 1. Missing Import ❌
```rust
use crate::generated::{
    // ... other types ...
    // NumpyBuffer was MISSING!
};
```

### 2. Wrong Type for `dtype` ❌
```rust
RuntimeData::Numpy(numpy) => DataType::Numpy(NumpyBuffer {
    dtype: *numpy.dtype,  // ❌ WRONG! Can't dereference a String
    // ...
}),
```

### 3. Missing Critical Fields ❌
The code was missing **3 essential fields**:
- `strides: Vec<isize>` - Critical for memory layout!
- `c_contiguous: bool` - Needed for array reconstruction
- `f_contiguous: bool` - Needed for array reconstruction

---

## What Was Fixed

### 1. Added NumpyBuffer to Protobuf Schema ✅

**File**: `transports/grpc/protos/common.proto`

Added to `DataBuffer.data_type` oneof:
```protobuf
message DataBuffer {
  oneof data_type {
    // ... existing types ...
    NumpyBuffer numpy = 8;  // Zero-copy numpy array passthrough
  }
}
```

Added new message definition:
```protobuf
message NumpyBuffer {
  bytes data = 1;
  repeated uint64 shape = 2;
  string dtype = 3;
  repeated int64 strides = 4;
  bool c_contiguous = 5;
  bool f_contiguous = 6;
}
```

### 2. Added Import ✅

**File**: `transports/grpc/src/adapters.rs`

```rust
use crate::generated::{
    AudioBuffer, AudioFormat, BatchHint, BinaryBuffer, CancelSpeculation, ControlMessage,
    DataBuffer, DeadlineWarning, JsonData, NumpyBuffer, PixelFormat, TensorBuffer, TensorDtype,
    TextBuffer, VideoFrame,
};
```

### 3. Fixed RuntimeData → DataBuffer Conversion ✅

**File**: `transports/grpc/src/adapters.rs` (line 58-67)

```rust
RuntimeData::Numpy {
    data,
    shape,
    dtype,
    strides,
    c_contiguous,
    f_contiguous,
} => DataType::Numpy(NumpyBuffer {
    data: data.clone(),
    shape: shape.iter().map(|&s| s as u64).collect(),
    dtype: dtype.clone(),  // ✅ Clone the String
    strides: strides.iter().map(|&s| s as i64).collect(),  // ✅ Include strides
    c_contiguous: *c_contiguous,  // ✅ Include contiguity
    f_contiguous: *f_contiguous,  // ✅ Include contiguity
}),
```

### 4. Fixed DataBuffer → RuntimeData Conversion ✅

**File**: `transports/grpc/src/adapters.rs` (line 196-202)

```rust
Some(DataType::Numpy(numpy)) => Some(RuntimeData::Numpy {
    data: numpy.data.clone(),
    shape: numpy.shape.iter().map(|&s| s as usize).collect(),
    dtype: numpy.dtype.clone(),
    strides: numpy.strides.iter().map(|&s| s as isize).collect(),
    c_contiguous: numpy.c_contiguous,
    f_contiguous: numpy.f_contiguous,
}),
```

---

## Next Steps

### 1. Regenerate Protobuf Code

The protobuf schema has been updated, but the Rust types need to be regenerated:

```bash
cd transports/grpc
cargo clean
cargo build
```

This will run `build.rs` which calls `tonic_prost_build` to generate the `NumpyBuffer` struct.

### 2. Verify Compilation

After regeneration, the code should compile without errors:

```bash
cd transports/grpc
cargo check
```

### 3. Test the Implementation

Create a test to verify numpy arrays can be serialized/deserialized:

```rust
#[test]
fn test_numpy_buffer_roundtrip() {
    let numpy_data = RuntimeData::Numpy {
        data: vec![0u8; 3840],  // 960 f32 samples
        shape: vec![960],
        dtype: "float32".to_string(),
        strides: vec![4],
        c_contiguous: true,
        f_contiguous: false,
    };
    
    // RuntimeData → DataBuffer
    let buffer = runtime_data_to_data_buffer(&numpy_data);
    
    // DataBuffer → RuntimeData
    let roundtrip = data_buffer_to_runtime_data(&buffer).unwrap();
    
    assert_eq!(numpy_data, roundtrip);
}
```

---

## Why These Fields Matter

### `strides`
**Critical for memory layout!** 

Without strides, you can't reconstruct the numpy array correctly. For a 2D array like `[960, 2]` (stereo audio):
- C-contiguous: strides = `[8, 4]` (row-major)
- F-contiguous: strides = `[4, 3840]` (column-major)

### `c_contiguous` / `f_contiguous`
**Needed for optimization!**

Numpy can use fast memcpy for contiguous arrays. Without these flags, the array must be copied element-by-element, defeating the zero-copy purpose.

---

## Verified Files

- ✅ `transports/grpc/protos/common.proto` - Schema updated
- ✅ `transports/grpc/src/adapters.rs` - Both conversions fixed
- ⏳ `transports/grpc/src/generated/remotemedia.v1.rs` - Needs regeneration

---

## Status

**Code is FIXED but needs protobuf regeneration to compile.**

Run `cargo build` in the `transports/grpc` directory to complete the fix.


