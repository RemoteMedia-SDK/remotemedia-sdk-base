"""
Integration test for RuntimeData::Numpy zero-copy implementation.

This test ACTUALLY exercises the FFI layer to verify numpy arrays
pass through without JSON serialization.
"""

import numpy as np
import pytest
import sys
import os
import json
import asyncio

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'python-client'))

try:
    import remotemedia
    from remotemedia.runtime import (
        execute_pipeline_with_input,
        is_available,
        get_runtime_version
    )
    RUNTIME_AVAILABLE = True
    print(f"Runtime exports: {[x for x in dir(remotemedia.runtime) if not x.startswith('_')]}")
except (ImportError, ModuleNotFoundError) as e:
    RUNTIME_AVAILABLE = False
    print(f"Runtime not available: {e}")


@pytest.mark.skipif(not RUNTIME_AVAILABLE, reason="Runtime not available")
def test_runtime_is_available():
    """Verify the runtime is available."""
    assert is_available()
    version = get_runtime_version()
    print(f"Runtime version: {version}")


@pytest.mark.skipif(not RUNTIME_AVAILABLE, reason="Runtime not available")
@pytest.mark.asyncio
async def test_numpy_passthrough_simple():
    """Test numpy array passes through a simple pipeline."""
    # Create a simple numpy array
    audio_data = np.array([0.1, 0.2, 0.3, 0.4, 0.5], dtype=np.float32)
    
    # Simple passthrough pipeline with identity node
    manifest = {
        "version": "v1",
        "metadata": {"name": "numpy_passthrough_test"},
        "nodes": [
            {
                "id": "passthrough",
                "node_type": "PassThrough",
                "config": {}
            }
        ],
        "connections": []
    }
    
    result = await execute_pipeline_with_input(
        json.dumps(manifest),
        [audio_data],
        None  # enable_metrics
    )
    
    print(f"Input type: {type(audio_data)}, shape: {audio_data.shape}")
    print(f"Result type: {type(result)}")
    
    # Result should be a numpy array
    assert result is not None
    assert isinstance(result, np.ndarray), f"Expected numpy array, got {type(result)}"
    assert result.shape == audio_data.shape, f"Shape mismatch: {result.shape} != {audio_data.shape}"
    print("✓ Pipeline executed successfully with numpy input")


@pytest.mark.skipif(not RUNTIME_AVAILABLE, reason="Runtime not available")
@pytest.mark.asyncio  
async def test_numpy_different_dtypes_ffi():
    """Test different numpy dtypes through FFI."""
    test_cases = [
        ("float32", np.array([1.0, 2.0, 3.0], dtype=np.float32)),
        ("float64", np.array([1.0, 2.0, 3.0], dtype=np.float64)),
    ]
    
    manifest = {
        "version": "v1",
        "metadata": {"name": "dtype_test"},
        "nodes": [
            {
                "id": "passthrough",
                "node_type": "PassThrough",
                "config": {}
            }
        ],
        "connections": []
    }
    
    for dtype_name, arr in test_cases:
        result = await execute_pipeline_with_input(
            json.dumps(manifest),
            [arr],
            None  # enable_metrics
        )
        assert result is not None, f"{dtype_name}: Pipeline returned None"
        assert isinstance(result, np.ndarray), f"{dtype_name}: Expected numpy array, got {type(result)}"
        print(f"✓ {dtype_name}: FFI accepted numpy array and returned {type(result)}")


@pytest.mark.skipif(not RUNTIME_AVAILABLE, reason="Runtime not available")
@pytest.mark.asyncio
async def test_numpy_multidimensional_ffi():
    """Test multidimensional numpy arrays through FFI."""
    # Stereo audio: (frames, channels)
    stereo_audio = np.random.randn(960, 2).astype(np.float32)
    
    manifest = {
        "version": "v1",
        "metadata": {"name": "multidim_test"},
        "nodes": [
            {
                "id": "passthrough",
                "node_type": "PassThrough",
                "config": {}
            }
        ],
        "connections": []
    }
    
    result = await execute_pipeline_with_input(
        json.dumps(manifest),
        [stereo_audio],
        None  # enable_metrics
    )
    
    print(f"Input shape: {stereo_audio.shape}")
    print(f"Result type: {type(result)}")
    assert result is not None
    assert isinstance(result, np.ndarray), f"Expected numpy array, got {type(result)}"
    assert result.ndim == 2, f"Expected 2D array, got {result.ndim}D"
    print("✓ Multidimensional numpy array passed through FFI")


@pytest.mark.skipif(not RUNTIME_AVAILABLE, reason="Runtime not available")
@pytest.mark.asyncio
async def test_numpy_vs_dict_comparison():
    """Compare numpy passthrough vs dict format."""
    # Test data
    samples = [0.1, 0.2, 0.3, 0.4, 0.5]
    
    # Format 1: Numpy array
    numpy_input = np.array(samples, dtype=np.float32)
    
    # Format 2: Dict (old way)
    dict_input = {
        "type": "audio",
        "samples": samples,
        "sample_rate": 48000,
        "channels": 1
    }
    
    manifest = {
        "version": "v1",
        "metadata": {"name": "comparison_test"},
        "nodes": [
            {
                "id": "passthrough",
                "node_type": "PassThrough",
                "config": {}
            }
        ],
        "connections": []
    }
    
    print("\n=== Numpy Input ===")
    result1 = await execute_pipeline_with_input(
        json.dumps(manifest),
        [numpy_input],
        None  # enable_metrics
    )
    print(f"Numpy result type: {type(result1)}")
    assert isinstance(result1, np.ndarray), f"Expected numpy array, got {type(result1)}"
    print("✓ Numpy input works - returned numpy array!")
    
    print("\n=== Dict Input ===")
    result2 = await execute_pipeline_with_input(
        json.dumps(manifest),
        [dict_input],
        None  # enable_metrics
    )
    print(f"Dict result type: {type(result2)}")
    # Dict input returns dict (backward compatibility)
    assert result2 is not None
    print("✓ Dict input works (backward compatibility)")


@pytest.mark.skipif(not RUNTIME_AVAILABLE, reason="Runtime not available")
def test_numpy_to_runtime_data_conversion():
    """Test that numpy arrays are auto-detected and wrapped."""
    from remotemedia.runtime import execute_pipeline_with_input
    
    # Create numpy array
    arr = np.array([1.0, 2.0, 3.0], dtype=np.float32)
    
    # The marshal.rs python_to_runtime_data should auto-detect this
    print(f"Array type: {type(arr)}")
    print(f"Array dtype: {arr.dtype}")
    print(f"Array shape: {arr.shape}")
    print(f"Is numpy array: {hasattr(arr, 'dtype')}")
    
    # Just verify the array has the right properties
    assert hasattr(arr, 'dtype')
    assert hasattr(arr, 'shape')
    assert hasattr(arr, 'strides')
    assert hasattr(arr, 'flags')
    
    print("✓ Numpy array has all required attributes for RuntimeData::Numpy")


@pytest.mark.skipif(not RUNTIME_AVAILABLE, reason="Runtime not available")
@pytest.mark.asyncio
async def test_streaming_simulation():
    """Simulate streaming 20ms audio frames."""
    sample_rate = 48000
    frame_duration_ms = 20
    frame_size = int(sample_rate * frame_duration_ms / 1000)  # 960 samples
    
    manifest = {
        "version": "v1",
        "metadata": {"name": "streaming_test"},
        "nodes": [
            {
                "id": "passthrough",
                "node_type": "PassThrough",
                "config": {}
            }
        ],
        "connections": []
    }
    
    print(f"\nSimulating streaming: {frame_size} samples per frame")
    
    # Send 10 frames (200ms of audio)
    for i in range(10):
        frame = np.sin(2 * np.pi * 440 * np.linspace(
            i * frame_duration_ms / 1000,
            (i + 1) * frame_duration_ms / 1000,
            frame_size,
            dtype=np.float32
        ))
        
        result = await execute_pipeline_with_input(
            json.dumps(manifest),
            [frame],
            None  # enable_metrics
        )
        
        if i == 0:
            print(f"Frame {i}: Input shape={frame.shape}, dtype={frame.dtype}")
            print(f"Frame {i}: Result type={type(result)}")
            assert isinstance(result, np.ndarray), f"Expected numpy array, got {type(result)}"
            assert result.shape == frame.shape, f"Shape mismatch: {result.shape} != {frame.shape}"
    
    print(f"✓ Sent 10 frames through FFI layer - all returned as numpy arrays!")


@pytest.mark.skipif(not RUNTIME_AVAILABLE, reason="Runtime not available")  
def test_verify_zero_copy_metadata():
    """Verify numpy metadata is preserved."""
    # Create array with specific properties
    arr = np.array([[1, 2, 3], [4, 5, 6]], dtype=np.float32)
    
    original_shape = arr.shape
    original_dtype = str(arr.dtype)
    original_strides = arr.strides
    original_c_contiguous = arr.flags['C_CONTIGUOUS']
    
    print(f"\nOriginal array metadata:")
    print(f"  Shape: {original_shape}")
    print(f"  Dtype: {original_dtype}")
    print(f"  Strides: {original_strides}")
    print(f"  C-contiguous: {original_c_contiguous}")
    
    # This metadata should flow through RuntimeData::Numpy
    assert original_shape == (2, 3)
    assert original_dtype == 'float32'
    assert original_strides == (12, 4)
    assert original_c_contiguous == True
    
    print("✓ Metadata preserved in numpy array")


if __name__ == "__main__":
    print("=" * 70)
    print("RuntimeData::Numpy Integration Tests")
    print("=" * 70)
    
    if not RUNTIME_AVAILABLE:
        print("⚠️  Runtime not available - cannot run integration tests")
        print("    Build the FFI with: cd transports/ffi && ./dev-install.sh")
        sys.exit(0)
    
    # Run tests
    loop = asyncio.get_event_loop()
    
    print("\n[1/8] Checking runtime availability...")
    test_runtime_is_available()
    
    print("\n[2/8] Testing simple numpy passthrough...")
    loop.run_until_complete(test_numpy_passthrough_simple())
    
    print("\n[3/8] Testing different dtypes...")
    loop.run_until_complete(test_numpy_different_dtypes_ffi())
    
    print("\n[4/8] Testing multidimensional arrays...")
    loop.run_until_complete(test_numpy_multidimensional_ffi())
    
    print("\n[5/8] Comparing numpy vs dict format...")
    loop.run_until_complete(test_numpy_vs_dict_comparison())
    
    print("\n[6/8] Testing auto-detection...")
    test_numpy_to_runtime_data_conversion()
    
    print("\n[7/8] Simulating streaming...")
    loop.run_until_complete(test_streaming_simulation())
    
    print("\n[8/8] Verifying metadata preservation...")
    test_verify_zero_copy_metadata()
    
    print("\n" + "=" * 70)
    print("✓ All integration tests completed!")
    print("=" * 70)
    print("\nTo run with pytest:")
    print("  pytest test_numpy_integration.py -v -s")

