"""
REAL performance benchmark for RuntimeData::Numpy zero-copy implementation.

This actually MEASURES performance, not just theoretical calculations.
"""

import numpy as np
import sys
import os
import time
import json

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'python-client'))

try:
    import remotemedia
    from remotemedia.runtime import execute_pipeline_with_input
    RUNTIME_AVAILABLE = True
except (ImportError, ModuleNotFoundError) as e:
    RUNTIME_AVAILABLE = False
    print(f"Runtime not available: {e}")
    sys.exit(1)


import asyncio

async def benchmark_numpy_throughput():
    """Measure actual throughput of numpy array processing."""
    print("=" * 70)
    print("REAL BENCHMARK: Numpy Array Throughput")
    print("=" * 70)
    
    # Test parameters
    sample_rate = 48000
    frame_duration_ms = 20
    frame_size = int(sample_rate * frame_duration_ms / 1000)  # 960 samples
    num_frames = 100  # Process 2 seconds of audio
    
    manifest = {
        "version": "v1",
        "metadata": {"name": "benchmark_test"},
        "nodes": [
            {
                "id": "passthrough",
                "node_type": "PassThrough",
                "config": {}
            }
        ],
        "connections": []
    }
    manifest_json = json.dumps(manifest)
    
    print(f"\nTest Setup:")
    print(f"  Frame size: {frame_size} samples ({frame_duration_ms}ms @ {sample_rate}Hz)")
    print(f"  Number of frames: {num_frames}")
    print(f"  Total audio: {num_frames * frame_duration_ms / 1000:.1f} seconds")
    print(f"  Data per frame: {frame_size * 4} bytes (float32)")
    
    # Generate test data
    frames = [
        np.sin(2 * np.pi * 440 * np.linspace(
            i * frame_duration_ms / 1000,
            (i + 1) * frame_duration_ms / 1000,
            frame_size,
            dtype=np.float32
        ))
        for i in range(num_frames)
    ]
    
    print("\n" + "-" * 70)
    print("WARMUP: Running 10 frames...")
    print("-" * 70)
    
    # Warmup
    for i in range(10):
        result = await execute_pipeline_with_input(manifest_json, [frames[i]], None)
        
        if i == 0:
            print(f"  First result type: {type(result)}")
            print(f"  First result shape: {result.shape if hasattr(result, 'shape') else 'N/A'}")
    
    print("  Warmup complete!")
    
    print("\n" + "-" * 70)
    print(f"BENCHMARK: Processing {num_frames} frames...")
    print("-" * 70)
    
    # Actual benchmark
    start_time = time.perf_counter()
    
    for i, frame in enumerate(frames):
        result = await execute_pipeline_with_input(manifest_json, [frame], None)
        
        # Verify result
        assert isinstance(result, np.ndarray), f"Frame {i}: Expected numpy array"
        assert result.shape == frame.shape, f"Frame {i}: Shape mismatch"
    
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    
    # Calculate metrics
    frames_per_second = num_frames / elapsed_time
    audio_realtime_factor = (num_frames * frame_duration_ms / 1000) / elapsed_time
    latency_per_frame_ms = (elapsed_time / num_frames) * 1000
    throughput_mb_per_sec = (num_frames * frame_size * 4) / (elapsed_time * 1024 * 1024)
    
    print("\n" + "=" * 70)
    print("RESULTS")
    print("=" * 70)
    print(f"\n⏱️  Timing:")
    print(f"  Total time: {elapsed_time:.3f} seconds")
    print(f"  Avg time per frame: {latency_per_frame_ms:.3f} ms")
    
    print(f"\n📊 Throughput:")
    print(f"  Frames per second: {frames_per_second:.1f} fps")
    print(f"  Data throughput: {throughput_mb_per_sec:.2f} MB/s")
    
    print(f"\n🎵 Audio Performance:")
    print(f"  Realtime factor: {audio_realtime_factor:.1f}x")
    print(f"  (Can process {audio_realtime_factor:.1f}x faster than realtime)")
    
    print(f"\n✅ Verification:")
    print(f"  All {num_frames} frames returned as numpy.ndarray")
    print(f"  All shapes preserved correctly")
    print(f"  Zero pickle events (check logs)")
    
    # Performance expectations
    print(f"\n📈 Performance Analysis:")
    if latency_per_frame_ms < 1.0:
        print(f"  ✅ EXCELLENT: {latency_per_frame_ms:.3f}ms per frame")
    elif latency_per_frame_ms < 5.0:
        print(f"  ✅ GOOD: {latency_per_frame_ms:.3f}ms per frame")
    elif latency_per_frame_ms < 10.0:
        print(f"  ⚠️  OK: {latency_per_frame_ms:.3f}ms per frame")
    else:
        print(f"  ❌ SLOW: {latency_per_frame_ms:.3f}ms per frame")
    
    if audio_realtime_factor > 100:
        print(f"  ✅ Can easily handle realtime streaming")
    elif audio_realtime_factor > 10:
        print(f"  ✅ Can handle realtime streaming")
    elif audio_realtime_factor > 1:
        print(f"  ⚠️  Barely realtime")
    else:
        print(f"  ❌ Cannot keep up with realtime")
    
    print("\n" + "=" * 70)
    
    return {
        'elapsed_time': elapsed_time,
        'frames_per_second': frames_per_second,
        'latency_per_frame_ms': latency_per_frame_ms,
        'throughput_mb_per_sec': throughput_mb_per_sec,
        'realtime_factor': audio_realtime_factor
    }


if __name__ == "__main__":
    if not RUNTIME_AVAILABLE:
        print("❌ Runtime not available - cannot run benchmark")
        print("   Build with: cd transports/ffi && ./dev-install.sh")
        sys.exit(1)
    
    results = asyncio.run(benchmark_numpy_throughput())
    
    print("\n💾 Raw Results (for comparison):")
    print(f"  elapsed_time: {results['elapsed_time']:.6f}s")
    print(f"  latency_per_frame_ms: {results['latency_per_frame_ms']:.6f}ms")
    print(f"  frames_per_second: {results['frames_per_second']:.2f}")
    print(f"  throughput_mb_per_sec: {results['throughput_mb_per_sec']:.4f}")
    print(f"  realtime_factor: {results['realtime_factor']:.2f}x")

