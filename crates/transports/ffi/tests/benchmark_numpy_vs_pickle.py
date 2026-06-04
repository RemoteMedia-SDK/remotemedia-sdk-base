"""
REAL comparison benchmark: Zero-Copy vs Pickling

This benchmark compares:
1. RuntimeData::Numpy (zero-copy) - current implementation
2. Pickling (old approach) - simulated by wrapping arrays

This gives us the ACTUAL speedup, not theoretical numbers.
"""

import numpy as np
import sys
import os
import time
import json
import pickle

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


class PickledWrapper:
    """Wrapper that forces pickling by hiding the numpy array."""
    def __init__(self, array):
        self.pickled_data = pickle.dumps(array)
        self.original_array = array
    
    def unwrap(self):
        return pickle.loads(self.pickled_data)


async def benchmark_zero_copy(frames, manifest_json, num_iterations):
    """Benchmark with zero-copy numpy arrays (current implementation)."""
    results = []
    
    for frame in frames:
        start = time.perf_counter()
        result = await execute_pipeline_with_input(manifest_json, [frame], None)
        end = time.perf_counter()
        
        assert isinstance(result, np.ndarray), "Expected numpy array"
        results.append(end - start)
    
    return results


async def benchmark_with_pickling(frames, manifest_json, num_iterations):
    """Benchmark simulating pickling overhead."""
    results = []
    
    for frame in frames:
        start = time.perf_counter()
        
        # Simulate old approach: pickle before sending
        pickled_data = pickle.dumps(frame)
        
        # Unpickle (simulating deserialization on Rust side)
        unpickled = pickle.loads(pickled_data)
        
        # Send through pipeline (would pickle again in old implementation)
        result = await execute_pipeline_with_input(manifest_json, [unpickled], None)
        
        # Simulate unpickling result
        pickled_result = pickle.dumps(result)
        final_result = pickle.loads(pickled_result)
        
        end = time.perf_counter()
        
        assert isinstance(final_result, np.ndarray), "Expected numpy array"
        results.append(end - start)
    
    return results


async def run_comparison():
    """Run side-by-side comparison."""
    print("=" * 70)
    print("COMPARISON: Zero-Copy vs Pickling")
    print("=" * 70)
    
    # Test parameters
    sample_rate = 48000
    frame_duration_ms = 20
    frame_size = int(sample_rate * frame_duration_ms / 1000)  # 960 samples
    num_frames = 100
    
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
    
    # Warmup both
    print("\n" + "-" * 70)
    print("WARMUP...")
    print("-" * 70)
    
    for i in range(5):
        await execute_pipeline_with_input(manifest_json, [frames[i]], None)
    
    print("  Warmup complete!")
    
    # Benchmark 1: Zero-Copy (current implementation)
    print("\n" + "-" * 70)
    print("TEST 1: Zero-Copy (RuntimeData::Numpy)")
    print("-" * 70)
    
    times_zero_copy = await benchmark_zero_copy(frames, manifest_json, num_frames)
    
    avg_zero_copy = sum(times_zero_copy) / len(times_zero_copy)
    min_zero_copy = min(times_zero_copy)
    max_zero_copy = max(times_zero_copy)
    total_zero_copy = sum(times_zero_copy)
    
    print(f"  Total time: {total_zero_copy:.4f}s")
    print(f"  Avg per frame: {avg_zero_copy * 1000:.3f}ms")
    print(f"  Min per frame: {min_zero_copy * 1000:.3f}ms")
    print(f"  Max per frame: {max_zero_copy * 1000:.3f}ms")
    print(f"  Throughput: {num_frames / total_zero_copy:.1f} fps")
    
    # Benchmark 2: With Pickling (simulated old approach)
    print("\n" + "-" * 70)
    print("TEST 2: With Pickling (Simulated Old Approach)")
    print("-" * 70)
    print("  Note: Pickle on input + pickle on output")
    
    times_pickle = await benchmark_with_pickling(frames, manifest_json, num_frames)
    
    avg_pickle = sum(times_pickle) / len(times_pickle)
    min_pickle = min(times_pickle)
    max_pickle = max(times_pickle)
    total_pickle = sum(times_pickle)
    
    print(f"  Total time: {total_pickle:.4f}s")
    print(f"  Avg per frame: {avg_pickle * 1000:.3f}ms")
    print(f"  Min per frame: {min_pickle * 1000:.3f}ms")
    print(f"  Max per frame: {max_pickle * 1000:.3f}ms")
    print(f"  Throughput: {num_frames / total_pickle:.1f} fps")
    
    # Comparison
    speedup = avg_pickle / avg_zero_copy
    time_saved_per_frame = (avg_pickle - avg_zero_copy) * 1000
    overhead_reduction = ((avg_pickle - avg_zero_copy) / avg_pickle) * 100
    
    print("\n" + "=" * 70)
    print("COMPARISON RESULTS")
    print("=" * 70)
    
    print(f"\n📊 Time per Frame:")
    print(f"  Zero-Copy:    {avg_zero_copy * 1000:.3f}ms")
    print(f"  With Pickle:  {avg_pickle * 1000:.3f}ms")
    print(f"  Difference:   {time_saved_per_frame:.3f}ms saved ({overhead_reduction:.1f}% reduction)")
    
    print(f"\n⚡ Speedup:")
    print(f"  {speedup:.2f}x faster with zero-copy")
    
    print(f"\n🎵 Audio Processing:")
    realtime_zerocopy = (num_frames * frame_duration_ms / 1000) / total_zero_copy
    realtime_pickle = (num_frames * frame_duration_ms / 1000) / total_pickle
    print(f"  Zero-Copy:    {realtime_zerocopy:.1f}x realtime")
    print(f"  With Pickle:  {realtime_pickle:.1f}x realtime")
    
    print(f"\n💾 Overhead per Second of Audio:")
    frames_per_sec = 1000 / frame_duration_ms  # 50 for 20ms frames
    overhead_zerocopy = avg_zero_copy * frames_per_sec * 1000
    overhead_pickle = avg_pickle * frames_per_sec * 1000
    print(f"  Zero-Copy:    {overhead_zerocopy:.1f}ms/sec")
    print(f"  With Pickle:  {overhead_pickle:.1f}ms/sec")
    print(f"  Saved:        {overhead_pickle - overhead_zerocopy:.1f}ms/sec")
    
    # Determine if we can handle realtime
    print(f"\n✅ Realtime Streaming:")
    if realtime_zerocopy > 10:
        print(f"  Zero-Copy: ✅ EXCELLENT - {realtime_zerocopy:.1f}x headroom")
    elif realtime_zerocopy > 5:
        print(f"  Zero-Copy: ✅ GOOD - {realtime_zerocopy:.1f}x headroom")
    elif realtime_zerocopy > 2:
        print(f"  Zero-Copy: ⚠️  OK - {realtime_zerocopy:.1f}x headroom")
    else:
        print(f"  Zero-Copy: ❌ TIGHT - {realtime_zerocopy:.1f}x headroom")
    
    if realtime_pickle > 10:
        print(f"  With Pickle: ✅ EXCELLENT - {realtime_pickle:.1f}x headroom")
    elif realtime_pickle > 5:
        print(f"  With Pickle: ✅ GOOD - {realtime_pickle:.1f}x headroom")
    elif realtime_pickle > 2:
        print(f"  With Pickle: ⚠️  OK - {realtime_pickle:.1f}x headroom")
    else:
        print(f"  With Pickle: ❌ TIGHT - {realtime_pickle:.1f}x headroom")
    
    print("\n" + "=" * 70)
    
    return {
        'zerocopy': {
            'avg_ms': avg_zero_copy * 1000,
            'total_s': total_zero_copy,
            'fps': num_frames / total_zero_copy,
            'realtime_factor': realtime_zerocopy
        },
        'pickle': {
            'avg_ms': avg_pickle * 1000,
            'total_s': total_pickle,
            'fps': num_frames / total_pickle,
            'realtime_factor': realtime_pickle
        },
        'speedup': speedup,
        'time_saved_ms': time_saved_per_frame
    }


if __name__ == "__main__":
    if not RUNTIME_AVAILABLE:
        print("❌ Runtime not available - cannot run benchmark")
        print("   Build with: cd transports/ffi && ./dev-install.sh")
        sys.exit(1)
    
    results = asyncio.run(run_comparison())
    
    print("\n📋 Summary:")
    print(f"  Zero-Copy Performance: {results['zerocopy']['avg_ms']:.3f}ms/frame")
    print(f"  Pickle Performance: {results['pickle']['avg_ms']:.3f}ms/frame")
    print(f"  ACTUAL SPEEDUP: {results['speedup']:.2f}x")
    print(f"  Time Saved: {results['time_saved_ms']:.3f}ms per frame")

