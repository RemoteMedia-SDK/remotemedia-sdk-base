#!/usr/bin/env node
/**
 * Zero-copy benchmark for napi buffer implementation
 *
 * This benchmark verifies:
 * 1. Zero-copy is actually working (mutation test)
 * 2. Performance comparison: zero-copy vs copy
 * 3. Memory overhead comparison
 */

const { performance } = require('perf_hooks');

let native;
try {
  native = require('./index.js');
} catch (e) {
  console.error('Failed to load native binding:', e.message);
  console.error('Build the native addon first: cd transports/ffi && cargo build --release');
  process.exit(1);
}

const { NapiRuntimeData, AudioBuffer, VideoFrame } = native;

// Test parameters
const AUDIO_SAMPLE_COUNT = 48000 * 10; // 10 seconds of 48kHz audio
const VIDEO_WIDTH = 1920;
const VIDEO_HEIGHT = 1080;
const VIDEO_BYTES = VIDEO_WIDTH * VIDEO_HEIGHT * 3; // RGB24
const ITERATIONS = 1000;

console.log('='.repeat(60));
console.log('Zero-Copy Buffer Benchmark');
console.log('='.repeat(60));
console.log();

// Helper: Create f32 audio samples as Buffer
function createAudioBuffer(numSamples) {
  const buffer = Buffer.alloc(numSamples * 4);
  for (let i = 0; i < numSamples; i++) {
    buffer.writeFloatLE(Math.sin(i * 0.01), i * 4);
  }
  return buffer;
}

// Helper: Create video pixel data
function createVideoBuffer(width, height) {
  const buffer = Buffer.alloc(width * height * 3);
  for (let i = 0; i < buffer.length; i++) {
    buffer[i] = i % 256;
  }
  return buffer;
}

// ============================================
// Test 1: Verify zero-copy with mutation test
// ============================================
console.log('Test 1: Zero-copy verification (mutation test)');
console.log('-'.repeat(40));

try {
  // Create audio data
  const audioBytes = createAudioBuffer(100);
  const originalValue = audioBytes.readFloatLE(0);

  // Create NapiRuntimeData with audio
  const runtimeData = NapiRuntimeData.audio(audioBytes, 48000, 1);

  // Get the buffer (should be zero-copy)
  const buffer1 = runtimeData.getAudioSamples();
  const buffer2 = runtimeData.getAudioSamples();

  // Check if both buffers point to the same memory
  const samples1 = new Float32Array(buffer1.buffer, buffer1.byteOffset, 100);
  const samples2 = new Float32Array(buffer2.buffer, buffer2.byteOffset, 100);

  console.log(`  Original sample[0]: ${samples1[0].toFixed(6)}`);

  // Mutate via buffer1
  samples1[0] = 999.0;

  // Check if buffer2 sees the mutation (same memory = zero-copy)
  const isZeroCopy = samples2[0] === 999.0;

  console.log(`  After mutation via buffer1, buffer2[0]: ${samples2[0].toFixed(6)}`);
  console.log(`  Zero-copy verified: ${isZeroCopy ? 'YES ✓' : 'NO ✗ (data was copied)'}`);
  console.log();

  if (!isZeroCopy) {
    console.log('  WARNING: Buffer data is being copied, not zero-copy!');
    console.log('  This may be expected in Electron or other restricted runtimes.');
  }
} catch (e) {
  console.log(`  Error: ${e.message}`);
}

// ============================================
// Test 2: Performance benchmark - Audio
// ============================================
console.log('Test 2: Audio buffer access performance');
console.log('-'.repeat(40));
console.log(`  Sample count: ${AUDIO_SAMPLE_COUNT.toLocaleString()} (${(AUDIO_SAMPLE_COUNT / 48000).toFixed(1)}s @ 48kHz)`);
console.log(`  Buffer size: ${((AUDIO_SAMPLE_COUNT * 4) / 1024 / 1024).toFixed(2)} MB`);
console.log(`  Iterations: ${ITERATIONS.toLocaleString()}`);
console.log();

try {
  const audioBytes = createAudioBuffer(AUDIO_SAMPLE_COUNT);
  const runtimeData = NapiRuntimeData.audio(audioBytes, 48000, 1);

  // Warm up
  for (let i = 0; i < 10; i++) {
    const buf = runtimeData.getAudioSamples();
    const samples = new Float32Array(buf.buffer, buf.byteOffset, AUDIO_SAMPLE_COUNT);
    void samples[0]; // Access to prevent optimization
  }

  // Benchmark: Get buffer + create Float32Array view
  const start = performance.now();
  for (let i = 0; i < ITERATIONS; i++) {
    const buf = runtimeData.getAudioSamples();
    const samples = new Float32Array(buf.buffer, buf.byteOffset, AUDIO_SAMPLE_COUNT);
    void samples[0]; // Access to prevent optimization
  }
  const elapsed = performance.now() - start;

  const avgMs = elapsed / ITERATIONS;
  const throughputMBps = ((AUDIO_SAMPLE_COUNT * 4) / 1024 / 1024) / (avgMs / 1000);

  console.log(`  Total time: ${elapsed.toFixed(2)} ms`);
  console.log(`  Average per call: ${(avgMs * 1000).toFixed(2)} µs`);
  console.log(`  Throughput: ${throughputMBps.toFixed(2)} MB/s`);

  if (avgMs < 0.1) {
    console.log(`  Result: LIKELY ZERO-COPY (< 100µs per call) ✓`);
  } else if (avgMs < 1.0) {
    console.log(`  Result: POSSIBLE ZERO-COPY (< 1ms per call)`);
  } else {
    console.log(`  Result: LIKELY COPYING (> 1ms per call) ✗`);
  }
} catch (e) {
  console.log(`  Error: ${e.message}`);
}
console.log();

// ============================================
// Test 3: Performance benchmark - Video
// ============================================
console.log('Test 3: Video buffer access performance');
console.log('-'.repeat(40));
console.log(`  Resolution: ${VIDEO_WIDTH}x${VIDEO_HEIGHT} RGB24`);
console.log(`  Buffer size: ${(VIDEO_BYTES / 1024 / 1024).toFixed(2)} MB`);
console.log(`  Iterations: ${ITERATIONS.toLocaleString()}`);
console.log();

try {
  const videoBytes = createVideoBuffer(VIDEO_WIDTH, VIDEO_HEIGHT);
  const runtimeData = NapiRuntimeData.video(
    videoBytes,
    VIDEO_WIDTH,
    VIDEO_HEIGHT,
    4, // RGB24 format
    null, // no codec
    0, // frame number
    true // is keyframe
  );

  // Warm up
  for (let i = 0; i < 10; i++) {
    const buf = runtimeData.getVideoPixels();
    void buf[0];
  }

  // Benchmark
  const start = performance.now();
  for (let i = 0; i < ITERATIONS; i++) {
    const buf = runtimeData.getVideoPixels();
    void buf[0];
  }
  const elapsed = performance.now() - start;

  const avgMs = elapsed / ITERATIONS;
  const throughputMBps = (VIDEO_BYTES / 1024 / 1024) / (avgMs / 1000);

  console.log(`  Total time: ${elapsed.toFixed(2)} ms`);
  console.log(`  Average per call: ${(avgMs * 1000).toFixed(2)} µs`);
  console.log(`  Throughput: ${throughputMBps.toFixed(2)} MB/s`);

  if (avgMs < 0.1) {
    console.log(`  Result: LIKELY ZERO-COPY (< 100µs per call) ✓`);
  } else if (avgMs < 1.0) {
    console.log(`  Result: POSSIBLE ZERO-COPY (< 1ms per call)`);
  } else {
    console.log(`  Result: LIKELY COPYING (> 1ms per call) ✗`);
  }
} catch (e) {
  console.log(`  Error: ${e.message}`);
}
console.log();

// ============================================
// Test 4: Memory overhead comparison
// ============================================
console.log('Test 4: Memory overhead analysis');
console.log('-'.repeat(40));

try {
  // Force GC if available
  if (global.gc) {
    global.gc();
  }

  const initialMemory = process.memoryUsage().heapUsed;

  // Create multiple large buffers
  const buffers = [];
  const audioBytes = createAudioBuffer(AUDIO_SAMPLE_COUNT);

  for (let i = 0; i < 10; i++) {
    const runtimeData = NapiRuntimeData.audio(audioBytes, 48000, 1);
    const buf = runtimeData.getAudioSamples();
    buffers.push({ runtimeData, buf });
  }

  const afterMemory = process.memoryUsage().heapUsed;
  const memoryPerBuffer = (afterMemory - initialMemory) / 10;
  const dataSize = AUDIO_SAMPLE_COUNT * 4;

  console.log(`  Data size per buffer: ${(dataSize / 1024 / 1024).toFixed(2)} MB`);
  console.log(`  Memory per buffer: ${(memoryPerBuffer / 1024 / 1024).toFixed(2)} MB`);
  console.log(`  Overhead ratio: ${(memoryPerBuffer / dataSize).toFixed(2)}x`);

  if (memoryPerBuffer < dataSize * 1.5) {
    console.log(`  Result: LOW OVERHEAD (< 1.5x data size) - suggests zero-copy ✓`);
  } else if (memoryPerBuffer < dataSize * 2.5) {
    console.log(`  Result: MODERATE OVERHEAD (1.5-2.5x) - possible copy`);
  } else {
    console.log(`  Result: HIGH OVERHEAD (> 2.5x) - likely copying ✗`);
  }

  // Keep buffers alive for measurement
  void buffers.length;
} catch (e) {
  console.log(`  Error: ${e.message}`);
}

console.log();
console.log('='.repeat(60));
console.log('Benchmark complete');
console.log('='.repeat(60));
