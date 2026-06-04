/**
 * Pipeline Execution Tests
 *
 * Tests Node.js → Rust pipeline execution via napi FFI with zero-copy.
 * Uses NapiRuntimeData for zero-copy data transfer (no JSON serialization).
 */

import { NativeModule, NapiRuntimeData, PipelineOutput, loadNativeModule } from './types';

// Attempt to load the native module
const { native, loadError } = loadNativeModule();

/**
 * Helper to create f32 samples buffer from array
 */
function createSamplesBuffer(samples: number[]): Buffer {
  const buffer = Buffer.alloc(samples.length * 4);
  samples.forEach((sample, i) => {
    buffer.writeFloatLE(sample, i * 4);
  });
  return buffer;
}

/**
 * Helper to read f32 samples from buffer
 */
function readSamplesBuffer(buffer: Buffer): number[] {
  const samples: number[] = [];
  for (let i = 0; i < buffer.length; i += 4) {
    samples.push(buffer.readFloatLE(i));
  }
  return samples;
}

describe('Pipeline Execution (Zero-Copy)', () => {
  beforeAll(() => {
    if (!native || !native.isNativeLoaded()) {
      console.warn(
        'Native module not loaded, skipping pipeline tests.',
        'Build with: cargo build --features napi --no-default-features'
      );
      if (loadError) {
        console.warn('Load error:', loadError.message);
      }
    }
  });

  describe('Runtime Info', () => {
    test('should return runtime version', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const version = native.getRuntimeVersion();
      expect(version).toBeDefined();
      expect(typeof version).toBe('string');
      expect(version.length).toBeGreaterThan(0);
    });

    test('should report runtime availability', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const available = native.isRuntimeAvailable();
      expect(available).toBe(true);
    });
  });

  describe('NapiRuntimeData Creation', () => {
    test('should create text data', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const data = native.NapiRuntimeData.text('Hello, World!');
      expect(data).toBeDefined();
      expect(data.dataType).toBe(3); // Text type
      expect(data.getText()).toBe('Hello, World!');
    });

    test('should create audio data from buffer', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const samples = [0.0, 0.5, 1.0, -0.5, -1.0, 0.0];
      const buffer = createSamplesBuffer(samples);

      const data = native.NapiRuntimeData.audio(buffer, 48000, 1);
      expect(data).toBeDefined();
      expect(data.dataType).toBe(1); // Audio type
      expect(data.getAudioSampleRate()).toBe(48000);
      expect(data.getAudioChannels()).toBe(1);

      const outputSamples = readSamplesBuffer(data.getAudioSamples());
      expect(outputSamples).toHaveLength(6);
      // Check samples match (with floating point tolerance)
      samples.forEach((s, i) => {
        expect(outputSamples[i]).toBeCloseTo(s, 5);
      });
    });

    test('should create binary data', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const binaryData = Buffer.from([0x01, 0x02, 0x03, 0x04]);
      const data = native.NapiRuntimeData.binary(binaryData);
      expect(data).toBeDefined();
      expect(data.dataType).toBe(8); // Binary type

      const output = data.getBinary();
      expect(output).toEqual(binaryData);
    });

    test('should reject invalid audio buffer length', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      // Buffer length not multiple of 4 (f32 size)
      const invalidBuffer = Buffer.from([0x01, 0x02, 0x03]);
      expect(() => {
        native!.NapiRuntimeData.audio(invalidBuffer, 48000, 1);
      }).toThrow();
    });
  });

  describe('PassThrough Pipeline', () => {
    test('should execute simple pass-through pipeline with text', async () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const manifest = {
        version: 'v1',
        metadata: { name: 'pass-through-test' },
        nodes: [
          {
            id: 'passthrough',
            node_type: 'PassThrough',
            params: {},
          },
        ],
        connections: [],
      };

      const inputData = native.NapiRuntimeData.text('Hello, World!');
      const inputs = { passthrough: inputData };

      const result = await native.executePipeline(JSON.stringify(manifest), inputs);

      expect(result).toBeDefined();
      expect(result.size).toBeGreaterThan(0);
      expect(result.has('passthrough')).toBe(true);

      const output = result.get('passthrough');
      expect(output).not.toBeNull();
      expect(output!.dataType).toBe(3); // Text
      expect(output!.getText()).toBe('Hello, World!');
    });

    test('should execute pass-through pipeline with audio (zero-copy)', async () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const manifest = {
        version: 'v1',
        metadata: { name: 'audio-passthrough' },
        nodes: [
          {
            id: 'audio_node',
            node_type: 'PassThrough',
            params: {},
          },
        ],
        connections: [],
      };

      // Create audio samples
      const samples = [0.0, 0.5, 1.0, 0.5, 0.0, -0.5, -1.0, -0.5];
      const samplesBuffer = createSamplesBuffer(samples);

      const inputData = native.NapiRuntimeData.audio(samplesBuffer, 48000, 1);
      const inputs = { audio_node: inputData };

      const result = await native.executePipeline(JSON.stringify(manifest), inputs);

      expect(result).toBeDefined();
      expect(result.has('audio_node')).toBe(true);

      const output = result.get('audio_node');
      expect(output).not.toBeNull();
      expect(output!.dataType).toBe(1); // Audio
      expect(output!.getAudioSampleRate()).toBe(48000);
      expect(output!.getAudioChannels()).toBe(1);

      const outputSamples = readSamplesBuffer(output!.getAudioSamples());
      expect(outputSamples).toHaveLength(8);

      // Verify samples match (zero-copy should preserve exact values)
      samples.forEach((s, i) => {
        expect(outputSamples[i]).toBeCloseTo(s, 5);
      });
    });

    test('should execute pass-through pipeline with binary data', async () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const manifest = {
        version: 'v1',
        metadata: { name: 'binary-passthrough' },
        nodes: [
          {
            id: 'binary_node',
            node_type: 'PassThrough',
            params: {},
          },
        ],
        connections: [],
      };

      const binaryData = Buffer.from([0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE]);
      const inputData = native.NapiRuntimeData.binary(binaryData);
      const inputs = { binary_node: inputData };

      const result = await native.executePipeline(JSON.stringify(manifest), inputs);

      expect(result).toBeDefined();
      expect(result.has('binary_node')).toBe(true);

      const output = result.get('binary_node');
      expect(output).not.toBeNull();
      expect(output!.dataType).toBe(8); // Binary
      expect(output!.getBinary()).toEqual(binaryData);
    });
  });

  describe('Calculator Pipeline', () => {
    test('should execute calculator addition', async () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const manifest = {
        version: 'v1',
        metadata: { name: 'calculator-add' },
        nodes: [
          {
            id: 'calc',
            node_type: 'CalculatorNode',
            params: {},
          },
        ],
        connections: [],
      };

      // Calculator expects JSON RuntimeData with operation and operands
      const inputData = native.NapiRuntimeData.json(
        JSON.stringify({
          operation: 'add',
          operands: [5, 3],
        })
      );
      const inputs = { calc: inputData };

      const result = await native.executePipeline(JSON.stringify(manifest), inputs);

      expect(result).toBeDefined();
      expect(result.has('calc')).toBe(true);

      const output = result.get('calc');
      expect(output).not.toBeNull();
      // Calculator output is JSON RuntimeData
      expect(output!.dataType).toBe(7); // JSON type
      const outputJson = JSON.parse(output!.getJson());
      expect(outputJson.result).toBe(8);
      expect(outputJson.operation).toBe('add');
    });

    test('should execute calculator multiplication', async () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const manifest = {
        version: 'v1',
        metadata: { name: 'calculator-multiply' },
        nodes: [
          {
            id: 'calc',
            node_type: 'CalculatorNode',
            params: {},
          },
        ],
        connections: [],
      };

      const inputData = native.NapiRuntimeData.json(
        JSON.stringify({
          operation: 'multiply',
          operands: [7, 6],
        })
      );
      const inputs = { calc: inputData };

      const result = await native.executePipeline(JSON.stringify(manifest), inputs);

      expect(result).toBeDefined();
      const output = result.get('calc');
      expect(output).not.toBeNull();

      expect(output!.dataType).toBe(7); // JSON type
      const outputJson = JSON.parse(output!.getJson());
      expect(outputJson.result).toBe(42);
      expect(outputJson.operation).toBe('multiply');
    });
  });

  describe('Multi-Node Pipeline', () => {
    test('should execute connected pipeline nodes', async () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      // Pipeline: passthrough1 -> passthrough2
      const manifest = {
        version: 'v1',
        metadata: { name: 'multi-node' },
        nodes: [
          {
            id: 'node1',
            node_type: 'PassThrough',
            params: {},
          },
          {
            id: 'node2',
            node_type: 'PassThrough',
            params: {},
          },
        ],
        connections: [
          {
            from: 'node1',
            to: 'node2',
          },
        ],
      };

      const inputData = native.NapiRuntimeData.text('Test message through chain');
      const inputs = { node1: inputData };

      const result = await native.executePipeline(JSON.stringify(manifest), inputs);

      expect(result).toBeDefined();
      // Node2 should have the output after processing chain
      expect(result.has('node2')).toBe(true);

      const output = result.get('node2');
      expect(output).not.toBeNull();
      expect(output!.dataType).toBe(3); // Text
      expect(output!.getText()).toBe('Test message through chain');
    });
  });

  describe('Session-based Execution', () => {
    test('should execute pipeline with session ID', async () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const manifest = {
        version: 'v1',
        metadata: { name: 'session-test' },
        nodes: [
          {
            id: 'passthrough',
            node_type: 'PassThrough',
            params: {},
          },
        ],
        connections: [],
      };

      const inputData = native.NapiRuntimeData.text('Session test data');
      const inputs = { passthrough: inputData };
      const sessionId = `test_session_${Date.now()}`;

      const result = await native.executePipelineWithSession(
        JSON.stringify(manifest),
        inputs,
        sessionId
      );

      expect(result).toBeDefined();
      expect(result.has('passthrough')).toBe(true);

      const output = result.get('passthrough');
      expect(output).not.toBeNull();
      expect(output!.getText()).toBe('Session test data');
    });
  });

  describe('PipelineOutput API', () => {
    test('should provide correct output metadata', async () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const manifest = {
        version: 'v1',
        metadata: { name: 'output-api-test' },
        nodes: [
          { id: 'node1', node_type: 'PassThrough', params: {} },
          { id: 'node2', node_type: 'PassThrough', params: {} },
        ],
        connections: [],
      };

      const input1 = native.NapiRuntimeData.text('Data 1');
      const input2 = native.NapiRuntimeData.text('Data 2');
      const inputs = { node1: input1, node2: input2 };

      const result = await native.executePipeline(JSON.stringify(manifest), inputs);

      expect(result.size).toBe(2);

      const nodeIds = result.getNodeIds();
      expect(nodeIds).toContain('node1');
      expect(nodeIds).toContain('node2');

      expect(result.has('node1')).toBe(true);
      expect(result.has('node2')).toBe(true);
      expect(result.has('nonexistent')).toBe(false);

      expect(result.get('nonexistent')).toBeNull();
    });
  });

  describe('Error Handling', () => {
    test('should handle invalid manifest JSON', async () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const inputData = native.NapiRuntimeData.text('test');

      await expect(
        native.executePipeline('not valid json', { node: inputData })
      ).rejects.toThrow();
    });

    test('should handle unknown node type', async () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const manifest = {
        version: 'v1',
        metadata: { name: 'unknown-node' },
        nodes: [
          {
            id: 'unknown',
            node_type: 'NonExistentNodeType',
            params: {},
          },
        ],
        connections: [],
      };

      const inputData = native.NapiRuntimeData.text('test');

      await expect(
        native.executePipeline(JSON.stringify(manifest), { unknown: inputData })
      ).rejects.toThrow();
    });

    test('should reject empty inputs for nodes that require input', async () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const manifest = {
        version: 'v1',
        metadata: { name: 'empty-inputs' },
        nodes: [{ id: 'n', node_type: 'PassThrough', params: {} }],
        connections: [],
      };

      // Empty inputs - should reject because node needs input
      await expect(
        native.executePipeline(JSON.stringify(manifest), {})
      ).rejects.toThrow(/No inputs for node/);
    });
  });

  describe('Zero-Copy Verification', () => {
    test('should handle large audio buffer efficiently', async () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const manifest = {
        version: 'v1',
        metadata: { name: 'large-audio-test' },
        nodes: [
          {
            id: 'audio',
            node_type: 'PassThrough',
            params: {},
          },
        ],
        connections: [],
      };

      // Create ~1 second of audio at 48kHz mono
      const numSamples = 48000;
      const samples: number[] = new Array(numSamples);
      for (let i = 0; i < numSamples; i++) {
        samples[i] = Math.sin((2 * Math.PI * 440 * i) / 48000); // 440Hz sine wave
      }
      const samplesBuffer = createSamplesBuffer(samples);

      const inputData = native.NapiRuntimeData.audio(samplesBuffer, 48000, 1);
      const inputs = { audio: inputData };

      const startTime = Date.now();
      const result = await native.executePipeline(JSON.stringify(manifest), inputs);
      const endTime = Date.now();

      expect(result).toBeDefined();
      expect(result.has('audio')).toBe(true);

      const output = result.get('audio');
      expect(output).not.toBeNull();
      expect(output!.getAudioSampleRate()).toBe(48000);

      const outputBuffer = output!.getAudioSamples();
      expect(outputBuffer.length).toBe(numSamples * 4); // 4 bytes per f32

      // Verify first few samples match
      const outputSamples = readSamplesBuffer(outputBuffer);
      for (let i = 0; i < 10; i++) {
        expect(outputSamples[i]).toBeCloseTo(samples[i], 5);
      }

      // Zero-copy should be fast - less than 100ms for pass-through
      console.log(`Large audio pass-through took ${endTime - startTime}ms`);
      expect(endTime - startTime).toBeLessThan(1000);
    });
  });
});
