/**
 * Node Builder Classes Tests
 *
 * Tests for the generated type-safe node builder classes and PipelineBuilder.
 */

import {
  AudioResample,
  SileroVAD,
  WhisperNode,
  CalculatorNode,
  VideoFlip,
  PassThrough,
  TextCollector,
  KokoroTTSNode,
  Echo,
  AudioChunker,
  PipelineBuilder,
  Nodes,
  nodeSchemas,
  PipelineManifest,
} from '../node-schemas';
import { loadNativeModule } from './types';

const { native, loadError } = loadNativeModule();

describe('Node Builder Classes', () => {
  describe('Node Class Construction', () => {
    test('should create Echo node without config', () => {
      const node = new Echo('my-echo');
      expect(node.id).toBe('my-echo');
      expect(node.nodeType).toBe('Echo');
      expect(node.config).toBeUndefined();
    });

    test('should create PassThrough node without config', () => {
      const node = new PassThrough('passthrough-1');
      expect(node.id).toBe('passthrough-1');
      expect(node.nodeType).toBe('PassThrough');
    });

    test('should create AudioResample with typed config', () => {
      const node = new AudioResample('resample', { target_sample_rate: 16000 });
      expect(node.id).toBe('resample');
      expect(node.nodeType).toBe('AudioResample');
      expect(node.config?.target_sample_rate).toBe(16000);
    });

    test('should create SileroVAD with all config options', () => {
      const node = new SileroVAD('vad', {
        threshold: 0.6,
        min_silence_duration_ms: 150,
        min_speech_duration_ms: 300,
      });
      expect(node.nodeType).toBe('SileroVAD');
      expect(node.config?.threshold).toBe(0.6);
      expect(node.config?.min_silence_duration_ms).toBe(150);
      expect(node.config?.min_speech_duration_ms).toBe(300);
    });

    test('should create WhisperNode with enum config values', () => {
      const node = new WhisperNode('whisper', {
        model: 'base',
        task: 'transcribe',
        language: 'en',
      });
      expect(node.nodeType).toBe('WhisperNode');
      expect(node.config?.model).toBe('base');
      expect(node.config?.task).toBe('transcribe');
    });

    test('should create KokoroTTSNode with voice enum', () => {
      const node = new KokoroTTSNode('tts', {
        voice: 'af_bella',
        speed: 1.2,
        language: 'en-us',
      });
      expect(node.nodeType).toBe('KokoroTTSNode');
      expect(node.config?.voice).toBe('af_bella');
      expect(node.config?.speed).toBe(1.2);
    });

    test('should create CalculatorNode with precision', () => {
      const node = new CalculatorNode('calc', { precision: 5 });
      expect(node.nodeType).toBe('CalculatorNode');
      expect(node.config?.precision).toBe(5);
    });

    test('should create VideoFlip with boolean config', () => {
      const node = new VideoFlip('flip', {
        horizontal: true,
        vertical: false,
      });
      expect(node.nodeType).toBe('VideoFlip');
      expect(node.config?.horizontal).toBe(true);
      expect(node.config?.vertical).toBe(false);
    });

    test('should create TextCollector with config', () => {
      const node = new TextCollector('collector', {
        delimiter: '\n',
        flush_on_silence: true,
      });
      expect(node.nodeType).toBe('TextCollector');
      expect(node.config?.delimiter).toBe('\n');
    });

    test('should create AudioChunker with chunk size', () => {
      const node = new AudioChunker('chunker', { chunk_size_ms: 30 });
      expect(node.nodeType).toBe('AudioChunker');
      expect(node.config?.chunk_size_ms).toBe(30);
    });
  });

  describe('Static Node Metadata', () => {
    test('Echo should have correct accepts/produces', () => {
      expect(Echo.nodeType).toBe('Echo');
      expect(Echo.accepts).toContain('audio');
      expect(Echo.accepts).toContain('text');
      expect(Echo.accepts).toContain('json');
      expect(Echo.produces).toContain('audio');
      expect(Echo.produces).toContain('text');
    });

    test('AudioResample should accept and produce audio', () => {
      expect(AudioResample.accepts).toEqual(['audio']);
      expect(AudioResample.produces).toEqual(['audio']);
    });

    test('SileroVAD should produce audio and controlmessage', () => {
      expect(SileroVAD.accepts).toEqual(['audio']);
      expect(SileroVAD.produces).toContain('audio');
      expect(SileroVAD.produces).toContain('controlmessage');
    });

    test('WhisperNode should accept audio and produce text/json', () => {
      expect(WhisperNode.accepts).toEqual(['audio']);
      expect(WhisperNode.produces).toContain('text');
      expect(WhisperNode.produces).toContain('json');
    });

    test('KokoroTTSNode should accept text and produce audio', () => {
      expect(KokoroTTSNode.accepts).toEqual(['text']);
      expect(KokoroTTSNode.produces).toEqual(['audio']);
    });
  });

  describe('toPipelineNode conversion', () => {
    test('should convert node to pipeline format', () => {
      const node = new SileroVAD('vad', { threshold: 0.7 });
      const pipelineNode = node.toPipelineNode();

      expect(pipelineNode.id).toBe('vad');
      expect(pipelineNode.nodeType).toBe('SileroVAD');
      expect(pipelineNode.config?.threshold).toBe(0.7);
    });

    test('should convert node without config', () => {
      const node = new Echo('echo');
      const pipelineNode = node.toPipelineNode();

      expect(pipelineNode.id).toBe('echo');
      expect(pipelineNode.nodeType).toBe('Echo');
      expect(pipelineNode.config).toBeUndefined();
    });
  });

  describe('connectTo helper', () => {
    test('should create connection to another node', () => {
      const vad = new SileroVAD('vad');
      const whisper = new WhisperNode('whisper');

      const connection = vad.connectTo(whisper);

      expect(connection.source).toBe('vad');
      expect(connection.destination).toBe('whisper');
    });

    test('should create connection by node id string', () => {
      const vad = new SileroVAD('vad');
      const connection = vad.connectTo('output-node');

      expect(connection.source).toBe('vad');
      expect(connection.destination).toBe('output-node');
    });

    test('should support port specification', () => {
      const vad = new SileroVAD('vad');
      const connection = vad.connectTo('whisper', 'audio_out', 'audio_in');

      expect(connection.sourcePort).toBe('audio_out');
      expect(connection.destinationPort).toBe('audio_in');
    });
  });

  describe('Nodes namespace', () => {
    test('should contain all node classes', () => {
      expect(Nodes.AudioResample).toBe(AudioResample);
      expect(Nodes.SileroVAD).toBe(SileroVAD);
      expect(Nodes.WhisperNode).toBe(WhisperNode);
      expect(Nodes.CalculatorNode).toBe(CalculatorNode);
      expect(Nodes.VideoFlip).toBe(VideoFlip);
      expect(Nodes.PassThrough).toBe(PassThrough);
      expect(Nodes.TextCollector).toBe(TextCollector);
      expect(Nodes.KokoroTTSNode).toBe(KokoroTTSNode);
      expect(Nodes.Echo).toBe(Echo);
      expect(Nodes.AudioChunker).toBe(AudioChunker);
    });
  });
});

describe('PipelineBuilder', () => {
  describe('Basic construction', () => {
    test('should create empty pipeline with default version', () => {
      const builder = new PipelineBuilder();
      const manifest = builder.build();

      expect(manifest.version).toBe('1.0');
      expect(manifest.nodes).toEqual([]);
      expect(manifest.connections).toEqual([]);
    });

    test('should create pipeline with custom version', () => {
      const builder = new PipelineBuilder('2.0');
      const manifest = builder.build();

      expect(manifest.version).toBe('2.0');
    });
  });

  describe('Metadata', () => {
    test('should set pipeline name', () => {
      const manifest = new PipelineBuilder()
        .name('My Pipeline')
        .build();

      expect(manifest.metadata?.name).toBe('My Pipeline');
    });

    test('should set pipeline description', () => {
      const manifest = new PipelineBuilder()
        .description('A test pipeline')
        .build();

      expect(manifest.metadata?.description).toBe('A test pipeline');
    });

    test('should set custom metadata', () => {
      const manifest = new PipelineBuilder()
        .meta('author', 'test')
        .meta('version', '1.0.0')
        .build();

      expect(manifest.metadata?.author).toBe('test');
      expect(manifest.metadata?.version).toBe('1.0.0');
    });

    test('should not include metadata if empty', () => {
      const manifest = new PipelineBuilder().build();
      expect(manifest.metadata).toBeUndefined();
    });
  });

  describe('Adding nodes', () => {
    test('should add single node', () => {
      const manifest = new PipelineBuilder()
        .add(new Echo('echo'))
        .build();

      expect(manifest.nodes).toHaveLength(1);
      expect(manifest.nodes[0].id).toBe('echo');
      expect(manifest.nodes[0].nodeType).toBe('Echo');
    });

    test('should add multiple nodes', () => {
      const manifest = new PipelineBuilder()
        .add(new SileroVAD('vad', { threshold: 0.6 }))
        .add(new WhisperNode('whisper', { model: 'base' }))
        .add(new KokoroTTSNode('tts', { voice: 'af_bella' }))
        .build();

      expect(manifest.nodes).toHaveLength(3);
      expect(manifest.nodes[0].id).toBe('vad');
      expect(manifest.nodes[1].id).toBe('whisper');
      expect(manifest.nodes[2].id).toBe('tts');
    });

    test('should add raw node definition', () => {
      const manifest = new PipelineBuilder()
        .addRaw({ id: 'raw', nodeType: 'Echo' })
        .build();

      expect(manifest.nodes).toHaveLength(1);
      expect(manifest.nodes[0].id).toBe('raw');
    });
  });

  describe('Connections', () => {
    test('should connect nodes by reference', () => {
      const vad = new SileroVAD('vad');
      const whisper = new WhisperNode('whisper');

      const manifest = new PipelineBuilder()
        .add(vad)
        .add(whisper)
        .connect(vad, whisper)
        .build();

      expect(manifest.connections).toHaveLength(1);
      expect(manifest.connections[0].source).toBe('vad');
      expect(manifest.connections[0].destination).toBe('whisper');
    });

    test('should connect nodes by string id', () => {
      const manifest = new PipelineBuilder()
        .add(new Echo('node1'))
        .add(new PassThrough('node2'))
        .connect('node1', 'node2')
        .build();

      expect(manifest.connections).toHaveLength(1);
      expect(manifest.connections[0].source).toBe('node1');
      expect(manifest.connections[0].destination).toBe('node2');
    });

    test('should connect with port specification', () => {
      const manifest = new PipelineBuilder()
        .add(new SileroVAD('vad'))
        .add(new WhisperNode('whisper'))
        .connect('vad', 'whisper', 'audio_out', 'audio_in')
        .build();

      expect(manifest.connections[0].sourcePort).toBe('audio_out');
      expect(manifest.connections[0].destinationPort).toBe('audio_in');
    });

    test('should create linear pipeline chain', () => {
      const manifest = new PipelineBuilder()
        .add(new AudioResample('resample', { target_sample_rate: 16000 }))
        .add(new SileroVAD('vad', { threshold: 0.5 }))
        .add(new WhisperNode('whisper', { model: 'base' }))
        .connect('resample', 'vad')
        .connect('vad', 'whisper')
        .build();

      expect(manifest.nodes).toHaveLength(3);
      expect(manifest.connections).toHaveLength(2);
    });
  });

  describe('toJson output', () => {
    test('should serialize to JSON string', () => {
      const builder = new PipelineBuilder()
        .name('Test')
        .add(new Echo('echo'));

      const json = builder.toJson();
      const parsed = JSON.parse(json);

      expect(parsed.version).toBe('1.0');
      expect(parsed.metadata.name).toBe('Test');
      expect(parsed.nodes).toHaveLength(1);
    });
  });

  describe('Real-world pipeline examples', () => {
    test('should build speech-to-text pipeline', () => {
      const manifest = new PipelineBuilder('1.0')
        .name('Speech to Text')
        .description('Converts audio input to text transcription')
        .add(new AudioResample('resample', { target_sample_rate: 16000 }))
        .add(new SileroVAD('vad', {
          threshold: 0.5,
          min_silence_duration_ms: 100,
          min_speech_duration_ms: 250,
        }))
        .add(new WhisperNode('whisper', {
          model: 'base',
          task: 'transcribe',
        }))
        .connect('resample', 'vad')
        .connect('vad', 'whisper')
        .build();

      expect(manifest.version).toBe('1.0');
      expect(manifest.metadata?.name).toBe('Speech to Text');
      expect(manifest.nodes).toHaveLength(3);
      expect(manifest.connections).toHaveLength(2);

      // Verify node configs
      const resampleNode = manifest.nodes.find(n => n.id === 'resample');
      expect((resampleNode?.config as any)?.target_sample_rate).toBe(16000);

      const vadNode = manifest.nodes.find(n => n.id === 'vad');
      expect((vadNode?.config as any)?.threshold).toBe(0.5);

      const whisperNode = manifest.nodes.find(n => n.id === 'whisper');
      expect((whisperNode?.config as any)?.model).toBe('base');
    });

    test('should build text-to-speech pipeline', () => {
      const manifest = new PipelineBuilder()
        .name('Text to Speech')
        .add(new TextCollector('collector', { flush_on_silence: true }))
        .add(new KokoroTTSNode('tts', {
          voice: 'am_michael',
          speed: 1.0,
          language: 'en-us',
        }))
        .add(new AudioChunker('chunker', { chunk_size_ms: 20 }))
        .connect('collector', 'tts')
        .connect('tts', 'chunker')
        .build();

      expect(manifest.nodes).toHaveLength(3);
      expect(manifest.connections).toHaveLength(2);

      const ttsNode = manifest.nodes.find(n => n.id === 'tts');
      expect((ttsNode?.config as any)?.voice).toBe('am_michael');
    });
  });
});

describe('nodeSchemas constant', () => {
  test('should export array of node metadata', () => {
    expect(Array.isArray(nodeSchemas)).toBe(true);
    expect(nodeSchemas.length).toBeGreaterThan(0);
  });

  test('should include all registered nodes', () => {
    const nodeTypes = nodeSchemas.map(s => s.nodeType);
    expect(nodeTypes).toContain('Echo');
    expect(nodeTypes).toContain('PassThrough');
    expect(nodeTypes).toContain('SileroVAD');
    expect(nodeTypes).toContain('WhisperNode');
    expect(nodeTypes).toContain('KokoroTTSNode');
  });

  test('should have valid metadata for each node', () => {
    nodeSchemas.forEach(schema => {
      expect(schema.nodeType).toBeDefined();
      expect(Array.isArray(schema.accepts)).toBe(true);
      expect(Array.isArray(schema.produces)).toBe(true);
      expect(typeof schema.streaming).toBe('boolean');
    });
  });
});

describe('Integration with Native Module', () => {
  beforeAll(() => {
    if (!native || !native.isNativeLoaded()) {
      console.warn(
        'Native module not loaded, skipping integration tests.',
        'Build with: cargo build --features napi --no-default-features'
      );
      if (loadError) {
        console.warn('Load error:', loadError.message);
      }
    }
  });

  /**
   * Helper to convert PipelineBuilder manifest to native manifest format.
   * The native runtime expects snake_case `node_type` instead of camelCase `nodeType`.
   */
  function toNativeManifest(manifest: PipelineManifest): object {
    return {
      version: manifest.version,
      metadata: manifest.metadata ?? { name: 'unnamed' },
      nodes: manifest.nodes.map(node => ({
        id: node.id,
        node_type: node.nodeType,
        params: node.config ?? {},
      })),
      connections: manifest.connections.map(conn => ({
        from: conn.source,
        to: conn.destination,
        from_port: conn.sourcePort,
        to_port: conn.destinationPort,
      })),
    };
  }

  test('should validate manifest built with PipelineBuilder', () => {
    if (!native?.isNativeLoaded()) {
      console.log('Skipping: native module not loaded');
      return;
    }

    const manifest = new PipelineBuilder()
      .add(new Echo('echo1'))
      .add(new PassThrough('pass1'))
      .connect('echo1', 'pass1')
      .build();

    const nativeManifest = toNativeManifest(manifest);
    const errors = native.validateManifest(JSON.stringify(nativeManifest));

    expect(Array.isArray(errors)).toBe(true);
    expect(errors.length).toBe(0);
  });

  test('should execute pipeline built with node builders', async () => {
    if (!native?.isNativeLoaded()) {
      console.log('Skipping: native module not loaded');
      return;
    }

    // Build pipeline using typed classes - use PassThrough as it's registered in the runtime
    const manifest = new PipelineBuilder('v1')
      .name('Builder Test Pipeline')
      .add(new PassThrough('input'))
      .add(new PassThrough('output'))
      .connect('input', 'output')
      .build();

    // Convert to native format
    const nativeManifest = toNativeManifest(manifest);

    // Create input data
    const inputData = native.NapiRuntimeData.text('Hello from PipelineBuilder!');
    const inputs = { input: inputData };

    // Execute
    const result = await native.executePipeline(JSON.stringify(nativeManifest), inputs);

    expect(result).toBeDefined();
    expect(result.has('output')).toBe(true);

    const output = result.get('output');
    expect(output).not.toBeNull();
    expect(output!.getText()).toBe('Hello from PipelineBuilder!');
  });

  test('should execute calculator pipeline built with PipelineBuilder', async () => {
    if (!native?.isNativeLoaded()) {
      console.log('Skipping: native module not loaded');
      return;
    }

    const manifest = new PipelineBuilder('v1')
      .name('Calculator Test')
      .add(new CalculatorNode('calc', { precision: 10 }))
      .build();

    const nativeManifest = toNativeManifest(manifest);

    const inputData = native.NapiRuntimeData.json(JSON.stringify({
      operation: 'add',
      operands: [10, 25],
    }));
    const inputs = { calc: inputData };

    const result = await native.executePipeline(JSON.stringify(nativeManifest), inputs);

    expect(result.has('calc')).toBe(true);
    const output = result.get('calc');
    expect(output).not.toBeNull();

    const outputJson = JSON.parse(output!.getJson());
    expect(outputJson.result).toBe(35);
    expect(outputJson.operation).toBe('add');
  });

  test('should match schema registry with generated types', () => {
    if (!native?.isNativeLoaded()) {
      console.log('Skipping: native module not loaded');
      return;
    }

    // Get schemas from native registry
    const nativeSchemas = native.getNodeSchemas();
    const nativeTypes = nativeSchemas.map((s: any) => s.nodeType).sort();

    // Get schemas from generated constant
    const generatedTypes = nodeSchemas.map(s => s.nodeType).sort();

    // They should match
    expect(nativeTypes).toEqual(generatedTypes);
  });
});
