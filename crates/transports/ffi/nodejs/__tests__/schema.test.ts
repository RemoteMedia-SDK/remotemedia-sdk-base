/**
 * Node Schema Registry Tests
 *
 * Tests for introspecting registered pipeline nodes and their parameters.
 */

import { loadNativeModule } from './types';

const { native, loadError } = loadNativeModule();

describe('Node Schema Registry', () => {
  beforeAll(() => {
    if (!native || !native.isNativeLoaded()) {
      console.warn(
        'Native module not loaded, skipping schema tests.',
        'Build with: cargo build --features napi --no-default-features'
      );
      if (loadError) {
        console.warn('Load error:', loadError.message);
      }
    }
  });

  describe('getNodeSchemas', () => {
    test('should return array of node schemas', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const schemas = native.getNodeSchemas();
      expect(Array.isArray(schemas)).toBe(true);
      expect(schemas.length).toBeGreaterThan(0);

      // Check that each schema has required fields
      schemas.forEach((schema: any) => {
        expect(schema.nodeType).toBeDefined();
        expect(typeof schema.nodeType).toBe('string');
        expect(Array.isArray(schema.accepts)).toBe(true);
        expect(Array.isArray(schema.produces)).toBe(true);
        expect(Array.isArray(schema.parameters)).toBe(true);
      });
    });

    test('should include known built-in nodes', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const schemas = native.getNodeSchemas();
      const nodeTypes = schemas.map((s: any) => s.nodeType);

      expect(nodeTypes).toContain('Echo');
      expect(nodeTypes).toContain('PassThrough');
      expect(nodeTypes).toContain('CalculatorNode');
    });
  });

  describe('getNodeSchema', () => {
    test('should return schema for existing node type', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const schema = native.getNodeSchema('CalculatorNode');
      expect(schema).not.toBeNull();
      expect(schema.nodeType).toBe('CalculatorNode');
      expect(schema.accepts).toContain('json');
      expect(schema.produces).toContain('json');
    });

    test('should return null for non-existent node type', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const schema = native.getNodeSchema('NonExistentNode');
      expect(schema).toBeNull();
    });

    test('should include parameters for configured nodes', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const schema = native.getNodeSchema('AudioResample');
      expect(schema).not.toBeNull();
      expect(schema.parameters.length).toBeGreaterThan(0);

      // Check target_sample_rate parameter
      const sampleRateParam = schema.parameters.find(
        (p: any) => p.name === 'target_sample_rate'
      );
      expect(sampleRateParam).toBeDefined();
      expect(sampleRateParam.paramType).toBe('integer');
      expect(sampleRateParam.description).toContain('sample rate');
    });
  });

  describe('getNodeParameters', () => {
    test('should return parameters for KokoroTTSNode', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const params = native.getNodeParameters('KokoroTTSNode');
      expect(Array.isArray(params)).toBe(true);
      expect(params.length).toBeGreaterThan(0);

      // Check voice parameter
      const voiceParam = params.find((p: any) => p.name === 'voice');
      expect(voiceParam).toBeDefined();
      expect(voiceParam.paramType).toBe('string');
      expect(voiceParam.enumValues).toBeDefined();

      // Parse enum values
      const enumValues = JSON.parse(voiceParam.enumValues);
      expect(Array.isArray(enumValues)).toBe(true);
      expect(enumValues).toContain('af_bella');

      // Check default value
      expect(voiceParam.defaultValue).toBe('"af_bella"');
    });

    test('should return parameters with numeric constraints', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const params = native.getNodeParameters('SileroVAD');
      const thresholdParam = params.find((p: any) => p.name === 'threshold');

      expect(thresholdParam).toBeDefined();
      expect(thresholdParam.paramType).toBe('number');
      expect(thresholdParam.minimum).toBe(0.0);
      expect(thresholdParam.maximum).toBe(1.0);
      expect(thresholdParam.defaultValue).toBe('0.5');
    });

    test('should return empty array for node without parameters', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const params = native.getNodeParameters('Echo');
      expect(Array.isArray(params)).toBe(true);
      expect(params.length).toBe(0);
    });

    test('should return empty array for non-existent node', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const params = native.getNodeParameters('NonExistentNode');
      expect(Array.isArray(params)).toBe(true);
      expect(params.length).toBe(0);
    });
  });

  describe('getNodeTypes', () => {
    test('should return array of node type names', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const types = native.getNodeTypes();
      expect(Array.isArray(types)).toBe(true);
      expect(types.length).toBeGreaterThan(0);
      expect(types).toContain('Echo');
      expect(types).toContain('PassThrough');
    });
  });

  describe('getNodeTypesByCategory', () => {
    test('should return audio nodes', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const audioNodes = native.getNodeTypesByCategory('audio');
      expect(Array.isArray(audioNodes)).toBe(true);
      expect(audioNodes).toContain('AudioResample');
      expect(audioNodes).toContain('SileroVAD');
    });

    test('should return ML nodes', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const mlNodes = native.getNodeTypesByCategory('ml');
      expect(Array.isArray(mlNodes)).toBe(true);
      expect(mlNodes).toContain('KokoroTTSNode');
      expect(mlNodes).toContain('WhisperNode');
    });

    test('should return empty array for unknown category', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const nodes = native.getNodeTypesByCategory('nonexistent');
      expect(Array.isArray(nodes)).toBe(true);
      expect(nodes.length).toBe(0);
    });
  });

  describe('getNodeCategories', () => {
    test('should return unique categories', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const categories = native.getNodeCategories();
      expect(Array.isArray(categories)).toBe(true);
      expect(categories).toContain('audio');
      expect(categories).toContain('ml');
      expect(categories).toContain('utility');

      // Check uniqueness
      const uniqueCategories = [...new Set(categories)];
      expect(categories.length).toBe(uniqueCategories.length);
    });
  });

  describe('hasNodeType', () => {
    test('should return true for existing node', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      expect(native.hasNodeType('Echo')).toBe(true);
      expect(native.hasNodeType('PassThrough')).toBe(true);
      expect(native.hasNodeType('CalculatorNode')).toBe(true);
    });

    test('should return false for non-existent node', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      expect(native.hasNodeType('NonExistentNode')).toBe(false);
    });
  });

  describe('validateManifest', () => {
    test('should validate correct manifest', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const manifest = {
        version: '1.0',
        nodes: [
          { id: 'echo1', node_type: 'Echo' },
          { id: 'pass1', node_type: 'PassThrough' },
        ],
        connections: [{ source: 'echo1', destination: 'pass1' }],
      };

      const errors = native.validateManifest(JSON.stringify(manifest));
      expect(Array.isArray(errors)).toBe(true);
      expect(errors.length).toBe(0);
    });

    test('should detect unknown node type', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const manifest = {
        version: '1.0',
        nodes: [{ id: 'unknown', node_type: 'NonExistentNode' }],
        connections: [],
      };

      const errors = native.validateManifest(JSON.stringify(manifest));
      expect(errors.length).toBeGreaterThan(0);
      expect(errors[0]).toContain('Unknown node type');
    });

    test('should detect missing node id', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const manifest = {
        version: '1.0',
        nodes: [{ node_type: 'Echo' }],
        connections: [],
      };

      const errors = native.validateManifest(JSON.stringify(manifest));
      expect(errors.length).toBeGreaterThan(0);
      expect(errors[0]).toContain("Missing 'id'");
    });

    test('should detect invalid connection source', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const manifest = {
        version: '1.0',
        nodes: [{ id: 'echo1', node_type: 'Echo' }],
        connections: [{ source: 'nonexistent', destination: 'echo1' }],
      };

      const errors = native.validateManifest(JSON.stringify(manifest));
      expect(errors.length).toBeGreaterThan(0);
      expect(errors[0]).toContain('Source');
      expect(errors[0]).toContain('not found');
    });
  });

  describe('getNodeSchemasJson', () => {
    test('should return valid JSON array', () => {
      if (!native?.isNativeLoaded()) {
        console.log('Skipping: native module not loaded');
        return;
      }

      const json = native.getNodeSchemasJson();
      expect(typeof json).toBe('string');

      const parsed = JSON.parse(json);
      expect(Array.isArray(parsed)).toBe(true);
      expect(parsed.length).toBeGreaterThan(0);
    });
  });
});
