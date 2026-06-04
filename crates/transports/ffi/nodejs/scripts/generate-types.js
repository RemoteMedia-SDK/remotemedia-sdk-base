#!/usr/bin/env node
/**
 * Generate TypeScript type definitions from the node schema registry.
 *
 * This script loads the native module and generates TypeScript type definitions
 * for all registered pipeline nodes, including:
 * - RuntimeData types
 * - Config interfaces for each node
 * - Node builder classes with typed constructors
 * - PipelineBuilder fluent API
 *
 * Usage:
 *   node scripts/generate-types.js              # Generate node-schemas.d.ts
 *   node scripts/generate-types.js --stdout     # Output to stdout
 *   node scripts/generate-types.js --json       # Output JSON schemas to stdout
 */

const fs = require('fs');
const path = require('path');

// Output file paths
const OUTPUT_FILE = path.join(__dirname, '..', 'node-schemas.ts');
const OUTPUT_JSON = path.join(__dirname, '..', 'node-schemas.json');

// =============================================================================
// TypeScript Generation
// =============================================================================

const RUNTIME_DATA_TYPES = `// RuntimeData types (matches Rust enum)
export type RuntimeDataType = 'audio' | 'video' | 'json' | 'text' | 'binary' | 'tensor' | 'numpy' | 'control' | 'controlmessage';

export interface AudioData {
  samples: Float32Array;
  sampleRate: number;
  channels: number;
  streamId?: string;
}

export interface VideoData {
  pixelData: Uint8Array;
  width: number;
  height: number;
  format: 'yuv420p' | 'rgb24' | 'rgba32' | 'gray8';
  codec?: 'raw' | 'h264' | 'vp8' | 'vp9' | 'av1';
  frameNumber?: number;
  isKeyframe?: boolean;
}

export interface TensorData {
  data: Uint8Array;
  shape: number[];
  dtype: 'f32' | 'f16' | 'i32' | 'i8' | 'u8';
}

export interface NumpyArray {
  data: Uint8Array;
  shape: number[];
  dtype: string;
  strides: number[];
}

export interface ControlMessage {
  type: 'start' | 'stop' | 'cancel' | 'flush' | 'config_update' | 'custom';
  timestamp?: number;
  segmentId?: string;
  metadata?: Record<string, unknown>;
  cancelRange?: { start: number; end: number };
}

export type RuntimeData =
  | { type: 'audio'; data: AudioData }
  | { type: 'video'; data: VideoData }
  | { type: 'json'; data: Record<string, unknown> }
  | { type: 'text'; data: string }
  | { type: 'binary'; data: Uint8Array }
  | { type: 'tensor'; data: TensorData }
  | { type: 'numpy'; data: NumpyArray }
  | { type: 'control'; data: ControlMessage };
`;

const NODE_BUILDER_BASE = `/**
 * Base class for all node builders.
 * Provides type-safe construction of pipeline nodes.
 */
export abstract class NodeBuilder<T extends NodeType = NodeType, C = unknown> {
  readonly id: string;
  readonly nodeType: T;
  readonly config?: C;

  constructor(id: string, nodeType: T, config?: C) {
    this.id = id;
    this.nodeType = nodeType;
    this.config = config;
  }

  /** Convert to PipelineNode format for manifest */
  toPipelineNode(): PipelineNode<T> {
    return {
      id: this.id,
      nodeType: this.nodeType,
      config: this.config as T extends keyof NodeConfigMap ? NodeConfigMap[T] : Record<string, unknown>,
    };
  }

  /** Create connection to another node */
  connectTo(target: NodeBuilder<NodeType, unknown> | string, sourcePort?: string, destinationPort?: string): PipelineConnection {
    const targetId = typeof target === 'string' ? target : target.id;
    return {
      source: this.id,
      sourcePort,
      destination: targetId,
      destinationPort,
    };
  }
}

/** Connection between pipeline nodes */
export interface PipelineConnection {
  source: string;
  sourcePort?: string;
  destination: string;
  destinationPort?: string;
}
`;

const PIPELINE_BUILDER = `/**
 * Fluent pipeline builder for constructing manifests.
 *
 * @example
 * \`\`\`typescript
 * const manifest = new PipelineBuilder('1.0')
 *   .name('My Pipeline')
 *   .add(new SileroVAD('vad', { threshold: 0.6 }))
 *   .add(new WhisperNode('whisper', { model: 'base' }))
 *   .connect('vad', 'whisper')
 *   .build();
 * \`\`\`
 */
export class PipelineBuilder {
  private version: string;
  private metadata: { name?: string; description?: string; [key: string]: unknown } = {};
  private nodes: PipelineNode[] = [];
  private connections: PipelineConnection[] = [];

  constructor(version: string = '1.0') {
    this.version = version;
  }

  /** Set pipeline name */
  name(name: string): this {
    this.metadata.name = name;
    return this;
  }

  /** Set pipeline description */
  description(description: string): this {
    this.metadata.description = description;
    return this;
  }

  /** Add metadata key-value */
  meta(key: string, value: unknown): this {
    this.metadata[key] = value;
    return this;
  }

  /** Add a node to the pipeline */
  add(node: NodeBuilder<NodeType, unknown>): this {
    this.nodes.push(node.toPipelineNode());
    return this;
  }

  /** Add a raw node definition */
  addRaw<T extends NodeType>(node: PipelineNode<T>): this {
    this.nodes.push(node);
    return this;
  }

  /** Connect two nodes */
  connect(
    source: NodeBuilder<NodeType, unknown> | string,
    destination: NodeBuilder<NodeType, unknown> | string,
    sourcePort?: string,
    destinationPort?: string
  ): this {
    const sourceId = typeof source === 'string' ? source : source.id;
    const destId = typeof destination === 'string' ? destination : destination.id;
    this.connections.push({
      source: sourceId,
      sourcePort,
      destination: destId,
      destinationPort,
    });
    return this;
  }

  /** Build the final pipeline manifest */
  build(): PipelineManifest {
    return {
      version: this.version,
      metadata: Object.keys(this.metadata).length > 0 ? this.metadata : undefined,
      nodes: this.nodes,
      connections: this.connections,
    };
  }

  /** Convert to JSON string */
  toJson(): string {
    return JSON.stringify(this.build(), null, 2);
  }
}
`;

/**
 * Convert a JSON Schema property to TypeScript type
 */
function jsonSchemaToTsType(schema) {
  if (!schema || typeof schema !== 'object') {
    return 'unknown';
  }

  const type = schema.type;

  if (type === 'string') {
    if (schema.enum && Array.isArray(schema.enum)) {
      return schema.enum.map((v) => `'${v}'`).join(' | ');
    }
    return 'string';
  }

  if (type === 'number' || type === 'integer') {
    return 'number';
  }

  if (type === 'boolean') {
    return 'boolean';
  }

  if (type === 'array') {
    if (schema.items) {
      return `${jsonSchemaToTsType(schema.items)}[]`;
    }
    return 'unknown[]';
  }

  if (type === 'object') {
    if (schema.properties) {
      return jsonSchemaToTsInterface(schema);
    }
    if (schema.additionalProperties) {
      return `Record<string, ${jsonSchemaToTsType(schema.additionalProperties)}>`;
    }
    return 'Record<string, unknown>';
  }

  if (type === 'null') {
    return 'null';
  }

  // Handle oneOf/anyOf
  const variants = schema.oneOf || schema.anyOf;
  if (variants && Array.isArray(variants)) {
    return variants.map(jsonSchemaToTsType).join(' | ');
  }

  return 'unknown';
}

/**
 * Convert a JSON Schema object to TypeScript interface body
 */
function jsonSchemaToTsInterface(schema) {
  if (!schema || !schema.properties) {
    return 'Record<string, unknown>';
  }

  const required = schema.required || [];
  const fields = [];

  for (const [key, prop] of Object.entries(schema.properties)) {
    const optional = required.includes(key) ? '' : '?';
    const tsType = jsonSchemaToTsType(prop);
    const desc = prop.description ? `  /** ${prop.description} */\n` : '';
    fields.push(`${desc}  ${key}${optional}: ${tsType};`);
  }

  return `{\n${fields.join('\n')}\n}`;
}

/**
 * Generate TypeScript definitions from schemas
 */
function generateTypescript(schemas) {
  let ts = '';

  // Header
  ts += '// Auto-generated by RemoteMedia SDK - DO NOT EDIT\n';
  ts += '// Run `npm run generate-types` to regenerate\n\n';

  // Base RuntimeData types
  ts += RUNTIME_DATA_TYPES;
  ts += '\n';

  // Generate config types for each node that has a config schema
  for (const schema of schemas) {
    if (schema.configSchema) {
      const configSchema = JSON.parse(schema.configSchema);
      const desc = schema.description ? `/** ${schema.description} - Configuration */\n` : '';
      ts += `${desc}export interface ${schema.nodeType}Config ${jsonSchemaToTsInterface(configSchema)}\n\n`;
    }
  }

  // Node metadata type
  ts += '/** Node metadata from registry */\n';
  ts += 'export interface NodeMetadata {\n';
  ts += '  nodeType: string;\n';
  ts += '  description?: string;\n';
  ts += '  category?: string;\n';
  ts += '  accepts: RuntimeDataType[];\n';
  ts += '  produces: RuntimeDataType[];\n';
  ts += '  isPython: boolean;\n';
  ts += '  streaming: boolean;\n';
  ts += '  multiOutput: boolean;\n';
  ts += '}\n\n';

  // Node type union
  ts += '/** All registered node types */\n';
  ts += 'export type NodeType =\n';
  for (let i = 0; i < schemas.length; i++) {
    const sep = i < schemas.length - 1 ? '' : ';';
    ts += `  | '${schemas[i].nodeType}'${sep}\n`;
  }
  ts += '\n';

  // Config type map
  ts += '/** Node type to config type mapping */\n';
  ts += 'export interface NodeConfigMap {\n';
  for (const schema of schemas) {
    if (schema.configSchema) {
      ts += `  '${schema.nodeType}': ${schema.nodeType}Config;\n`;
    } else {
      ts += `  '${schema.nodeType}': Record<string, unknown>;\n`;
    }
  }
  ts += '}\n\n';

  // Typed pipeline node
  ts += '/** Pipeline node with typed config */\n';
  ts += 'export interface PipelineNode<T extends NodeType = NodeType> {\n';
  ts += '  id: string;\n';
  ts += '  nodeType: T;\n';
  ts += '  config?: T extends keyof NodeConfigMap ? NodeConfigMap[T] : Record<string, unknown>;\n';
  ts += '}\n\n';

  // Pipeline manifest
  ts += '/** Pipeline manifest */\n';
  ts += 'export interface PipelineManifest {\n';
  ts += '  version: string;\n';
  ts += '  metadata?: { name?: string; description?: string; [key: string]: unknown };\n';
  ts += '  nodes: PipelineNode[];\n';
  ts += '  connections: Array<{\n';
  ts += '    source: string;\n';
  ts += '    sourcePort?: string;\n';
  ts += '    destination: string;\n';
  ts += '    destinationPort?: string;\n';
  ts += '  }>;\n';
  ts += '}\n\n';

  // Runtime node schemas array
  ts += '/** All node schemas (for runtime introspection) */\n';
  ts += 'export const nodeSchemas: NodeMetadata[] = ';
  const schemasJson = schemas.map((s) => ({
    nodeType: s.nodeType,
    description: s.description,
    category: s.category,
    accepts: s.accepts,
    produces: s.produces,
    isPython: s.isPython,
    streaming: s.streaming,
    multiOutput: s.multiOutput,
  }));
  ts += JSON.stringify(schemasJson, null, 2);
  ts += ';\n\n';

  // Node builder classes
  ts += '// =============================================================================\n';
  ts += '// Node Builder Classes\n';
  ts += '// =============================================================================\n\n';

  ts += NODE_BUILDER_BASE;
  ts += '\n';

  // Generate a class for each node type
  for (const schema of schemas) {
    const configType = schema.configSchema ? `${schema.nodeType}Config` : 'Record<string, unknown>';

    // Class documentation
    const desc = schema.description || `${schema.nodeType} node builder`;
    ts += '/**\n';
    ts += ` * ${desc}\n`;
    ts += ' *\n';
    ts += ' * @example\n';
    ts += ' * ```typescript\n';
    if (schema.configSchema) {
      ts += ` * const node = new ${schema.nodeType}('my-${schema.nodeType.toLowerCase()}', { });\n`;
    } else {
      ts += ` * const node = new ${schema.nodeType}('my-${schema.nodeType.toLowerCase()}');\n`;
    }
    ts += ' * pipeline.addNode(node);\n';
    ts += ' * ```\n';
    ts += ' */\n';

    // Class definition
    ts += `export class ${schema.nodeType} extends NodeBuilder<'${schema.nodeType}', ${configType}> {\n`;

    // Static metadata
    ts += `  static readonly nodeType = '${schema.nodeType}' as const;\n`;

    // Static accepts/produces
    const accepts = schema.accepts.map((t) => `'${t}'`).join(', ');
    const produces = schema.produces.map((t) => `'${t}'`).join(', ');
    ts += `  static readonly accepts: RuntimeDataType[] = [${accepts}];\n`;
    ts += `  static readonly produces: RuntimeDataType[] = [${produces}];\n`;

    // Constructor
    ts += '\n';
    ts += `  constructor(id: string, config?: ${configType}) {\n`;
    ts += `    super(id, '${schema.nodeType}', config);\n`;
    ts += '  }\n';

    ts += '}\n\n';
  }

  // Nodes namespace
  ts += '/** Namespace containing all node builder classes */\n';
  ts += 'export const Nodes = {\n';
  for (let i = 0; i < schemas.length; i++) {
    const comma = i < schemas.length - 1 ? ',' : '';
    ts += `  ${schemas[i].nodeType}${comma}\n`;
  }
  ts += '} as const;\n\n';

  // Pipeline builder
  ts += PIPELINE_BUILDER;

  return ts;
}

// =============================================================================
// Main
// =============================================================================

function main() {
  const args = process.argv.slice(2);
  const toStdout = args.includes('--stdout');
  const toJson = args.includes('--json');

  try {
    // Try to load the native module
    const native = require('..');

    // Check if the schema functions exist
    if (typeof native.getNodeSchemas !== 'function') {
      console.error('Error: getNodeSchemas function not found.');
      console.error('Make sure the native module was built with the napi feature.');
      console.error('Run: npm run build');
      process.exit(1);
    }

    // Get schemas from native module
    const schemas = native.getNodeSchemas();

    if (toJson) {
      // Output JSON schema
      console.log(JSON.stringify(schemas, null, 2));
      return;
    }

    // Generate TypeScript definitions
    const typescript = generateTypescript(schemas);

    if (toStdout) {
      console.log(typescript);
    } else {
      // Write to file
      fs.writeFileSync(OUTPUT_FILE, typescript);
      console.log(`Generated: ${OUTPUT_FILE}`);

      // Also write JSON schemas for runtime introspection
      fs.writeFileSync(OUTPUT_JSON, JSON.stringify(schemas, null, 2));
      console.log(`Generated: ${OUTPUT_JSON}`);
    }

    // Print summary
    const nodeTypes = schemas.map((s) => s.nodeType);
    console.log(`\nRegistered ${nodeTypes.length} node types:`);
    nodeTypes.forEach((t) => console.log(`  - ${t}`));
  } catch (err) {
    if (err.code === 'MODULE_NOT_FOUND') {
      console.error('Error: Native module not found.');
      console.error('Make sure to build the native module first:');
      console.error('  npm run build');
      process.exit(1);
    }
    throw err;
  }
}

main();
