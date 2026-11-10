/**
 * Shared TypeScript types for Node.js FFI tests
 *
 * These types mirror the napi-rs exported types from @matbee/remotemedia-native
 */

export interface NapiRuntimeData {
  dataType: number;
  getAudioSamples(): Buffer;
  getAudioSampleRate(): number;
  getAudioChannels(): number;
  getAudioNumSamples(): number;
  getVideoPixels(): Buffer;
  getVideoWidth(): number;
  getVideoHeight(): number;
  getText(): string;
  getBinary(): Buffer;
  getTensorData(): Buffer;
  getTensorShape(): number[];
  getJson(): string;
}

export interface PipelineOutput {
  size: number;
  getNodeIds(): string[];
  get(nodeId: string): NapiRuntimeData | null;
  has(nodeId: string): boolean;
  /** Transport ledger of the execution (JSON), including output snapshots. */
  copyReport(): string;
  /** Per-node materialization ledgers (JSON object keyed by node id). */
  nodeCopyReports(): string;
}

/** Copy policy and retention budget for a pipeline session. */
export interface PipelineBufferConfig {
  policy: 'require_zero_copy' | 'allow_copies';
  nodeRetentionOwners: number;
  nodeRetentionBytes: number;
}

export interface StreamSession {
  readonly sessionId: string;
  readonly isActive: boolean;
  sendInput(data: NapiRuntimeData): Promise<void>;
  recvOutput(): Promise<NapiRuntimeData | null>;
  close(): Promise<void>;
  copyReport(): string;
}

export interface NativeModule {
  // Zero-copy runtime data factory
  NapiRuntimeData: {
    audio(samplesBuffer: Buffer, sampleRate: number, channels: number): NapiRuntimeData;
    video(
      pixelData: Buffer,
      width: number,
      height: number,
      format: number,
      codec: number | undefined,
      frameNumber: number,
      isKeyframe: boolean
    ): NapiRuntimeData;
    text(text: string): NapiRuntimeData;
    binary(data: Buffer): NapiRuntimeData;
    tensor(data: Buffer, shape: number[], dtype: number): NapiRuntimeData;
    json(jsonString: string): NapiRuntimeData;
  };

  // Pipeline execution
  // Runs one session: each input goes to the named source node; the result
  // holds the last output of every sink.
  executePipeline(
    manifestJson: string,
    inputs: Record<string, NapiRuntimeData>,
    pipelineBuffers?: PipelineBufferConfig
  ): Promise<PipelineOutput>;

  executePipelineWithSession(
    manifestJson: string,
    inputs: Record<string, NapiRuntimeData>,
    sessionId: string,
    pipelineBuffers?: PipelineBufferConfig
  ): Promise<PipelineOutput>;

  // Streaming API
  createStreamSession(
    manifestJson: string,
    pipelineBuffers?: PipelineBufferConfig
  ): Promise<StreamSession>;

  // Runtime info
  getRuntimeVersion(): string;
  isRuntimeAvailable(): boolean;
  isNativeLoaded(): boolean;
  getLoadError(): Error | null;

  // Node schema registry
  getNodeSchemas(): NapiNodeSchema[];
  getNodeSchema(nodeType: string): NapiNodeSchema | null;
  getNodeParameters(nodeType: string): NapiNodeParameter[];
  getNodeSchemasJson(): string;
  getNodeSchemaJson(nodeType: string): string | null;
  getNodeTypes(): string[];
  getNodeTypesByCategory(category: string): string[];
  hasNodeType(nodeType: string): boolean;
  getNodeCategories(): string[];
  validateManifest(manifestJson: string): string[];
}

export interface NapiNodeParameter {
  name: string;
  paramType: string;
  description?: string;
  defaultValue?: string;
  required: boolean;
  enumValues?: string;
  minimum?: number;
  maximum?: number;
}

export interface NapiNodeCapabilities {
  parallelizable: boolean;
  batchAware: boolean;
  supportsControl: boolean;
  latencyClass: number;
}

export interface NapiNodeSchema {
  nodeType: string;
  description?: string;
  category?: string;
  accepts: string[];
  produces: string[];
  parameters: NapiNodeParameter[];
  configSchema?: string;
  configDefaults?: string;
  isPython: boolean;
  streaming: boolean;
  multiOutput: boolean;
  capabilities?: NapiNodeCapabilities;
}

/**
 * Load the native module, returning null if loading fails
 */
export function loadNativeModule(): { native: NativeModule | null; loadError: Error | null } {
  let native: NativeModule | null = null;
  let loadError: Error | null = null;

  try {
    native = require('..') as NativeModule;
  } catch (e) {
    loadError = e as Error;
  }

  return { native, loadError };
}
