/** Published native payloads are immutable owning handles, not writable JS views. */
export declare class NapiRuntimeData {
  private constructor();
  static audio(samples: Buffer, sampleRate: number, channels: number, requireZeroCopy?: boolean): NapiRuntimeData;
  static binary(data: Buffer, requireZeroCopy?: boolean): NapiRuntimeData;
  static video(data: Buffer, width: number, height: number, format: number, codec: number | undefined,
    frameNumber: number, isKeyframe: boolean, requireZeroCopy?: boolean): NapiRuntimeData;
  static tensor(data: Buffer, shape: number[], dtype: number, requireZeroCopy?: boolean): NapiRuntimeData;
  static text(value: string): NapiRuntimeData;
  static json(value: string): NapiRuntimeData;
  static file(path: string, filename?: string, mimeType?: string, size?: number,
    offset?: number, length?: number, streamId?: string): NapiRuntimeData;
  readonly dataType: number;
  sharesBufferWith(other: NapiRuntimeData): boolean;
  sliceBinary(start: number, end: number): NapiRuntimeData;
  readByte(index: number): number;
  readAudioSample(index: number): number;
  /** Independent mutable copies. Passing true rejects a nonempty copy. */
  getAudioSamples(requireZeroCopy?: boolean): Buffer;
  getVideoPixels(requireZeroCopy?: boolean): Buffer;
  getBinary(requireZeroCopy?: boolean): Buffer;
  getTensorData(requireZeroCopy?: boolean): Buffer;
  getTextBuffer(requireZeroCopy?: boolean): Buffer;
  getNumpyData(requireZeroCopy?: boolean): Buffer;
  getAudioNumSamples(): number;
  getAudioSampleRate(): number;
  getAudioChannels(): number;
  getVideoWidth(): number;
  getVideoHeight(): number;
  getTensorShape(): number[];
  getNumpyShape(): number[];
  getNumpyDtype(): string;
  getText(): string;
  getJson(): string;
  getFilePath(): string;
  getFileFilename(): string | null;
  getFileMimeType(): string | null;
  getFileSize(): number | null;
  getFileOffset(): number | null;
  getFileLength(): number | null;
  getFileStreamId(): string | null;
}

export declare class NapiOutputAllocator {
  private constructor();
  allocateBytes(length: number): NapiByteLoan;
  allocateAudio(samples: number, sampleRate: number, channels: number): NapiAudioLoan;
}
/** Scalar generation writes directly into the selected provider. No writable alias escapes. */
export declare class NapiByteLoan {
  private constructor();
  set(index: number, value: number): void;
  fill(value: number): void;
  /** Consumes the loan. Later writes/publication throw. */
  publish(): NapiRuntimeData;
}
export declare class NapiAudioLoan {
  private constructor();
  set(index: number, value: number): void;
  fill(value: number): void;
  publish(): NapiRuntimeData;
}
export declare class PipelineEvent {
  private constructor();
  readonly kind: string;
  readonly data: NapiRuntimeData;
}
export declare class NapiStreamingSession {
  private constructor();
  readonly sessionId: string;
  outputAllocator(): NapiOutputAllocator;
  sendInput(data: NapiRuntimeData): Promise<void>;
  recvAudio(): Promise<NapiRuntimeData | null>;
  recvVideo(): Promise<NapiRuntimeData | null>;
  recvData(): Promise<NapiRuntimeData | null>;
  recvOutput(): Promise<PipelineEvent | null>;
  signalInputComplete(): Promise<void>;
  close(): Promise<void>;
}
export declare function createStreamingSession(manifestJson: string): Promise<NapiStreamingSession>;
/** JSON process-wide ledger for this buffer boundary only; excludes other adapters. */
export declare function nodejsBufferCopyReport(): string;
