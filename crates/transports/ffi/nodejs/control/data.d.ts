/// <reference types="node" />

export type DataKind =
  | 'text'
  | 'json'
  | 'audio'
  | 'binary'
  | 'tensor'
  | 'video'
  | 'file'
  | 'numpy'
  | 'control'
  | 'image'
  | 'unknown';

export interface AudioBufferView {
  samples: Float32Array;
  sampleRate: number;
  channels: number;
}

/**
 * High-level wrapper around the native `NapiRuntimeData` class.
 *
 * Mirrors `clients/python/remotemedia/control/data.py`.
 */
export class Data {
  /** @internal */
  private constructor(opts: { native: any });

  static fromText(s: string): Data;
  static fromJson(obj: unknown): Data;
  static fromBytes(buf: Buffer | Uint8Array): Data;
  static fromAudio(
    samples: Float32Array | number[] | Buffer,
    sampleRate: number,
    channels: number,
  ): Data;

  /** @internal — wrap a `NapiRuntimeData` arriving from native. */
  static _fromNative(napiData: any): Data;
  /** @internal — unwrap for the publish path. */
  _toNative(): any;

  readonly kind: DataKind;
  readonly textValue: string;
  readonly jsonValue: unknown;
  readonly audioBuffer: AudioBufferView;

  asBuffer(): Buffer;
}
