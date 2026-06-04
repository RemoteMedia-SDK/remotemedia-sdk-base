// Data — high-level wrapper around NapiRuntimeData for the in-proc control
// surface. Mirrors `clients/python/remotemedia/control/data.py`.
//
// Internal representation:
//   `#native: NapiRuntimeData` — backs every constructor and accessor below.
//   The gRPC path (which would carry a protobuf `_buf`) is deferred to a
//   future Node.js gRPC client and not exercised here.
//
// The constructors mirror the Python class:
//   Data.fromText(s)              → text
//   Data.fromJson(obj)            → json
//   Data.fromBytes(buf)           → binary
//   Data.fromAudio(samples, sr, ch) → audio
//
// Accessors:
//   .kind          — 'text' | 'json' | 'audio' | 'binary' | 'tensor' | 'video' | 'file' | 'numpy' | 'control' | 'image'
//   .textValue     — string for kind=='text'
//   .jsonValue     — parsed object for kind=='json'
//   .audioBuffer   — { samples: Float32Array, sampleRate, channels } for kind=='audio'
//   .asBuffer()    — raw bytes for binary/text-as-bytes

const native = require('..');

// NapiRuntimeData.dataType returns numeric codes (see RuntimeData enum in pipeline.rs)
const KIND_BY_TYPE = {
  1: 'audio',
  2: 'video',
  3: 'text',
  4: 'tensor',
  5: 'control',
  6: 'numpy',
  7: 'json',
  8: 'binary',
  9: 'file',
  10: 'image',
};

class Data {
  /**
   * @param {object} opts
   * @param {object} [opts.native] - a NapiRuntimeData instance from the native binding
   */
  constructor({ native: napiData } = {}) {
    if (!napiData) {
      throw new TypeError('Data must be constructed via Data.fromText/fromJson/fromBytes/fromAudio');
    }
    this._native = napiData;
  }

  static fromText(s) {
    if (typeof s !== 'string') {
      throw new TypeError(`Data.fromText expects a string, got ${typeof s}`);
    }
    return new Data({ native: native.NapiRuntimeData.text(s) });
  }

  static fromJson(obj) {
    const s = JSON.stringify(obj);
    return new Data({ native: native.NapiRuntimeData.json(s) });
  }

  static fromBytes(buf) {
    const b = Buffer.isBuffer(buf) ? buf : Buffer.from(buf);
    return new Data({ native: native.NapiRuntimeData.binary(b) });
  }

  /**
   * @param {Float32Array | number[] | Buffer} samples - audio samples (f32)
   * @param {number} sampleRate
   * @param {number} channels
   */
  static fromAudio(samples, sampleRate, channels) {
    let buf;
    if (Buffer.isBuffer(samples)) {
      buf = samples;
    } else if (samples instanceof Float32Array) {
      buf = Buffer.from(samples.buffer, samples.byteOffset, samples.byteLength);
    } else if (Array.isArray(samples)) {
      const f32 = new Float32Array(samples);
      buf = Buffer.from(f32.buffer, f32.byteOffset, f32.byteLength);
    } else {
      throw new TypeError(`Data.fromAudio expects Float32Array | number[] | Buffer, got ${typeof samples}`);
    }
    return new Data({
      native: native.NapiRuntimeData.audio(buf, sampleRate, channels),
    });
  }

  /**
   * Internal — wrap a NapiRuntimeData arriving from a subscription / intercept.
   * @internal
   */
  static _fromNative(napiData) {
    return new Data({ native: napiData });
  }

  /**
   * Internal — unwrap for publish path. The native side accepts a
   * `&NapiRuntimeData` reference.
   * @internal
   */
  _toNative() {
    return this._native;
  }

  get kind() {
    return KIND_BY_TYPE[this._native.dataType] || 'unknown';
  }

  get textValue() {
    return this._native.getText();
  }

  get jsonValue() {
    return JSON.parse(this._native.getJson());
  }

  get audioBuffer() {
    const samplesBuf = this._native.getAudioSamples();
    const numSamples = this._native.getAudioNumSamples();
    const samples = new Float32Array(
      samplesBuf.buffer,
      samplesBuf.byteOffset,
      numSamples,
    );
    return {
      samples,
      sampleRate: this._native.getAudioSampleRate(),
      channels: this._native.getAudioChannels(),
    };
  }

  asBuffer() {
    // For text we return UTF-8 bytes; for binary the raw buffer; otherwise throw.
    switch (this.kind) {
      case 'text':
        return Buffer.from(this._native.getTextBuffer());
      case 'binary':
        return Buffer.from(this._native.getBinary());
      default:
        throw new Error(`asBuffer() not supported for kind=${this.kind}`);
    }
  }
}

module.exports = { Data };
