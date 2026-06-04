/**
 * Protobuf DataBuffer Encoder/Decoder Utilities
 *
 * Browser and Node.js compatible utilities for encoding/decoding
 * DataBuffer protobuf messages for WebRTC data channel communication.
 */

// =============================================================================
// Decoded Types
// =============================================================================

export interface DecodedTextBuffer {
  type: 'text';
  textData: string;
  encoding: string;
}

export interface DecodedJsonBuffer {
  type: 'json';
  jsonPayload: string;
  schemaType?: string;
}

export interface DecodedAudioBuffer {
  type: 'audio';
  samples: Float32Array;
  sampleRate: number;
  channels: number;
}

export interface DecodedUnknown {
  type: 'unknown';
}

export type DecodedDataBuffer =
  | DecodedTextBuffer
  | DecodedJsonBuffer
  | DecodedAudioBuffer
  | DecodedUnknown;

// =============================================================================
// Encoder Functions
// =============================================================================

/**
 * Encode a text string as a Protobuf DataBuffer with TextBuffer variant
 * @param text - The text string to encode
 * @returns Protobuf-encoded DataBuffer
 */
export function encodeTextData(text: string): ArrayBuffer;

/**
 * Encode a JSON object as a Protobuf DataBuffer with JsonData variant
 * @param data - The object to encode as JSON
 * @param schemaType - Optional schema type identifier
 * @returns Protobuf-encoded DataBuffer
 */
export function encodeJsonData(data: unknown, schemaType?: string): ArrayBuffer;

// =============================================================================
// Decoder Functions
// =============================================================================

/**
 * Decode a DataBuffer message from binary data
 * @param data - The binary data to decode
 * @returns Decoded DataBuffer
 */
export function decodeDataBuffer(data: ArrayBuffer | Uint8Array): DecodedDataBuffer;

/**
 * Decode a TextBuffer message
 * @param data - The binary data to decode
 * @returns Decoded TextBuffer
 */
export function decodeTextBuffer(data: Uint8Array): DecodedTextBuffer;

/**
 * Decode a JsonData message
 * @param data - The binary data to decode
 * @returns Decoded JsonBuffer
 */
export function decodeJsonData(data: Uint8Array): DecodedJsonBuffer;

/**
 * Decode an AudioBuffer message
 * @param data - The binary data to decode
 * @returns Decoded AudioBuffer
 */
export function decodeAudioBuffer(data: Uint8Array): DecodedAudioBuffer;

/**
 * Parse JSON from a decoded DataBuffer
 * @param decoded - The decoded DataBuffer
 * @returns Parsed JSON object or null
 */
export function parseJsonFromDataBuffer<T = unknown>(decoded: DecodedDataBuffer): T | null;
