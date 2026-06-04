/**
 * Protobuf DataBuffer Encoder/Decoder Utilities
 *
 * Browser and Node.js compatible utilities for encoding/decoding
 * DataBuffer protobuf messages for WebRTC data channel communication.
 *
 * Based on transports/webrtc/protos/common.proto
 */

// =============================================================================
// Encoder Functions
// =============================================================================

/**
 * Calculate the length of a varint encoding
 * @param {number} value
 * @returns {number}
 */
function varintLength(value) {
  if (value < 0x80) return 1;
  if (value < 0x4000) return 2;
  if (value < 0x200000) return 3;
  if (value < 0x10000000) return 4;
  return 5;
}

/**
 * Write a varint to a buffer
 * @param {Uint8Array} buffer
 * @param {number} offset
 * @param {number} value
 * @returns {number} New offset
 */
function writeVarint(buffer, offset, value) {
  while (value >= 0x80) {
    buffer[offset++] = (value & 0x7f) | 0x80;
    value >>>= 7;
  }
  buffer[offset++] = value;
  return offset;
}

/**
 * Encode a text string as a Protobuf DataBuffer with TextBuffer variant
 *
 * Wire format:
 * - DataBuffer.text (field 5): tag=0x2a
 * - TextBuffer.text_data (field 1): tag=0x0a
 * - TextBuffer.encoding (field 2): tag=0x12
 *
 * @param {string} text - The text string to encode
 * @returns {ArrayBuffer} Protobuf-encoded DataBuffer
 */
function encodeTextData(text) {
  const encoder = new TextEncoder();
  const textBytes = encoder.encode(text);
  const encoding = 'utf-8';
  const encodingBytes = encoder.encode(encoding);

  // Calculate sizes
  const textDataSize = 1 + varintLength(textBytes.length) + textBytes.length;
  const encodingSize = 1 + varintLength(encodingBytes.length) + encodingBytes.length;
  const textBufferSize = textDataSize + encodingSize;
  const dataBufferSize = 1 + varintLength(textBufferSize) + textBufferSize;

  const buffer = new Uint8Array(dataBufferSize);
  let offset = 0;

  // DataBuffer.text (field 5, wire type 2)
  buffer[offset++] = (5 << 3) | 2;
  offset = writeVarint(buffer, offset, textBufferSize);

  // TextBuffer.text_data (field 1, wire type 2)
  buffer[offset++] = (1 << 3) | 2;
  offset = writeVarint(buffer, offset, textBytes.length);
  buffer.set(textBytes, offset);
  offset += textBytes.length;

  // TextBuffer.encoding (field 2, wire type 2)
  buffer[offset++] = (2 << 3) | 2;
  offset = writeVarint(buffer, offset, encodingBytes.length);
  buffer.set(encodingBytes, offset);

  return buffer.buffer;
}

/**
 * Encode a JSON object as a Protobuf DataBuffer with JsonData variant
 *
 * Wire format:
 * - DataBuffer.json (field 6): tag=0x32
 * - JsonData.json_payload (field 1): tag=0x0a
 * - JsonData.schema_type (field 2): tag=0x12 [optional]
 *
 * @param {unknown} data - The object to encode as JSON
 * @param {string} [schemaType] - Optional schema type identifier
 * @returns {ArrayBuffer} Protobuf-encoded DataBuffer
 */
function encodeJsonData(data, schemaType) {
  const encoder = new TextEncoder();
  const jsonString = JSON.stringify(data);
  const jsonBytes = encoder.encode(jsonString);

  // Calculate JsonData message size
  const jsonPayloadSize = 1 + varintLength(jsonBytes.length) + jsonBytes.length;
  let schemaTypeSize = 0;
  let schemaBytes = null;

  if (schemaType) {
    schemaBytes = encoder.encode(schemaType);
    schemaTypeSize = 1 + varintLength(schemaBytes.length) + schemaBytes.length;
  }

  const jsonDataSize = jsonPayloadSize + schemaTypeSize;
  const dataBufferSize = 1 + varintLength(jsonDataSize) + jsonDataSize;

  const buffer = new Uint8Array(dataBufferSize);
  let offset = 0;

  // DataBuffer.json (field 6, wire type 2)
  buffer[offset++] = (6 << 3) | 2;
  offset = writeVarint(buffer, offset, jsonDataSize);

  // JsonData.json_payload (field 1, wire type 2)
  buffer[offset++] = (1 << 3) | 2;
  offset = writeVarint(buffer, offset, jsonBytes.length);
  buffer.set(jsonBytes, offset);
  offset += jsonBytes.length;

  // JsonData.schema_type (field 2, wire type 2) if provided
  if (schemaBytes) {
    buffer[offset++] = (2 << 3) | 2;
    offset = writeVarint(buffer, offset, schemaBytes.length);
    buffer.set(schemaBytes, offset);
  }

  return buffer.buffer;
}

// =============================================================================
// Decoder Functions
// =============================================================================

/**
 * Read a varint from a Uint8Array
 * @param {Uint8Array} buffer
 * @param {number} offset
 * @returns {[number, number]} [value, bytesRead]
 */
function readVarint(buffer, offset) {
  let result = 0;
  let shift = 0;
  let bytesRead = 0;

  while (offset + bytesRead < buffer.length) {
    const byte = buffer[offset + bytesRead];
    result |= (byte & 0x7f) << shift;
    bytesRead++;
    if ((byte & 0x80) === 0) {
      break;
    }
    shift += 7;
  }

  return [result, bytesRead];
}

/**
 * Decode a DataBuffer message from a Uint8Array
 *
 * DataBuffer oneof fields:
 * - field 1: audio (AudioBuffer)
 * - field 2: video (VideoBuffer)
 * - field 3: image (ImageBuffer)
 * - field 4: control (ControlBuffer)
 * - field 5: text (TextBuffer)
 * - field 6: json (JsonData)
 * - field 7: bytes (BytesBuffer)
 *
 * @param {ArrayBuffer|Uint8Array} data
 * @returns {{type: string, [key: string]: unknown}}
 */
function decodeDataBuffer(data) {
  const buffer = data instanceof Uint8Array ? data : new Uint8Array(data);
  let offset = 0;

  while (offset < buffer.length) {
    const [tag, tagBytes] = readVarint(buffer, offset);
    offset += tagBytes;

    const fieldNumber = tag >>> 3;
    const wireType = tag & 0x07;

    if (wireType === 2) {
      // Length-delimited
      const [length, lengthBytes] = readVarint(buffer, offset);
      offset += lengthBytes;

      const fieldData = buffer.slice(offset, offset + length);
      offset += length;

      switch (fieldNumber) {
        case 5: // text (TextBuffer)
          return decodeTextBuffer(fieldData);
        case 6: // json (JsonData)
          return decodeJsonData(fieldData);
        case 1: // audio (AudioBuffer)
          return decodeAudioBuffer(fieldData);
      }
    } else if (wireType === 0) {
      // Varint - skip
      const [, varBytes] = readVarint(buffer, offset);
      offset += varBytes;
    } else if (wireType === 5) {
      // 32-bit fixed
      offset += 4;
    } else if (wireType === 1) {
      // 64-bit fixed
      offset += 8;
    }
  }

  return { type: 'unknown' };
}

/**
 * Decode a TextBuffer message
 * @param {Uint8Array} data
 * @returns {{type: 'text', textData: string, encoding: string}}
 */
function decodeTextBuffer(data) {
  const decoder = new TextDecoder();
  let textData = '';
  let encoding = 'utf-8';
  let offset = 0;

  while (offset < data.length) {
    const [tag, tagBytes] = readVarint(data, offset);
    offset += tagBytes;

    const fieldNumber = tag >>> 3;
    const wireType = tag & 0x07;

    if (wireType === 2) {
      const [length, lengthBytes] = readVarint(data, offset);
      offset += lengthBytes;

      const fieldData = data.slice(offset, offset + length);
      offset += length;

      if (fieldNumber === 1) {
        textData = decoder.decode(fieldData);
      } else if (fieldNumber === 2) {
        encoding = decoder.decode(fieldData);
      }
    }
  }

  return { type: 'text', textData, encoding };
}

/**
 * Decode a JsonData message
 * @param {Uint8Array} data
 * @returns {{type: 'json', jsonPayload: string, schemaType?: string}}
 */
function decodeJsonData(data) {
  const decoder = new TextDecoder();
  let jsonPayload = '';
  let schemaType;
  let offset = 0;

  while (offset < data.length) {
    const [tag, tagBytes] = readVarint(data, offset);
    offset += tagBytes;

    const fieldNumber = tag >>> 3;
    const wireType = tag & 0x07;

    if (wireType === 2) {
      const [length, lengthBytes] = readVarint(data, offset);
      offset += lengthBytes;

      const fieldData = data.slice(offset, offset + length);
      offset += length;

      if (fieldNumber === 1) {
        jsonPayload = decoder.decode(fieldData);
      } else if (fieldNumber === 2) {
        schemaType = decoder.decode(fieldData);
      }
    }
  }

  return { type: 'json', jsonPayload, schemaType };
}

/**
 * Decode an AudioBuffer message
 * @param {Uint8Array} data
 * @returns {{type: 'audio', samples: Float32Array, sampleRate: number, channels: number}}
 */
function decodeAudioBuffer(data) {
  let samples = new Float32Array(0);
  let sampleRate = 48000;
  let channels = 1;
  let offset = 0;

  while (offset < data.length) {
    const [tag, tagBytes] = readVarint(data, offset);
    offset += tagBytes;

    const fieldNumber = tag >>> 3;
    const wireType = tag & 0x07;

    if (wireType === 2 && fieldNumber === 1) {
      // samples (packed repeated float)
      const [length, lengthBytes] = readVarint(data, offset);
      offset += lengthBytes;

      const floatCount = length / 4;
      samples = new Float32Array(floatCount);
      const view = new DataView(data.buffer, data.byteOffset + offset, length);
      for (let i = 0; i < floatCount; i++) {
        samples[i] = view.getFloat32(i * 4, true);
      }
      offset += length;
    } else if (wireType === 0) {
      const [value, varBytes] = readVarint(data, offset);
      offset += varBytes;

      if (fieldNumber === 2) {
        sampleRate = value;
      } else if (fieldNumber === 3) {
        channels = value;
      }
    } else if (wireType === 5) {
      offset += 4;
    }
  }

  return { type: 'audio', samples, sampleRate, channels };
}

/**
 * Parse JSON from a decoded DataBuffer
 * @template T
 * @param {{type: string, jsonPayload?: string}} decoded
 * @returns {T|null}
 */
function parseJsonFromDataBuffer(decoded) {
  if (decoded.type === 'json' && decoded.jsonPayload) {
    try {
      return JSON.parse(decoded.jsonPayload);
    } catch {
      return null;
    }
  }
  return null;
}

// =============================================================================
// Exports
// =============================================================================

module.exports = {
  // Encoders
  encodeTextData,
  encodeJsonData,
  // Decoders
  decodeDataBuffer,
  decodeTextBuffer,
  decodeJsonData,
  decodeAudioBuffer,
  parseJsonFromDataBuffer,
};
