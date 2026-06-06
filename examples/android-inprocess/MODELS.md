# Models Documentation

This document describes all ML models bundled with the RemoteMedia Android In-Process Voice Assistant app.

## Model Summary

| Model | Category | Format | Size | Quantization | License |
|-------|----------|--------|------|--------------|---------|
| Whisper tiny | STT | Candle | 39 MB | int8 | MIT |
| Whisper base | STT | Candle | 74 MB | int8 | MIT |
| Phi-3-mini-4k-instruct | LLM | GGUF | 2.2 GB | Q4 (4-bit) | MIT |
| Kokoro v0.19 | TTS | ONNX | 82 MB | int8 | Apache-2.0 |
| Silero VAD | VAD | ONNX | 5 MB | int8 | MIT |

Total: **~2.4 GB** (with all models)

## Detailed Model Information

### Whisper (Speech-to-Text)

**Source**: [OpenAI Whisper](https://github.com/openai/whisper) / [faster-whisper](https://github.com/guillaumekln/faster-whisper)

**Models**:
- `whisper_tiny` (39 MB): Fastest, good for short commands
- `whisper_base` (74 MB): Better accuracy, recommended for conversation

**Format**: Candle (Rust-native, no Python dependency at inference)
- Model weights: `model.bin` (converted from PyTorch)
- Config: `config.json`
- Tokenizer: `tokenizer.json`

**Configuration**:
```yaml
model_size: "tiny"  # or "base"
language: "auto"    # auto-detect
task: "transcribe"
temperature: 0.0
beam_size: 1        # 5 for base
```

**Precision**: int8 quantized for mobile

**License**: MIT

### Phi-3-mini (Large Language Model)

**Source**: [Microsoft Phi-3](https://huggingface.co/microsoft/Phi-3-mini-4k-instruct)

**Model**: `phi3_mini` (2.2 GB Q4)

**Format**: GGUF (llama.cpp compatible)
- Single file: `Phi-3-mini-4k-instruct-q4.gguf`
- 4-bit quantization (Q4_K_M)

**Context Window**: 4096 tokens

**Configuration**:
```yaml
max_tokens: 150
temperature: 0.7
top_p: 0.9
top_k: 50
system_prompt: "You are a concise voice assistant..."
stream: true
```

**Precision**: 4-bit quantized (Q4_K_M)

**License**: MIT

**Note**: This is the largest model (~2.2 GB). For lower-end devices, consider:
- Phi-3-mini Q3 (1.7 GB)
- Smaller models like TinyLlama (600 MB)

### Kokoro (Text-to-Speech)

**Source**: [Kokoro ONNX](https://huggingface.co/onnx-community/Kokoro-82M-v1.0-ONNX)

**Model**: `kokoro` (ONNX model + tokenizer + per-voice embedding)

**Format**: ONNX
- Main model: `model_q8f16.onnx`
- Tokenizer: `tokenizer.json`
- Voices: `voices/{voice_name}.bin` (256-float style rows)

**Available Voices**:
| Voice | Language | Gender | Description |
|-------|----------|--------|-------------|
| af_bella | English (US) | Female | Default, natural |
| af_sarah | English (US) | Female | Softer |
| am_michael | English (US) | Male | Deep |
| am_adam | English (US) | Male | Clear |

**Configuration**:
```yaml
backend: "onnx"
model_dir: "/data/data/com.remotemedia.inprocess/files/models/kokoro"
model: "model_q8f16.onnx"
tokenizer_path: "/data/data/com.remotemedia.inprocess/files/models/kokoro/tokenizer.json"
voice: "af_bella"
speed: 1.0
input_mode: "phonemes"
```

**Precision**: int8 quantized ONNX

**License**: Apache-2.0

### Misaki G2P (Text Frontend)

**Source**: [MisakiSwift reference](https://github.com/mlalma/MisakiSwift)

**Purpose**: Converts assistant text into Kokoro-compatible phoneme text before `KokoroTTSNode`.

**Required app-private layout**:

```text
/data/data/com.remotemedia.inprocess/files/models/misaki-g2p/
├── en-US/
│   ├── gold.json
│   └── silver.json
└── en-GB/
    ├── gold.json
    └── silver.json
```

The deploy script accepts production dictionaries at `${WORKSPACE_ROOT}/models/misaki-g2p`.
If absent, it stages the tiny fixture dictionaries from `${WORKSPACE_ROOT}/misaki-g2p/resources`
for development smoke tests.

### Silero VAD (Voice Activity Detection)

**Source**: [Silero VAD](https://github.com/snakers4/silero-vad)

**Model**: `silero_vad` (5 MB)

**Format**: ONNX
- Single file: `silero_vad.onnx`

**Configuration**:
```yaml
threshold: 0.5
min_speech_duration_ms: 250
min_silence_duration_ms: 1000
sample_rate: 16000
```

**Precision**: int8 quantized ONNX

**License**: MIT

## Model Paths

Models are stored in the APK assets:
```
assets/
└── models/
    ├── whisper/
    │   ├── tiny/
    │   │   ├── model.bin
    │   │   ├── config.json
    │   │   └── tokenizer.json
    │   └── base/
    │       ├── model.bin
    │       ├── config.json
    │       └── tokenizer.json
    ├── llm/
    │   └── phi3_mini/
    │       └── Phi-3-mini-4k-instruct-q4.gguf
    ├── kokoro/
    │   ├── tokenizer.json
    │   ├── onnx/
    │   │   └── model_q8f16.onnx
    │   └── voices/
    │       └── af_bella.bin
    ├── misaki-g2p/
    │   ├── en-US/
    │   │   ├── gold.json
    │   │   └── silver.json
    │   └── en-GB/
    │       ├── gold.json
    │       └── silver.json
    └── vad/
        └── silero_vad/
            └── silero_vad.onnx
```

## Manifest Format

The `models/manifest.json` contains metadata for each model:

```json
{
  "version": "1.0",
  "models": {
    "whisper_tiny": {
      "name": "whisper_tiny",
      "category": "whisper",
      "path": "models/whisper/tiny",
      "primary_file": "model.bin",
      "size_bytes": 40894464,
      "size_mb": 39.0,
      "sha256": "...",
      "format": "candle",
      "quantized": true
    }
    ...
  },
  "total_size_mb": 2400.5
}
```

## Model Extraction

On first app launch:
1. Models extracted from APK assets to `cacheDir/models/`
2. SHA256 verified against manifest
3. Extraction progress shown to user (~30 seconds)
4. Subsequent launches skip extraction

## Adding New Models

1. Add model info to `scripts/bundle_models.py` MODELS dict
2. Implement corresponding Python node (STT/LLM/TTS/VAD)
3. Add to pipeline manifests as needed
4. Run bundling script
5. Update this documentation

## Alternative Models

### Smaller LLM Options
| Model | Size (Q4) | Context | License |
|-------|-----------|---------|---------|
| TinyLlama-1.1B | 600 MB | 2048 | Apache-2.0 |
| Gemma-2B | 1.5 GB | 8192 | Gemma |
| Phi-2 | 1.4 GB | 2048 | MIT |

### Alternative STT
- **Whisper.cpp** (GGUF): Smaller, runs via llama.cpp
- **Vosk** (ONNX): ~50 MB, offline, multiple languages

### Alternative TTS
- **Piper** (ONNX): Multi-speaker, smaller
- **Coqui TTS** (ONNX): Multiple voices

## Verification

Verify model integrity:
```bash
# Generate manifest
python3 scripts/generate_model_manifest.py --verify

# Or manually check hashes
sha256sum assets/models/whisper/tiny/model.bin
```

## License Compliance

All models use permissive licenses (MIT, Apache-2.0). Ensure compliance:
- Include license texts in app (NOTICE file)
- No copyleft dependencies
- Attribution in about screen
