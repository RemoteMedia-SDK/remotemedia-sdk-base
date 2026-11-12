# litert-nodes

Whisper speech-to-text on LiteRT (TFLite), as a loadable plugin that runs the
same model on an Android phone's GPU and on a desktop.

`WhisperLiteRtNode` takes audio (any rate, mono or interleaved) and emits the
transcript as text; other inputs are ignored. The model is a Whisper export
with two signatures: `encode` (log-mel `[1, 80, 3000]` → `[1, 1500, d]`) and
`decode` (encoder output, `[1, T]` token ids, `[1, 1, T, T]` additive causal
mask → `[1, T, vocab]` logits), e.g. `whisper_tiny_30s_f32.tflite` with the
model's Hugging Face `tokenizer.json` beside it. Decoding is greedy, English
(`language`), no timestamps.

| Param | Default | |
|-------|---------|---|
| `model_path` | required | the `.tflite`; `${VAR}` is expanded, so each runner can name its own copy |
| `tokenizer_path` | `tokenizer.json` beside the model | |
| `accelerator` | `gpu` | `gpu` / `npu` / `npu,gpu`: LiteRT's CompiledModel API, CPU as fallback; `cpu`: the TFLite interpreter on XNNPACK |
| `accelerator_dir` | | where NPU dispatch and compiler plugin libraries are |
| `runtime_lib` | platform default | LiteRT library; also `REMOTEMEDIA_LITERT_LIB` |
| `threads` | 4 | XNNPACK threads (`cpu`) |

LiteRT is loaded when the node starts, not linked, so the plugin builds
anywhere:

- **Android, GPU**: `libLiteRt.so` and `libLiteRtClGlAccelerator.so` from
  Google's Maven (`com.google.ai.edge.litert:litert` and `:litert-gpu`,
  2.3.2, `jni/arm64-v8a`) on the loader path. The NPU path
  (`litert-npu-runtime-google-tensor`) supports Tensor G3 and later.
- **Windows, CPU**: LiteRT-LM's `litert-lm.dll` (the `litert_lm_c_api` zip of a
  LiteRT-LM release) with `accelerator: "cpu"`.

## Build

```bash
cargo build --release                                     # this host
cargo build --release --target aarch64-linux-android      # Android (NDK clang as linker)
cargo run --release --bin whisper-litert -- model.tflite audio.wav --accel cpu --lib litert-lm.dll
```

As a plugin directory, one build per platform named by `plugin.toml`'s
`asset_pattern`:

```
litert-nodes/
  plugin.toml
  liblitert-nodes-aarch64-android.so     # target/aarch64-linux-android/release/libremotemedia_litert_nodes.so
  liblitert-nodes-x86_64-windows.dll     # target/release/remotemedia_litert_nodes.dll
```

```json
"plugins": [{ "path": "./plugins/litert-nodes", "node_types": ["WhisperLiteRtNode"] }]
```

`node_types` lets a distributed runtime send the plugin only to the runners
that run the node.

## Measured

Whisper tiny (f32), `transcribe_demo.wav` (4.3 s), per turn:

| Where | Encode | Decode per token | Turn |
|-------|--------|------------------|------|
| Pixel 6 Pro, GPU (Mali-G78, LiteRT 2.3.2) | ~90 ms | ~42 ms | ~0.65 s |
| Pixel 6 Pro, CPU (XNNPACK, 4 threads) | ~450 ms | ~140 ms | ~2.1 s |
| Ryzen 9 7950X, CPU (XNNPACK, 4 threads) | ~80 ms | ~28 ms | ~0.44 s |

On the GPU every op of both signatures is delegated. The decoder has no KV
cache (each step recomputes the 128-token window), which is most of a turn's
time.
