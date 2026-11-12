# litert-nodes

Speech-to-text, a voice assistant and text-to-speech on Google's LiteRT, as
one loadable plugin that runs the same models on an Android phone's GPU and on
a desktop CPU:

| Node | Model | In | Out |
|------|-------|----|-----|
| `WhisperLiteRtNode` | Whisper (TFLite) | audio | the transcript (Text) |
| `GemmaAssistantNode` | a LiteRT-LM model, e.g. Gemma 4 E2B | transcripts and typed messages | the assistant's reply, a sentence at a time (JSON) |
| `KokoroTtsNode` | Kokoro-82M (`.litertlm` bundle) | Text, or JSON with `say` | 24 kHz mono speech |

The runtimes are loaded when a node starts, not linked, so the plugin builds
anywhere; see [Runtimes](#runtimes).

## WhisperLiteRtNode

Takes audio (any rate, mono or interleaved) and emits the transcript as text;
other inputs are ignored. The model is a Whisper export with two signatures:
`encode` (log-mel `[1, 80, 3000]` → `[1, 1500, d]`) and `decode` (encoder
output, `[1, T]` token ids, `[1, 1, T, T]` additive causal mask →
`[1, T, vocab]` logits), e.g. `whisper_tiny_30s_f32.tflite` with the model's
Hugging Face `tokenizer.json` beside it. Decoding is greedy, English
(`language`), no timestamps.

| Param | Default | |
|-------|---------|---|
| `model_path` | required | the `.tflite`; `${VAR}` is expanded, so each runner can name its own copy |
| `tokenizer_path` | `tokenizer.json` beside the model | |
| `accelerator` | `gpu` | `gpu` / `npu` / `npu,gpu`: LiteRT's CompiledModel API, CPU as fallback; `cpu`: the TFLite interpreter on XNNPACK |
| `accelerator_dir` | | where NPU dispatch and compiler plugin libraries are |
| `runtime_lib` | platform default | LiteRT library; also `REMOTEMEDIA_LITERT_LIB` |
| `threads` | 4 | XNNPACK threads (`cpu`) |

## GemmaAssistantNode

A voice assistant for a multi-person room, on LiteRT-LM's C API: one engine
and one conversation (the model's KV cache keeps the history; it starts over
when it nears `max_tokens`).

It reads everything said in the room: transcripts (Text, attributed to their
speaker by the runtime) and typed messages (`{"kind": "text", "text"}`), and
answers when a line names it (`wake_words`, or every line with
`respond: "all"`), with the lines since its last answer as context. A line
that is little more than the name ("Hey Pixel") gets no answer of its own:
turn detection often ends a turn there, so the same speaker's next line within
`follow_up_secs` counts as addressed.

The reply streams as it is generated, one complete sentence at a time (the
first may end at a comma, so speech can start sooner). Before it, activity
events let a page show what the assistant is doing: `listening` when a line
was only its name (it waits `for_ms` for the question), `thinking` when a
reply starts generating.

```json
{"kind": "assistant", "state": "listening", "to": "alice", "for_ms": 8000}
{"kind": "assistant", "state": "thinking", "reply": "9183-7", "to": "alice"}
{"kind": "assistant", "say": "The Pacific is the largest ocean.", "reply": "9183-7", "to": "alice"}
{"kind": "assistant", "text": "<the whole reply>", "reply": "9183-7", "to": "alice", "done": true, "first_ms": 669, "total_ms": 3340}
```

`reply` ids are unique per node instance, so replies from before and after a
move are told apart.

| Param | Default | |
|-------|---------|---|
| `model_path` | required | `.litertlm` model (`${VAR}` expands) |
| `runtime_lib` | platform default | LiteRT-LM C API library; also `REMOTEMEDIA_LITERT_LM_LIB` |
| `backend` | `gpu` | `gpu` or `cpu` |
| `cache_dir` | | where compiled GPU kernels are kept between runs |
| `threads` | library default | CPU threads |
| `max_tokens` | 4096 | context length |
| `max_reply_tokens` | 160 | |
| `name` | `Pixel` | what it is called (in the system prompt) |
| `wake_words` | `[name]` | words that address it, lower case |
| `respond` | `mention` | `mention` or `all` |
| `follow_up_secs` | 8 | how long a bare "Hey Pixel" waits for its question |
| `system_prompt` | built in | asks for one to three short spoken sentences |

## KokoroTtsNode

Kokoro-82M from its LiteRT-LM bundle
([`litert-community/Kokoro-82M`](https://huggingface.co/litert-community/Kokoro-82M)),
English. Speaks Text, and JSON with a `say` field; other input is ignored.
Each slice of speech is emitted as it is synthesized, in `chunk_ms` frames.

The bundle is meant for LiteRT-LM's `omni` TTS engine, which is not released
for Android; this node runs the same pipeline itself:

1. text normalization with the bundle's `en-textnorm` rule table (a port of
   `omni/text2speech/text_normalizer`, with Rust's `regex` for RE2);
2. LiteRT-LM's English grapheme-to-phoneme front end over the bundle's
   `en-lexicon`, compiled from its C++ sources (`cpp/`, with Abseil);
3. the acoustic model (durations, pitch, energy) on the TFLite interpreter;
   its `litert_custom_op.bidirectional_lstm` op, which stock LiteRT runtimes
   do not ship, is implemented here (`src/bilstm.rs`) and registered with
   both LiteRT APIs;
4. the vocoder (magnitude and phase spectrograms) on the GPU, then a
   20-point inverse STFT.

The vocoder as shipped does not compile on LiteRT 2.3.2's GPU delegate: one
FULLY_CONNECTED turns `[1, 230400, 9]` into `[230400, 1]`, and ML Drift takes
the 230400 as the batch. The node rewrites that op to keep its dimensions
when it loads the model (`src/kokoro/patch.rs`); the result is the same
tensor, and the whole graph then runs on the GPU.

| Param | Default | |
|-------|---------|---|
| `model_path` | required | `kokoro_int8_gpu_fp16.litertlm` (GPU) or `kokoro_int8_fp16.litertlm` (CPU) |
| `runtime_lib` | platform default | LiteRT library; also `REMOTEMEDIA_LITERT_LIB` |
| `accelerator` | `gpu` | `gpu` (CompiledModel) or `cpu` (TFLite interpreter; for libraries without CompiledModel) |
| `voice` | `af_heart` | any of the bundle's 54 voices |
| `language` | `en-us` | `en-us` or `en-gb` |
| `threads` | 4 | XNNPACK threads |
| `gpu_options` | | LiteRT GPU options as TOML, e.g. `precision = 2` |
| `chunk_ms` | 500 | audio per output frame |

## Runtimes

- **Android**: `libLiteRt.so` and `libLiteRtClGlAccelerator.so` from Google's
  Maven (`com.google.ai.edge.litert:litert` and `:litert-gpu`, 2.3.2,
  `jni/arm64-v8a`) on the loader path, for Whisper and Kokoro. For Gemma,
  LiteRT-LM's `liblitert-lm.so` from `litert_lm_c_api-1.0.0.zip` on the
  [LiteRT-LM releases](https://github.com/google-ai-edge/LiteRT-LM/releases)
  page (v0.19.1); it has no SONAME, so it is opened by path (`runtime_lib`),
  never linked. `libc++_shared.so` (NDK) on the loader path too. The NPU
  path (`litert-npu-runtime-google-tensor`) supports Tensor G3 and later.
- **Windows, CPU**: LiteRT-LM's `litert-lm.dll` from the same zip, for all
  three nodes (`accelerator: "cpu"`, `backend: "cpu"`).

## Build

The `kokoro` feature (on by default) compiles the C++ phonemizer, so a build
needs CMake, Ninja and a C++ compiler (MSVC on Windows; the NDK's clang for
Android, with `ANDROID_NDK_HOME` set). The first build downloads Abseil
20260526.0 into `cpp/third_party/` (or set `ABSEIL_SOURCE_DIR`).

```bash
cargo build --release                                     # this host
cargo build --release --target aarch64-linux-android      # Android (NDK clang as linker)
cargo run --release --bin whisper-litert -- model.tflite audio.wav --accel cpu --lib litert-lm.dll
cargo run --release --bin gemma-litert -- gemma-4-E2B-it.litertlm "What is a pixel?" --backend cpu --lib litert-lm.dll
cargo run --release --bin kokoro-litert -- kokoro_int8_fp16.litertlm "Hello there." --accel cpu --lib litert-lm.dll --out hello.wav
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
"plugins": [{ "path": "./plugins/litert-nodes", "node_types": ["WhisperLiteRtNode", "GemmaAssistantNode", "KokoroTtsNode"] }]
```

`node_types` lets a distributed runtime send the plugin only to the runners
that run its nodes.

## Measured

Pixel 6 Pro (Tensor G1, Mali-G78) and Ryzen 9 7950X, release builds.

Whisper tiny (f32), `transcribe_demo.wav` (4.3 s), per turn:

| Where | Encode | Decode per token | Turn |
|-------|--------|------------------|------|
| Pixel 6 Pro, GPU (LiteRT 2.3.2) | ~90 ms | ~42 ms | ~0.65 s |
| Pixel 6 Pro, CPU (XNNPACK, 4 threads) | ~450 ms | ~140 ms | ~2.1 s |
| Ryzen 9 7950X, CPU (XNNPACK, 4 threads) | ~80 ms | ~28 ms | ~0.44 s |

On the GPU every op of both signatures is delegated. The decoder has no KV
cache (each step recomputes the 128-token window), which is most of a turn's
time.

Gemma 4 E2B (`gemma-4-E2B-it.litertlm`), a two-question conversation
(`gemma-litert`):

| Where | Load | First token | Decode |
|-------|------|-------------|--------|
| Pixel 6 Pro, GPU | 31 s | 0.67 s, then 0.38 s | ~9.5 tokens/s |
| Ryzen 9 7950X, CPU (8 threads) | 0.2 s | 0.36 s, then 0.24 s | ~19 tokens/s |

Kokoro-82M, 8.2 s of speech (131 phonemes, two slices), voice `af_heart`:

| Where | Acoustic | Vocoder | First audio | Speed |
|-------|----------|---------|-------------|-------|
| Pixel 6 Pro: acoustic CPU, vocoder GPU | ~0.4 s / slice | ~0.9 s / slice | 1.4 s | 3.2× real time |
| Pixel 6 Pro: all CPU | ~0.35 s / slice | ~7.5 s / slice | 7.8 s | 0.5× |
| Ryzen 9 7950X, CPU | ~0.18 s / slice | ~1.5 s / slice | 1.7 s | 2.2× |

The vocoder renders its whole frame budget (384 frames, 9.6 s) whatever the
slice's length (its graph fixes the length in its reshapes), so a short
sentence costs as much as a long one. Whisper tiny transcribes the output of
both back word for word.
