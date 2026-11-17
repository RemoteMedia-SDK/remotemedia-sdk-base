# litert-nodes has moved

The LiteRT plugin (`WhisperLiteRtNode`, `GemmaAssistantNode`, `KokoroTtsNode`)
now lives in its own repository,
[RemoteMedia-SDK/litert-nodes](https://github.com/RemoteMedia-SDK/litert-nodes),
with its history. It was split from this directory at commit `635b6ad`.

- The plugin is the `remotemedia-litert-nodes` package there, and builds the
  same `remotemedia_litert_nodes` library.
- The LiteRT and LiteRT-LM bindings, without the plugin export, are its
  `remotemedia-litert-runtime` crate.
- The assistant's room behavior is `remotemedia-room-agent` in
  [remotemedia-plugin-sdk](https://github.com/RemoteMedia-SDK/remotemedia-plugin-sdk).

See plugin-sdk's OpenSpec change `extract-room-agent-and-litert-plugin`.
