# English phonemizer for Kokoro

`omni/text2speech/kokoro/` holds LiteRT-LM's English grapheme-to-phoneme
front end, unchanged, from
[google-ai-edge/LiteRT-LM](https://github.com/google-ai-edge/LiteRT-LM) at
commit `0c54891e5bc3fe02e7d2b37592dd9b6245d7e4bd` (Apache-2.0, see
`omni/LICENSE`):

- `english_g2p.{h,cc}`: misaki-style English G2P over the bundled lexicon
- `cjk_blob.{h,cc}`: the `LTTSCJK1` container the lexicon is stored in
- `unicode_utils.{h,cc}`

`g2p_capi.cc` is ours: four C functions the Rust Kokoro engine calls.

`build.rs` compiles these with Abseil (`20260526.0`, the version LiteRT-LM
pins). It builds Abseil with CMake from `ABSEIL_SOURCE_DIR`, or from
`cpp/third_party/abseil-cpp-20260526.0`, downloading the release tarball
there on the first build.
