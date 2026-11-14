// C entry points over LiteRT-LM's English grapheme-to-phoneme front end
// (omni/text2speech/kokoro/english_g2p.cc), for the Rust Kokoro engine.
//
// The lexicon is the container's `en-lexicon` section, passed in as bytes the
// caller keeps alive for the converter's lifetime.

#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>

#include "absl/strings/string_view.h"
#include "omni/text2speech/kokoro/cjk_blob.h"
#include "omni/text2speech/kokoro/english_g2p.h"

namespace {

using ::litert::omni::tts::kokoro::CjkBlob;
using ::litert::omni::tts::kokoro::EnglishG2p;

struct Converter {
  std::unique_ptr<CjkBlob> blob;
  std::unique_ptr<EnglishG2p> g2p;
};

char* Copy(const std::string& text) {
  char* out = static_cast<char*>(std::malloc(text.size() + 1));
  if (out != nullptr) {
    std::memcpy(out, text.data(), text.size());
    out[text.size()] = '\0';
  }
  return out;
}

}  // namespace

extern "C" {

// Returns a converter, or NULL with `*error` set (free it with
// rm_en_g2p_free). An empty lexicon uses the built-in rules only.
void* rm_en_g2p_create(const char* lexicon, size_t lexicon_len, char** error) {
  auto converter = std::make_unique<Converter>();
  if (lexicon_len > 0) {
    auto blob = CjkBlob::Create(absl::string_view(lexicon, lexicon_len));
    if (!blob.ok()) {
      *error = Copy(std::string(blob.status().message()));
      return nullptr;
    }
    converter->blob = std::make_unique<CjkBlob>(*std::move(blob));
    auto g2p = EnglishG2p::Create(*converter->blob);
    if (!g2p.ok()) {
      *error = Copy(std::string(g2p.status().message()));
      return nullptr;
    }
    converter->g2p = std::make_unique<EnglishG2p>(*std::move(g2p));
  } else {
    auto g2p = EnglishG2p::Create();
    if (!g2p.ok()) {
      *error = Copy(std::string(g2p.status().message()));
      return nullptr;
    }
    converter->g2p = std::make_unique<EnglishG2p>(*std::move(g2p));
  }
  return converter.release();
}

// Kokoro phonemes for `text`. Returns the string (free with rm_en_g2p_free),
// or NULL with `*error` set.
char* rm_en_g2p_text_to_ipa(void* handle, const char* text, size_t text_len,
                            int british, char** error) {
  auto* converter = static_cast<Converter*>(handle);
  auto ipa = converter->g2p->TextToIpa(absl::string_view(text, text_len),
                                       british != 0);
  if (!ipa.ok()) {
    *error = Copy(std::string(ipa.status().message()));
    return nullptr;
  }
  return Copy(*ipa);
}

void rm_en_g2p_free(char* text) { std::free(text); }

void rm_en_g2p_destroy(void* handle) { delete static_cast<Converter*>(handle); }

}  // extern "C"
