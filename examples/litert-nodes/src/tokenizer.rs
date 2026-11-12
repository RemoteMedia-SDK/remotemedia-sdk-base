//! Decoding Whisper token ids to text with a Hugging Face `tokenizer.json`
//! (GPT-2 byte-level BPE): each token's characters map back to bytes.

use std::collections::HashMap;

pub struct Tokenizer {
    tokens: Vec<Option<String>>,
    special: Vec<bool>,
    by_content: HashMap<String, u32>,
    byte_of: HashMap<char, u8>,
}

/// GPT-2's `bytes_to_unicode`: printable bytes map to themselves, the rest to
/// U+0100 onwards.
fn byte_decoder() -> HashMap<char, u8> {
    let mut printable: Vec<u32> = (b'!' as u32..=b'~' as u32).collect();
    printable.extend(0xA1..=0xAC);
    printable.extend(0xAE..=0xFF);
    let mut map = HashMap::new();
    let mut extra = 0;
    for byte in 0u32..256 {
        let ch = if printable.contains(&byte) {
            byte
        } else {
            extra += 1;
            255 + extra
        };
        map.insert(char::from_u32(ch).unwrap(), byte as u8);
    }
    map
}

impl Tokenizer {
    pub fn from_file(path: &str) -> Result<Self, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
        let json: serde_json::Value =
            serde_json::from_str(&text).map_err(|e| format!("{path}: {e}"))?;
        let vocab = json["model"]["vocab"]
            .as_object()
            .ok_or_else(|| format!("{path}: no model.vocab"))?;
        let mut entries: Vec<(u32, String, bool)> = vocab
            .iter()
            .filter_map(|(token, id)| Some((id.as_u64()? as u32, token.clone(), false)))
            .collect();
        for added in json["added_tokens"].as_array().into_iter().flatten() {
            if let (Some(id), Some(content)) = (added["id"].as_u64(), added["content"].as_str()) {
                entries.push((id as u32, content.to_string(), true));
            }
        }
        let size = entries
            .iter()
            .map(|(id, ..)| *id as usize + 1)
            .max()
            .unwrap_or(0);
        let mut tokens = vec![None; size];
        let mut special = vec![false; size];
        let mut by_content = HashMap::new();
        for (id, content, is_special) in entries {
            if is_special {
                by_content.insert(content.clone(), id);
            }
            special[id as usize] |= is_special;
            tokens[id as usize] = Some(content);
        }
        Ok(Self {
            tokens,
            special,
            by_content,
            byte_of: byte_decoder(),
        })
    }

    /// The id of a special token such as `<|en|>`.
    pub fn special_id(&self, content: &str) -> Option<u32> {
        self.by_content.get(content).copied()
    }

    pub fn is_special(&self, id: u32) -> bool {
        self.special.get(id as usize).copied().unwrap_or(true)
    }

    /// Text of the ordinary tokens in `ids` (special tokens are skipped).
    pub fn decode(&self, ids: &[u32]) -> String {
        let mut bytes = Vec::new();
        for &id in ids {
            if self.is_special(id) {
                continue;
            }
            if let Some(Some(token)) = self.tokens.get(id as usize) {
                bytes.extend(token.chars().filter_map(|c| self.byte_of.get(&c)));
            }
        }
        String::from_utf8_lossy(&bytes).into_owned()
    }
}
