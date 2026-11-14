//! Kokoro's phoneme vocabulary, slicing a long token sequence to the acoustic
//! model's capacity, and trimming the silence the model puts at a slice's
//! ends. Ported from LiteRT-LM's `omni/text2speech/kokoro/{phonemizer,common}.cc`.

pub const BOS: i64 = 0;
pub const EOS: i64 = 0;
pub const SPACE: i64 = 16;
const MIN_PUNCT: i64 = 1;
const MAX_PUNCT: i64 = 6;
const SPLIT_CAPACITY_DIVISOR: usize = 2;

pub const SAMPLE_RATE: u32 = 24_000;
const JOIN_SILENCE_RATIO: f32 = 0.01;
const JOIN_MARGIN_SAMPLES: usize = (SAMPLE_RATE as usize) * 5 / 1000;
const PUNCTUATION_PAUSE_SAMPLES: usize = (SAMPLE_RATE as usize) * 400 / 1000;

/// The 178-token Kokoro-82M vocabulary (the entries Kokoro uses).
const VOCAB: &[(char, i64)] = &[
    (';', 1), (':', 2), (',', 3), ('.', 4), ('!', 5), ('?', 6), ('—', 9), ('…', 10),
    ('"', 11), ('(', 12), (')', 13), ('“', 14), ('”', 15), (' ', 16), ('\u{303}', 17),
    ('ʣ', 18), ('ʥ', 19), ('ʦ', 20), ('ʨ', 21), ('ᵝ', 22), ('ꭧ', 23), ('A', 24), ('I', 25),
    ('O', 31), ('Q', 33), ('S', 35), ('T', 36), ('W', 39), ('Y', 41), ('ᵊ', 42), ('a', 43),
    ('b', 44), ('c', 45), ('d', 46), ('e', 47), ('f', 48), ('h', 50), ('i', 51), ('j', 52),
    ('k', 53), ('l', 54), ('m', 55), ('n', 56), ('o', 57), ('p', 58), ('q', 59), ('r', 60),
    ('s', 61), ('t', 62), ('u', 63), ('v', 64), ('w', 65), ('x', 66), ('y', 67), ('z', 68),
    ('ɑ', 69), ('ɐ', 70), ('ɒ', 71), ('æ', 72), ('β', 75), ('ɔ', 76), ('ɕ', 77), ('ç', 78),
    ('ɖ', 80), ('ð', 81), ('ʤ', 82), ('ə', 83), ('ɚ', 85), ('ɛ', 86), ('ɜ', 87), ('ɟ', 90),
    ('ɡ', 92), ('ɥ', 99), ('ɨ', 101), ('ɪ', 102), ('ʝ', 103), ('ɯ', 110), ('ɰ', 111),
    ('ŋ', 112), ('ɳ', 113), ('ɲ', 114), ('ɴ', 115), ('ø', 116), ('ɸ', 118), ('θ', 119),
    ('œ', 120), ('ɹ', 123), ('ɾ', 125), ('ɻ', 126), ('ʁ', 128), ('ɽ', 129), ('ʂ', 130),
    ('ʃ', 131), ('ʈ', 132), ('ʧ', 133), ('ʊ', 135), ('ʋ', 136), ('ʌ', 138), ('ɣ', 139),
    ('ɤ', 140), ('χ', 142), ('ʎ', 143), ('ʒ', 147), ('ʔ', 148), ('ˈ', 156), ('ˌ', 157),
    ('ː', 158), ('ʰ', 162), ('ʲ', 164), ('↓', 169), ('→', 171), ('↗', 172), ('↘', 173),
    ('ᵻ', 177),
];

fn token(c: char) -> Option<i64> {
    VOCAB.iter().find(|(v, _)| *v == c).map(|(_, id)| *id)
}

/// BOS, the phonemes' ids, EOS. Empty when nothing is pronounceable.
/// Symbols outside the vocabulary are dropped (and counted).
pub fn phoneme_ids(ipa: &str) -> (Vec<i64>, usize) {
    let mut ids = vec![BOS];
    let mut voiced = false;
    let mut dropped = 0;
    for c in ipa.chars() {
        match token(c) {
            Some(id) => {
                voiced |= id != SPACE;
                ids.push(id);
            }
            None if c.is_whitespace() => ids.push(SPACE),
            None => dropped += 1,
        }
    }
    if !voiced {
        return (Vec::new(), dropped);
    }
    ids.push(EOS);
    (ids, dropped)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Join {
    /// A real chunk start or end: its silence belongs to the text.
    Chunk,
    /// A capacity cut on punctuation: a clause-length pause.
    Punctuation,
    /// A capacity cut between words: no pause.
    Word,
}

#[derive(Debug)]
pub struct Slice {
    pub ids: Vec<i64>,
    pub before: Join,
    pub after: Join,
}

fn is_punct(id: i64) -> bool {
    (MIN_PUNCT..=MAX_PUNCT).contains(&id)
}

fn is_phonetic(id: i64) -> bool {
    id > SPACE
}

fn last_break(ids: &[i64], earliest: usize, end: usize, pred: impl Fn(i64) -> bool) -> usize {
    (earliest + 1..=end).rev().find(|&p| pred(ids[p - 1])).unwrap_or(0)
}

/// `SliceTokenIds`: slices of at most `capacity` tokens (plus BOS/EOS), cut
/// at punctuation, then spaces, no earlier than half the capacity.
pub fn slice(ids: &[i64], capacity: usize) -> Vec<Slice> {
    if ids.len() <= capacity {
        return vec![Slice {
            ids: ids.to_vec(),
            before: Join::Chunk,
            after: Join::Chunk,
        }];
    }
    let mut content_end = ids.len();
    while content_end > 1 && (ids[content_end - 1] == EOS || ids[content_end - 1] == SPACE) {
        content_end -= 1;
    }
    let mut phonetic_end = content_end;
    while phonetic_end > 0 && !is_phonetic(ids[phonetic_end - 1]) {
        phonetic_end -= 1;
    }
    let mut slices = Vec::new();
    let mut start = 0;
    let mut before = Join::Chunk;
    while start < content_end {
        let mut end = (start + capacity).min(content_end);
        let mut after = Join::Chunk;
        if end < content_end {
            if phonetic_end > start + 1 {
                end = end.min(phonetic_end - 1);
            }
            let earliest = start + capacity / SPLIT_CAPACITY_DIVISOR;
            let mut split = last_break(ids, earliest, end, is_punct);
            if split != 0 {
                after = Join::Punctuation;
            } else {
                split = last_break(ids, earliest, end, |id| id == SPACE);
                after = Join::Word;
            }
            if split > earliest {
                end = split;
            } else {
                after = Join::Word;
            }
        }
        let mut out = vec![BOS];
        if start == 0 && ids[0] == BOS {
            start = 1;
        }
        out.extend_from_slice(&ids[start..end]);
        if *out.last().unwrap() != EOS || out.len() == 1 {
            out.push(EOS);
        }
        slices.push(Slice {
            ids: out,
            before,
            after,
        });
        before = after;
        start = end;
    }
    slices
}

/// Caps the silence at a slice's cut ends (`TrimSliceJoinSilence`).
pub fn trim_join_silence(pcm: &mut Vec<f32>, before: Join, after: Join) {
    let head = match before {
        Join::Chunk => None,
        _ => Some(JOIN_MARGIN_SAMPLES),
    };
    let tail = match after {
        Join::Chunk => None,
        Join::Word => Some(JOIN_MARGIN_SAMPLES),
        Join::Punctuation => Some(PUNCTUATION_PAUSE_SAMPLES),
    };
    if pcm.is_empty() || (head.is_none() && tail.is_none()) {
        return;
    }
    let peak = pcm.iter().fold(0f32, |m, s| m.max(s.abs()));
    if peak <= 0.0 {
        if let (Some(h), Some(t)) = (head, tail) {
            pcm.truncate(h + t);
        }
        return;
    }
    let threshold = peak * JOIN_SILENCE_RATIO;
    let mut begin = 0;
    let mut end = pcm.len();
    if let Some(h) = head {
        while begin < end && pcm[begin].abs() < threshold {
            begin += 1;
        }
        begin -= begin.min(h);
    }
    if let Some(t) = tail {
        while end > begin && pcm[end - 1].abs() < threshold {
            end -= 1;
        }
        end = (end + t).min(pcm.len());
    }
    pcm.truncate(end);
    pcm.drain(..begin);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_frame_with_bos_eos() {
        let (ids, dropped) = phoneme_ids("hˈɛlO.");
        assert_eq!(ids, vec![0, 50, 156, 86, 54, 31, 4, 0]);
        assert_eq!(dropped, 0);
        assert!(phoneme_ids("  ").0.is_empty());
    }

    #[test]
    fn slices_cut_at_punctuation() {
        let mut ids = vec![BOS];
        for _ in 0..3 {
            ids.extend([50, 51, 52, 3, SPACE]);
        }
        ids.push(EOS);
        let slices = slice(&ids, 8);
        assert!(slices.len() >= 2);
        assert_eq!(slices[0].after, Join::Punctuation);
        for s in &slices {
            assert_eq!(s.ids[0], BOS);
            assert_eq!(*s.ids.last().unwrap(), EOS);
            assert!(s.ids.len() <= 8 + 2);
        }
    }
}
