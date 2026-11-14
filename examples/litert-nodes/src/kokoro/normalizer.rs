//! The rule-table text normalizer of LiteRT-LM's TTS front end
//! (`omni/text2speech/text_normalizer.{h,cc}`), driven by the container's
//! `<language>-textnorm` section: `digit`, `unit`, `map` and `rule` records,
//! rules applied in order to the whole text. Patterns are RE2 there and
//! `regex` here (same leftmost-first semantics; `\w`, `\d` and `\b` are
//! Unicode-aware here, ASCII there, which only matters for non-ASCII text).

use std::collections::HashMap;

use regex::Regex;

struct Rule {
    pattern: Regex,
    replacement: String,
}

pub struct TextNormalizer {
    rules: Vec<Rule>,
    digits: [String; 10],
    units: [String; 5],
    maps: HashMap<String, HashMap<String, String>>,
}

const MAX_CARDINAL_DIGITS: usize = 16;

impl TextNormalizer {
    pub fn parse(table: &str) -> Result<Self, String> {
        let mut this = Self {
            rules: Vec::new(),
            digits: Default::default(),
            units: Default::default(),
            maps: HashMap::new(),
        };
        for (n, line) in table.split('\n').enumerate() {
            let line = line.strip_suffix('\r').unwrap_or(line);
            let trimmed = line.trim_start();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let fields: Vec<&str> = line.split('\t').collect();
            let bad = |what: &str| format!("text normalizer line {}: {what}", n + 1);
            match fields[0] {
                "digit" if fields.len() == 3 => {
                    let d = fields[1]
                        .parse::<usize>()
                        .ok()
                        .filter(|d| *d < 10)
                        .ok_or_else(|| bad("bad digit"))?;
                    this.digits[d] = fields[2].to_string();
                }
                "unit" if fields.len() == 6 => {
                    for u in 0..5 {
                        this.units[u] = fields[u + 1].to_string();
                    }
                }
                "map" if fields.len() == 4 => {
                    this.maps
                        .entry(fields[1].to_string())
                        .or_default()
                        .insert(fields[2].to_string(), fields[3].to_string());
                }
                "rule" if fields.len() == 3 => {
                    let pattern = Regex::new(fields[1])
                        .map_err(|e| bad(&format!("pattern {:?}: {e}", fields[1])))?;
                    this.rules.push(Rule {
                        pattern,
                        replacement: fields[2].to_string(),
                    });
                }
                other => return Err(bad(&format!("unexpected record {other:?}"))),
            }
        }
        Ok(this)
    }

    pub fn normalize(&self, text: &str) -> String {
        let mut text = text.to_string();
        for rule in &self.rules {
            text = self.apply(rule, &text);
        }
        text
    }

    fn apply(&self, rule: &Rule, text: &str) -> String {
        let mut out = String::with_capacity(text.len());
        let mut last = 0;
        for caps in rule.pattern.captures_iter(text) {
            let whole = caps.get(0).unwrap();
            out.push_str(&text[last..whole.start()]);
            let groups: Vec<&str> = (0..caps.len())
                .map(|i| caps.get(i).map_or("", |m| m.as_str()))
                .collect();
            self.expand(&rule.replacement, &groups, &mut out);
            last = whole.end();
        }
        out.push_str(&text[last..]);
        out
    }

    fn expand(&self, replacement: &str, groups: &[&str], out: &mut String) {
        let r = replacement.as_bytes();
        let mut i = 0;
        let group = |b: u8| (b as char).to_digit(10).map(|d| d as usize);
        while i < r.len() {
            let c = r[i];
            let next = r.get(i + 1).copied();
            match (c, next) {
                (b'\\', Some(n)) => {
                    i += 2;
                    match n {
                        b't' => out.push('\t'),
                        b'n' => out.push('\n'),
                        _ => match group(n) {
                            Some(g) => out.push_str(groups.get(g).copied().unwrap_or("")),
                            None => out.push(n as char),
                        },
                    }
                    continue;
                }
                (b'#', Some(n)) if group(n).is_some() => {
                    i += 2;
                    for ch in groups.get(group(n).unwrap()).copied().unwrap_or("").chars() {
                        match ch.to_digit(10) {
                            Some(d) => out.push_str(&self.digits[d as usize]),
                            None => out.push(ch),
                        }
                    }
                    continue;
                }
                (b'@', Some(n)) if group(n).is_some() => {
                    i += 2;
                    self.cardinal(groups.get(group(n).unwrap()).copied().unwrap_or(""), out);
                    continue;
                }
                (b'{', _) => {
                    if let Some((name, g, len)) = map_lookup(&replacement[i..]) {
                        i += len;
                        let key = groups.get(g).copied().unwrap_or("");
                        match self.maps.get(name).and_then(|m| m.get(key)) {
                            Some(v) => out.push_str(v),
                            None => out.push_str(key),
                        }
                        continue;
                    }
                }
                _ => {}
            }
            // Copy one UTF-8 character.
            let len = utf8_len(c);
            out.push_str(&replacement[i..(i + len).min(r.len())]);
            i += len;
        }
    }

    /// `@N`: a run of digits as a cardinal number with the digit and unit
    /// tables (the CJK-style grouping LiteRT-LM uses).
    fn cardinal(&self, digits: &str, out: &mut String) {
        if digits.is_empty() {
            return;
        }
        if !digits.bytes().all(|b| b.is_ascii_digit()) {
            out.push_str(digits);
            return;
        }
        let stripped = digits.trim_start_matches('0');
        if stripped.is_empty() {
            out.push_str(&self.digits[0]);
            return;
        }
        if stripped.len() > MAX_CARDINAL_DIGITS {
            for b in digits.bytes() {
                out.push_str(&self.digits[(b - b'0') as usize]);
            }
            return;
        }
        let s = stripped.as_bytes();
        let len = s.len();
        let groups = (len + 3) / 4;
        let mut emitted = false;
        let mut zero_pending = false;
        for g_rev in 0..groups {
            let g = groups - 1 - g_rev;
            let end = len - 4 * g;
            let begin = end.saturating_sub(4);
            let part = &s[begin..end];
            if part.iter().all(|&b| b == b'0') {
                zero_pending = true;
                continue;
            }
            for (idx, &b) in part.iter().enumerate() {
                let pos = part.len() - 1 - idx;
                let d = (b - b'0') as usize;
                if d == 0 {
                    zero_pending = true;
                    continue;
                }
                if zero_pending && emitted {
                    out.push_str(&self.digits[0]);
                }
                zero_pending = false;
                if !(!emitted && pos == 1 && d == 1) {
                    out.push_str(&self.digits[d]);
                }
                if pos > 0 {
                    out.push_str(&self.units[pos - 1]);
                }
                emitted = true;
            }
            match g {
                1 => out.push_str(&self.units[3]),
                2 => out.push_str(&self.units[4]),
                3 => {
                    out.push_str(&self.units[3]);
                    out.push_str(&self.units[4]);
                }
                _ => {}
            }
        }
    }
}

/// `{name:\N}` at the start of `s`: (name, group, length in bytes).
fn map_lookup(s: &str) -> Option<(&str, usize, usize)> {
    let close = s.find('}')?;
    let body = &s[1..close];
    let colon = body.find(':')?;
    let b = body.as_bytes();
    if colon == 0 || b.len() != colon + 3 || b[colon + 1] != b'\\' || !b[colon + 2].is_ascii_digit() {
        return None;
    }
    Some((&body[..colon], (b[colon + 2] - b'0') as usize, close + 1))
}

fn utf8_len(lead: u8) -> usize {
    match lead {
        b if b < 0x80 => 1,
        b if b & 0xE0 == 0xC0 => 2,
        b if b & 0xF0 == 0xE0 => 3,
        b if b & 0xF8 == 0xF0 => 4,
        _ => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn english_style_rules() {
        let table = "digit\t0\t 0\ndigit\t1\t 1\ndigit\t2\t 2\ndigit\t3\t 3\ndigit\t4\t 4\n\
                     digit\t5\t 5\ndigit\t6\t 6\ndigit\t7\t 7\ndigit\t8\t 8\ndigit\t9\t 9\n\
                     map\tdecade\t90\tnineties\n\
                     rule\t\\$(\\d+)\\.(\\d\\d)\\b\t\\1 dollars and \\2 cents\n\
                     rule\t(\\d)\\.(\\d+)\t\\1 point#2\n\
                     rule\t(^|[^\\w.,:/$£€-])([1-9]\\d)([1-9]0)s\\b\t\\1\\2 {decade:\\3}\n";
        let n = TextNormalizer::parse(table).unwrap();
        assert_eq!(n.normalize("$6.50"), "6 dollars and 50 cents");
        assert_eq!(n.normalize("pi is 3.14"), "pi is 3 point 1 4");
        assert_eq!(n.normalize("the 1990s"), "the 19 nineties");
    }
}
