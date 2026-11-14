//! Reading a `.litertlm` container: an 8-byte `LITERTLM` magic, a version, the
//! end of the header, then a FlatBuffer (`schema/core/litertlm_header_schema.fbs`
//! in LiteRT-LM) listing sections by byte range, data type and key/value items.
//! The file is memory-mapped; sections are borrowed slices of it.

use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use memmap2::Mmap;

/// `AnySectionDataType`.
pub const TFLITE_MODEL: u8 = 3;
pub const GENERIC_BINARY: u8 = 1;

#[derive(Clone, Debug)]
pub struct Section {
    pub data_type: u8,
    pub begin: usize,
    pub end: usize,
    /// String-valued items (`name`, `model_type`, `backend_constraint`, ...);
    /// other value types are kept as their `Debug` text.
    pub items: HashMap<String, String>,
}

pub struct Container {
    map: Arc<Mmap>,
    pub sections: Vec<Section>,
    pub metadata: HashMap<String, String>,
}

/// A minimal FlatBuffer table reader: enough for this schema.
pub(crate) struct Table<'a> {
    pub(crate) buf: &'a [u8],
    pub(crate) pos: usize,
    vtable: usize,
}

fn u16_at(b: &[u8], p: usize) -> Result<u16, String> {
    b.get(p..p + 2)
        .map(|s| u16::from_le_bytes([s[0], s[1]]))
        .ok_or_else(|| "flatbuffer out of bounds".into())
}

pub(crate) fn u32_at(b: &[u8], p: usize) -> Result<u32, String> {
    b.get(p..p + 4)
        .map(|s| u32::from_le_bytes(s.try_into().unwrap()))
        .ok_or_else(|| "flatbuffer out of bounds".into())
}

fn u64_at(b: &[u8], p: usize) -> Result<u64, String> {
    b.get(p..p + 8)
        .map(|s| u64::from_le_bytes(s.try_into().unwrap()))
        .ok_or_else(|| "flatbuffer out of bounds".into())
}

impl<'a> Table<'a> {
    pub(crate) fn at(buf: &'a [u8], pos: usize) -> Result<Self, String> {
        let back = u32_at(buf, pos)? as i32;
        let vtable = (pos as i64 - back as i64) as usize;
        Ok(Self { buf, pos, vtable })
    }

    pub(crate) fn field(&self, index: usize) -> Result<Option<usize>, String> {
        let len = u16_at(self.buf, self.vtable)? as usize;
        let slot = 4 + 2 * index;
        if slot >= len {
            return Ok(None);
        }
        let off = u16_at(self.buf, self.vtable + slot)? as usize;
        Ok((off != 0).then_some(self.pos + off))
    }

    pub(crate) fn u8(&self, index: usize) -> Result<u8, String> {
        Ok(match self.field(index)? {
            Some(p) => *self.buf.get(p).ok_or("flatbuffer out of bounds")?,
            None => 0,
        })
    }

    pub(crate) fn u64(&self, index: usize) -> Result<u64, String> {
        Ok(match self.field(index)? {
            Some(p) => u64_at(self.buf, p)?,
            None => 0,
        })
    }

    pub(crate) fn reference(&self, index: usize) -> Result<Option<usize>, String> {
        Ok(match self.field(index)? {
            Some(p) => Some(p + u32_at(self.buf, p)? as usize),
            None => None,
        })
    }

    pub(crate) fn table(&self, index: usize) -> Result<Option<Table<'a>>, String> {
        self.reference(index)?
            .map(|p| Table::at(self.buf, p))
            .transpose()
    }

    pub(crate) fn string(&self, index: usize) -> Result<Option<String>, String> {
        let Some(p) = self.reference(index)? else {
            return Ok(None);
        };
        let n = u32_at(self.buf, p)? as usize;
        let bytes = self
            .buf
            .get(p + 4..p + 4 + n)
            .ok_or("flatbuffer string out of bounds")?;
        Ok(Some(String::from_utf8_lossy(bytes).into_owned()))
    }

    pub(crate) fn u32(&self, index: usize) -> Result<u32, String> {
        Ok(match self.field(index)? {
            Some(p) => u32_at(self.buf, p)?,
            None => 0,
        })
    }

    pub(crate) fn i32s(&self, index: usize) -> Result<Vec<i32>, String> {
        let Some(p) = self.reference(index)? else {
            return Ok(Vec::new());
        };
        let n = u32_at(self.buf, p)? as usize;
        (0..n).map(|k| Ok(u32_at(self.buf, p + 4 + 4 * k)? as i32)).collect()
    }

    pub(crate) fn tables(&self, index: usize) -> Result<Vec<Table<'a>>, String> {
        let Some(p) = self.reference(index)? else {
            return Ok(Vec::new());
        };
        let n = u32_at(self.buf, p)? as usize;
        (0..n)
            .map(|k| {
                let slot = p + 4 + 4 * k;
                Table::at(self.buf, slot + u32_at(self.buf, slot)? as usize)
            })
            .collect()
    }
}

/// A `KeyValuePair`: key (0), value type (1), value table (2).
fn key_value(kv: &Table) -> Result<(String, String), String> {
    let key = kv.string(0)?.unwrap_or_default();
    let kind = kv.u8(1)?;
    let value = match kv.table(2)? {
        None => String::new(),
        Some(v) if kind == 9 => v.string(0)?.unwrap_or_default(),
        Some(v) => match v.field(0)? {
            None => "0".into(),
            Some(p) => {
                let b = v.buf;
                match kind {
                    1 | 8 => b[p].to_string(),
                    5 | 6 => u32_at(b, p)?.to_string(),
                    10 | 11 => u64_at(b, p)?.to_string(),
                    _ => format!("<type {kind}>"),
                }
            }
        },
    };
    Ok((key, value))
}

impl Container {
    pub fn open(path: &str) -> Result<Self, String> {
        let file = File::open(path).map_err(|e| format!("{path}: {e}"))?;
        let map = unsafe { Mmap::map(&file) }.map_err(|e| format!("{path}: {e}"))?;
        if map.len() < 32 || &map[..8] != b"LITERTLM" {
            return Err(format!("{path}: not a .litertlm container"));
        }
        let header_end = u64_at(&map, 24)? as usize;
        let header = map
            .get(..header_end)
            .ok_or_else(|| format!("{path}: truncated header"))?;
        let root = Table::at(header, 32 + u32_at(header, 32)? as usize)?;

        let mut metadata = HashMap::new();
        if let Some(system) = root.table(0)? {
            for kv in system.tables(0)? {
                let (k, v) = key_value(&kv)?;
                metadata.insert(k, v);
            }
        }
        let mut sections = Vec::new();
        if let Some(list) = root.table(1)? {
            for object in list.tables(0)? {
                let mut items = HashMap::new();
                for kv in object.tables(0)? {
                    let (k, v) = key_value(&kv)?;
                    items.insert(k, v);
                }
                let (begin, end) = (object.u64(1)? as usize, object.u64(2)? as usize);
                if begin > end || end > map.len() {
                    return Err(format!("{path}: section out of bounds"));
                }
                sections.push(Section {
                    data_type: object.u8(3)?,
                    begin,
                    end,
                    items,
                });
            }
        }
        Ok(Self {
            map: Arc::new(map),
            sections,
            metadata,
        })
    }

    /// The first section of `data_type` whose item `key` is `value`.
    pub fn find(&self, data_type: u8, key: &str, value: &str) -> Option<&Section> {
        self.sections.iter().find(|s| {
            s.data_type == data_type && s.items.get(key).map(String::as_str) == Some(value)
        })
    }

    pub fn bytes(&self, section: &Section) -> &[u8] {
        &self.map[section.begin..section.end]
    }

    /// The mapping, for callers that keep borrowed sections alive.
    pub fn mapping(&self) -> Arc<Mmap> {
        Arc::clone(&self.map)
    }

    /// `GenericBinaryData` section by its `name` item.
    pub fn named(&self, name: &str) -> Option<&[u8]> {
        self.find(GENERIC_BINARY, "name", name)
            .map(|s| self.bytes(s))
    }
}
