//! Activation projection — read LLM hidden states out onto calibrated
//! direction vectors. Rust port of
//! [`tools/driver_affect_sim/lib/probes.py::project_VAD`] with the same
//! load-and-normalise contract.
//!
//! ## What this module is
//!
//! The Channel D steering NPZ produced by
//! `tools/affect_calibration/scripts/03b_extract_llm_directions_llama.py`
//! holds per-axis mean-difference direction vectors at one chosen layer.
//! Steering injects scaled copies of these vectors into the model's
//! residual stream during generation. *Reading them back out* — projecting
//! the model's hidden state onto the same directions — gives a scalar per
//! axis that estimates "how much of this direction is currently in the
//! model's state."
//!
//! Same coordinate system, opposite direction of dataflow. The
//! [`ActivationFaceNode`](super::super::activation_face::ActivationFaceNode)
//! consumes the projection output to drive the avatar's face from
//! whatever the LLM is actually generating, rather than from a
//! scenario-driven simulator.
//!
//! ## NPZ format expected
//!
//! Mirror of `03b_extract_llm_directions_llama.py`'s runtime-loadable
//! schema:
//!
//! - `D.npy`     — `<f4`, shape `(n_axes, n_embd)`. Per-axis raw
//!                 mean-difference vectors. **Not** unit-normalised in
//!                 the file; the loader unit-normalises in place and
//!                 caches the original norm for `project_normalised`.
//! - `n_embd.npy` — `<i4`, scalar. Hidden width.
//! - `layer.npy`  — `<i4`, scalar. The layer the directions were
//!                 captured at; surfaced for diagnostics only.
//! - `axes.npy`   — *optional*, object dtype. The Python pickle of axis
//!                 names. Object dtype is hard to parse from Rust, so
//!                 by default we ignore this entry and the caller must
//!                 supply labels via [`ActivationProjector::load`]'s
//!                 `labels` argument. The default is the
//!                 calibration-script default `["valence", "arousal",
//!                 "dominance"]` to match the Channel D NPZ.
//!
//! ## Why no labels in the NPZ here
//!
//! Updating the calibration script to also write a `<U`-dtype labels
//! array is the correct medium-term fix; today's NPZ uses `dtype=object`
//! and parsing Python pickles from Rust is out of scope. Until the
//! calibration script is updated, labels travel beside the NPZ via the
//! consumer node's config.

#![cfg(feature = "activation-face")]

use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use thiserror::Error;

/// Default axis labels for the Channel D V/A/D NPZ. Mirrors
/// `AXES = ("valence", "arousal", "dominance")` in
/// `03b_extract_llm_directions_llama.py:127`.
pub const DEFAULT_VAD_LABELS: &[&str] = &["valence", "arousal", "dominance"];

/// One row of the loaded direction matrix, paired with its calibration
/// anchor.
#[derive(Debug, Clone)]
pub struct ProjectionAxis {
    /// Axis label as supplied at load time (e.g. `"valence"`,
    /// `"happy"`).
    pub label: String,
    /// Unit-norm direction vector at the captured layer, length
    /// `n_embd`.
    pub direction: Vec<f32>,
    /// Original L2 norm of the direction before unit-normalisation.
    /// Used as the calibration anchor in [`project_normalised`].
    pub raw_norm: f32,
}

/// Loaded `(axes, hidden_size, layer)` projection bundle.
///
/// One projector per loaded NPZ. Constructed via
/// [`ActivationProjector::load`] from a path on disk; cheap clone via
/// `Arc` if shared across nodes.
#[derive(Debug, Clone)]
pub struct ActivationProjector {
    pub source_path: PathBuf,
    /// Hidden width — every input vector to [`project`] must match.
    pub hidden_size: usize,
    /// The layer the directions were captured at, surfaced from the
    /// NPZ's `layer.npy` entry. Diagnostic only.
    pub layer: i32,
    /// Per-axis directions in the order the labels were supplied to
    /// [`load`].
    pub axes: Vec<ProjectionAxis>,
}

/// Errors surfaced during projection load + use.
#[derive(Debug, Error)]
pub enum ActivationProjectorError {
    #[error("io error reading {1}: {0}")]
    Io(#[source] std::io::Error, String),

    #[error("zip error in {1}: {0}")]
    Zip(#[source] zip::result::ZipError, String),

    #[error("npy parse error in {1}: {0}")]
    Npy(String, String),

    #[error("missing required entry '{0}' in NPZ {1}")]
    MissingEntry(String, String),

    #[error(
        "label/direction count mismatch: NPZ has {npz_axes} direction rows but \
         {labels_supplied} labels were supplied (npz_path: {npz_path})"
    )]
    LabelCountMismatch {
        npz_axes: usize,
        labels_supplied: usize,
        npz_path: String,
    },

    #[error(
        "hidden-state dimension mismatch: projector expects n_embd={expected} \
         but got input length {actual}"
    )]
    DimensionMismatch { expected: usize, actual: usize },
}

impl ActivationProjector {
    /// Load a calibrated direction NPZ from disk and unit-normalise the
    /// direction rows in place. Caller supplies axis labels in the
    /// same order as the rows of `D.npy`.
    ///
    /// Use [`DEFAULT_VAD_LABELS`] for the Channel D V/A/D NPZ produced
    /// by `03b_extract_llm_directions_llama.py`.
    pub fn load<P: AsRef<Path>>(
        path: P,
        labels: &[&str],
    ) -> Result<Self, ActivationProjectorError> {
        let path = path.as_ref().to_path_buf();
        let source = path.display().to_string();

        let file =
            File::open(&path).map_err(|e| ActivationProjectorError::Io(e, source.clone()))?;
        let mut archive = zip::ZipArchive::new(file)
            .map_err(|e| ActivationProjectorError::Zip(e, source.clone()))?;

        let d_bytes = read_entry(&mut archive, "D.npy", &source)?;
        let directions = read_npy_f32(&d_bytes, &source)?;
        let layer_bytes = read_entry(&mut archive, "layer.npy", &source)?;
        let layer = read_npy_i32_scalar(&layer_bytes, &source)?;

        // n_embd is preferable when present; otherwise infer from D's last
        // axis. The runtime NPZ always writes it but the all-layers NPZ may
        // not — accept either.
        let hidden_size = match read_entry(&mut archive, "n_embd.npy", &source) {
            Ok(b) => read_npy_i32_scalar(&b, &source)? as usize,
            Err(_) => directions.shape.last().copied().unwrap_or(0),
        };

        Self::from_matrix(directions, hidden_size, layer, labels, path)
    }

    /// Construct from a pre-loaded direction matrix. Exposed for tests
    /// and for in-memory consumers (e.g. a future loader that pulls
    /// directions from a calibration service rather than a file).
    pub fn from_matrix(
        directions: NpyMatrix,
        hidden_size: usize,
        layer: i32,
        labels: &[&str],
        source_path: PathBuf,
    ) -> Result<Self, ActivationProjectorError> {
        let source = source_path.display().to_string();

        if directions.shape.len() != 2 {
            return Err(ActivationProjectorError::Npy(
                format!(
                    "expected 2-D direction matrix, got shape {:?}",
                    directions.shape
                ),
                source,
            ));
        }
        let n_axes = directions.shape[0];
        let n_embd = directions.shape[1];

        if n_embd != hidden_size && hidden_size != 0 {
            return Err(ActivationProjectorError::Npy(
                format!("n_embd ({n_embd}) does not match hidden_size ({hidden_size})"),
                source,
            ));
        }
        if labels.len() != n_axes {
            return Err(ActivationProjectorError::LabelCountMismatch {
                npz_axes: n_axes,
                labels_supplied: labels.len(),
                npz_path: source,
            });
        }

        let mut axes = Vec::with_capacity(n_axes);
        for (ai, label) in labels.iter().enumerate() {
            let start = ai * n_embd;
            let end = start + n_embd;
            let raw = &directions.data[start..end];
            let raw_norm: f32 = raw.iter().map(|x| x * x).sum::<f32>().sqrt();
            // Defensive: if a row is the zero vector (should never happen
            // in a real calibration but easy to guard against), keep it
            // zero rather than dividing by zero. Projection then produces
            // 0 for that axis, which is the sane neutral output.
            let direction = if raw_norm > 0.0 {
                raw.iter().map(|x| x / raw_norm).collect()
            } else {
                tracing::warn!(
                    label = %label,
                    "projection axis has zero norm — projection will always read 0",
                );
                vec![0.0; n_embd]
            };
            axes.push(ProjectionAxis {
                label: (*label).to_string(),
                direction,
                raw_norm,
            });
        }

        Ok(Self {
            source_path,
            hidden_size: n_embd,
            layer,
            axes,
        })
    }

    /// Project a hidden-state vector onto each unit-norm direction.
    /// Returns one scalar per axis label.
    ///
    /// Mirror of
    /// [`tools/driver_affect_sim/lib/probes.py::project_VAD`].
    pub fn project(
        &self,
        hidden: &[f32],
    ) -> Result<HashMap<String, f32>, ActivationProjectorError> {
        if hidden.len() != self.hidden_size {
            return Err(ActivationProjectorError::DimensionMismatch {
                expected: self.hidden_size,
                actual: hidden.len(),
            });
        }
        let mut out = HashMap::with_capacity(self.axes.len());
        for axis in &self.axes {
            let proj = dot(hidden, &axis.direction);
            out.insert(axis.label.clone(), proj);
        }
        Ok(out)
    }

    /// Same as [`project`] but each scalar is normalised by the
    /// per-axis half-anchor `raw_norm / 2` (mirrors
    /// [`tools/driver_affect_sim/lib/agent.py::SteeredAgent::_read_felt_VAD`]).
    /// Output range is approximately `[-1.5, 1.5]`; values outside that
    /// band are clamped.
    pub fn project_normalised(
        &self,
        hidden: &[f32],
    ) -> Result<HashMap<String, f32>, ActivationProjectorError> {
        if hidden.len() != self.hidden_size {
            return Err(ActivationProjectorError::DimensionMismatch {
                expected: self.hidden_size,
                actual: hidden.len(),
            });
        }
        let mut out = HashMap::with_capacity(self.axes.len());
        for axis in &self.axes {
            let proj = dot(hidden, &axis.direction);
            // The unit-normalised direction times raw_norm reproduces the
            // original mean-diff. The projection scalar's natural scale
            // is therefore proportional to raw_norm; dividing by half the
            // raw norm gives a roughly [-1, +1] range for typical
            // hidden states sampled from prompts on the same axis. The
            // ±0.5 slack in [-1.5, 1.5] absorbs out-of-distribution
            // states without clamping the obvious neutral region.
            let calib = (axis.raw_norm / 2.0).max(1.0);
            let normalised = (proj / calib).clamp(-1.5, 1.5);
            out.insert(axis.label.clone(), normalised);
        }
        Ok(out)
    }
}

#[inline]
fn dot(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

// ─── Inline NPY reader (subset) ────────────────────────────────────────────
//
// We need just enough of the NumPy `.npy` v1/v2 format to parse the two
// dtypes the calibration NPZ ships:
//   - `<f4` 2-D matrix (D.npy)
//   - `<i4` 0-D / 1-D scalar (n_embd.npy, layer.npy)
//
// The audio2face module already has a richer reader at
// `nodes/lip_sync/audio2face/npy.rs`, but it's gated behind the
// `avatar-audio2face` feature. Duplicating the small portion we need
// keeps the `activation-face` feature decoupled from the audio2face
// feature surface.
//
// Format reference: <https://numpy.org/doc/stable/reference/generated/numpy.lib.format.html>

const NPY_MAGIC: [u8; 6] = [0x93, b'N', b'U', b'M', b'P', b'Y'];

/// f32 matrix with row-major data and shape vector. Mirrors the
/// audio2face reader's output so future consolidation is mechanical.
#[derive(Debug, Clone)]
pub struct NpyMatrix {
    pub data: Vec<f32>,
    pub shape: Vec<usize>,
}

fn read_entry<R: Read + std::io::Seek>(
    archive: &mut zip::ZipArchive<R>,
    name: &str,
    source: &str,
) -> Result<Vec<u8>, ActivationProjectorError> {
    let mut entry = archive.by_name(name).map_err(|_| {
        ActivationProjectorError::MissingEntry(name.to_string(), source.to_string())
    })?;
    let mut bytes = Vec::with_capacity(entry.size() as usize);
    entry
        .read_to_end(&mut bytes)
        .map_err(|e| ActivationProjectorError::Io(e, source.to_string()))?;
    Ok(bytes)
}

/// Parse a `.npy` byte buffer as a row-major f32 array. Errors on
/// non-`<f4` dtype or truncated payload.
fn read_npy_f32(bytes: &[u8], source: &str) -> Result<NpyMatrix, ActivationProjectorError> {
    let (dtype, shape, payload) = parse_npy(bytes, source)?;
    if dtype != "<f4" && dtype != "f4" {
        return Err(ActivationProjectorError::Npy(
            format!("expected dtype '<f4', got '{dtype}'"),
            source.to_string(),
        ));
    }
    let total: usize = shape.iter().copied().product::<usize>().max(1);
    if payload.len() < total * 4 {
        return Err(ActivationProjectorError::Npy(
            format!(
                "truncated payload: need {} bytes, have {}",
                total * 4,
                payload.len()
            ),
            source.to_string(),
        ));
    }
    let mut data = Vec::with_capacity(total);
    for chunk in payload[..total * 4].chunks_exact(4) {
        let bytes_arr = [chunk[0], chunk[1], chunk[2], chunk[3]];
        data.push(f32::from_le_bytes(bytes_arr));
    }
    Ok(NpyMatrix { data, shape })
}

/// Parse a `.npy` byte buffer as a single i32 scalar (or 1-element
/// array). NumPy stores scalars as zero-d arrays, but on save the
/// runtime sometimes stores them as `(1,)`-shape — we handle both.
fn read_npy_i32_scalar(bytes: &[u8], source: &str) -> Result<i32, ActivationProjectorError> {
    let (dtype, shape, payload) = parse_npy(bytes, source)?;
    if dtype != "<i4" && dtype != "i4" {
        return Err(ActivationProjectorError::Npy(
            format!("expected dtype '<i4', got '{dtype}'"),
            source.to_string(),
        ));
    }
    let total: usize = shape.iter().copied().product::<usize>().max(1);
    if total != 1 {
        return Err(ActivationProjectorError::Npy(
            format!(
                "expected scalar i32, got shape {:?} ({total} elements)",
                shape
            ),
            source.to_string(),
        ));
    }
    if payload.len() < 4 {
        return Err(ActivationProjectorError::Npy(
            "truncated i32 scalar payload".to_string(),
            source.to_string(),
        ));
    }
    Ok(i32::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3],
    ]))
}

/// Split `.npy` bytes into `(dtype_str, shape, payload_bytes)`.
fn parse_npy<'a>(
    bytes: &'a [u8],
    source: &str,
) -> Result<(String, Vec<usize>, &'a [u8]), ActivationProjectorError> {
    if bytes.len() < 10 {
        return Err(ActivationProjectorError::Npy(
            "buffer too short for npy preamble".to_string(),
            source.to_string(),
        ));
    }
    if bytes[..6] != NPY_MAGIC {
        return Err(ActivationProjectorError::Npy(
            "bad magic — not an npy file".to_string(),
            source.to_string(),
        ));
    }
    let major = bytes[6];
    let (header_offset, header_len) = match major {
        1 => {
            let header_len = u16::from_le_bytes([bytes[8], bytes[9]]) as usize;
            (10, header_len)
        }
        2 => {
            if bytes.len() < 12 {
                return Err(ActivationProjectorError::Npy(
                    "truncated v2 header length".to_string(),
                    source.to_string(),
                ));
            }
            let header_len =
                u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;
            (12, header_len)
        }
        v => {
            return Err(ActivationProjectorError::Npy(
                format!("unsupported npy version {v}"),
                source.to_string(),
            ))
        }
    };
    let header_end = header_offset + header_len;
    if bytes.len() < header_end {
        return Err(ActivationProjectorError::Npy(
            "truncated npy header".to_string(),
            source.to_string(),
        ));
    }
    let header = std::str::from_utf8(&bytes[header_offset..header_end]).map_err(|_| {
        ActivationProjectorError::Npy(
            "npy header bytes are not valid UTF-8".to_string(),
            source.to_string(),
        )
    })?;

    let dtype = extract_string_value(header, "'descr':").ok_or_else(|| {
        ActivationProjectorError::Npy(
            "missing 'descr' key in npy header".to_string(),
            source.to_string(),
        )
    })?;
    let shape_str = extract_tuple_value(header, "'shape':").ok_or_else(|| {
        ActivationProjectorError::Npy(
            "missing 'shape' key in npy header".to_string(),
            source.to_string(),
        )
    })?;
    let inner = shape_str.trim_matches(|c: char| c == '(' || c == ')' || c.is_whitespace());
    let shape: Vec<usize> = if inner.is_empty() {
        vec![1]
    } else {
        inner
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.parse::<usize>().map_err(|_| {
                    ActivationProjectorError::Npy(
                        format!("non-integer dim '{s}' in shape"),
                        source.to_string(),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?
    };

    Ok((dtype, shape, &bytes[header_end..]))
}

fn extract_string_value(header: &str, key: &str) -> Option<String> {
    let idx = header.find(key)?;
    let rest = header[idx + key.len()..].trim_start();
    if !rest.starts_with('\'') {
        return None;
    }
    let after_quote = &rest[1..];
    let end = after_quote.find('\'')?;
    Some(after_quote[..end].to_string())
}

fn extract_tuple_value(header: &str, key: &str) -> Option<String> {
    let idx = header.find(key)?;
    let rest = header[idx + key.len()..].trim_start();
    if !rest.starts_with('(') {
        return None;
    }
    let end = rest.find(')')?;
    Some(rest[..=end].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a minimal valid v1 .npy byte stream from a dtype + shape
    /// + payload bytes. Mirror of the helper in
    /// `audio2face/npy.rs::tests::synth_npy` so tests here read
    /// independently.
    fn synth_npy(dtype: &str, shape: &[usize], payload: &[u8]) -> Vec<u8> {
        let shape_str = if shape.is_empty() {
            "()".to_string()
        } else if shape.len() == 1 {
            format!("({},)", shape[0])
        } else {
            let dims: Vec<String> = shape.iter().map(|d| d.to_string()).collect();
            format!("({})", dims.join(", "))
        };
        let header =
            format!("{{'descr': '{dtype}', 'fortran_order': False, 'shape': {shape_str}, }}");
        let preamble_len = 10;
        let mut header_bytes = header.into_bytes();
        let unpadded = preamble_len + header_bytes.len() + 1;
        let pad = (64 - (unpadded % 64)) % 64;
        header_bytes.extend(std::iter::repeat(b' ').take(pad));
        header_bytes.push(b'\n');
        let mut out = Vec::with_capacity(preamble_len + header_bytes.len() + payload.len());
        out.extend_from_slice(&NPY_MAGIC);
        out.push(1);
        out.push(0);
        out.extend_from_slice(&(header_bytes.len() as u16).to_le_bytes());
        out.extend_from_slice(&header_bytes);
        out.extend_from_slice(payload);
        out
    }

    /// Build an in-memory NPZ archive containing the given
    /// `(name, bytes)` entries. Returns the archive bytes ready to
    /// hand to `zip::ZipArchive::new`.
    fn synth_npz(entries: &[(&str, &[u8])]) -> Vec<u8> {
        use std::io::Write;
        let mut buf = Vec::new();
        {
            let mut zw = zip::ZipWriter::new(Cursor::new(&mut buf));
            let opts: zip::write::FileOptions<()> = zip::write::FileOptions::default()
                .compression_method(zip::CompressionMethod::Stored);
            for (name, bytes) in entries {
                zw.start_file(*name, opts).unwrap();
                zw.write_all(bytes).unwrap();
            }
            zw.finish().unwrap();
        }
        buf
    }

    fn f32_payload(values: &[f32]) -> Vec<u8> {
        values.iter().flat_map(|f| f.to_le_bytes()).collect()
    }

    #[test]
    fn from_matrix_unit_normalises_and_records_norms() {
        // 2 axes × 4-dim hidden. Row 0 is [3,4,0,0] (norm 5);
        // row 1 is [1,0,0,0] (norm 1).
        let matrix = NpyMatrix {
            data: vec![3.0, 4.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0],
            shape: vec![2, 4],
        };
        let proj = ActivationProjector::from_matrix(
            matrix,
            4,
            21,
            &["alpha", "beta"],
            PathBuf::from("synthetic.npz"),
        )
        .expect("from_matrix");
        assert_eq!(proj.hidden_size, 4);
        assert_eq!(proj.layer, 21);
        assert_eq!(proj.axes.len(), 2);
        assert_eq!(proj.axes[0].label, "alpha");
        assert!((proj.axes[0].raw_norm - 5.0).abs() < 1e-5);
        assert!((proj.axes[1].raw_norm - 1.0).abs() < 1e-5);
        // Unit-normalised: [3/5, 4/5, 0, 0] and [1, 0, 0, 0].
        let a = &proj.axes[0].direction;
        assert!((a[0] - 0.6).abs() < 1e-5);
        assert!((a[1] - 0.8).abs() < 1e-5);
        assert!((proj.axes[1].direction[0] - 1.0).abs() < 1e-5);
    }

    #[test]
    fn project_returns_dot_with_unit_directions() {
        let matrix = NpyMatrix {
            data: vec![1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0],
            shape: vec![2, 4],
        };
        let proj = ActivationProjector::from_matrix(
            matrix,
            4,
            0,
            &["x", "y"],
            PathBuf::from("synthetic.npz"),
        )
        .unwrap();
        let scores = proj.project(&[7.0, 11.0, 0.0, 0.0]).unwrap();
        // Unit-norm directions × the input gives the input components.
        assert!((scores["x"] - 7.0).abs() < 1e-5);
        assert!((scores["y"] - 11.0).abs() < 1e-5);
    }

    #[test]
    fn project_normalised_uses_half_raw_norm_calibration() {
        // raw_norm = 4 → calibration anchor = 2. Project with
        // ‖hidden‖ co-aligned to direction so we know the dot
        // product.
        let matrix = NpyMatrix {
            data: vec![4.0, 0.0],
            shape: vec![1, 2],
        };
        let proj =
            ActivationProjector::from_matrix(matrix, 2, 0, &["v"], PathBuf::from("synthetic.npz"))
                .unwrap();
        // Unit direction = [1, 0]. project([2, 0]) = 2.0. Normalised
        // by raw_norm/2 = 2 gives 1.0 exactly.
        let scores = proj.project_normalised(&[2.0, 0.0]).unwrap();
        assert!((scores["v"] - 1.0).abs() < 1e-5);
        // Off-axis input clamps to [-1.5, 1.5].
        let big = proj.project_normalised(&[100.0, 0.0]).unwrap();
        assert!((big["v"] - 1.5).abs() < 1e-5);
        let neg = proj.project_normalised(&[-100.0, 0.0]).unwrap();
        assert!((neg["v"] + 1.5).abs() < 1e-5);
    }

    #[test]
    fn project_dimension_mismatch_errors() {
        let matrix = NpyMatrix {
            data: vec![1.0, 0.0, 0.0, 0.0],
            shape: vec![1, 4],
        };
        let proj =
            ActivationProjector::from_matrix(matrix, 4, 0, &["v"], PathBuf::from("synthetic.npz"))
                .unwrap();
        let err = proj.project(&[1.0, 2.0, 3.0]).unwrap_err();
        assert!(matches!(
            err,
            ActivationProjectorError::DimensionMismatch {
                expected: 4,
                actual: 3
            }
        ));
    }

    #[test]
    fn label_count_mismatch_errors() {
        let matrix = NpyMatrix {
            data: vec![1.0, 0.0, 0.0, 1.0],
            shape: vec![2, 2],
        };
        let err = ActivationProjector::from_matrix(
            matrix,
            2,
            0,
            &["only_one_label"],
            PathBuf::from("synthetic.npz"),
        )
        .unwrap_err();
        match err {
            ActivationProjectorError::LabelCountMismatch {
                npz_axes,
                labels_supplied,
                npz_path,
            } => {
                assert_eq!(npz_axes, 2);
                assert_eq!(labels_supplied, 1);
                assert!(!npz_path.is_empty());
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn zero_norm_axis_warns_and_outputs_zero() {
        // Direction is the zero vector. Projection should always be 0
        // for that axis — no NaN, no Inf.
        let matrix = NpyMatrix {
            data: vec![0.0, 0.0, 0.0, 0.0],
            shape: vec![2, 2],
        };
        let proj = ActivationProjector::from_matrix(
            matrix,
            2,
            0,
            &["zero1", "zero2"],
            PathBuf::from("synthetic.npz"),
        )
        .unwrap();
        let scores = proj.project(&[3.0, 5.0]).unwrap();
        assert_eq!(scores["zero1"], 0.0);
        assert_eq!(scores["zero2"], 0.0);
    }

    #[test]
    fn load_round_trips_through_synthetic_npz() {
        // 3 axes × 2-dim. Hand-pick non-unit norms so we can confirm
        // the loader normalises in place.
        let directions = vec![
            3.0_f32, 4.0, // valence: ‖·‖ = 5
            0.0, 1.0, // arousal: ‖·‖ = 1
            -2.0, 0.0, // dominance: ‖·‖ = 2
        ];
        let d_npy = synth_npy("<f4", &[3, 2], &f32_payload(&directions));
        let n_embd_npy = synth_npy("<i4", &[], &(2i32).to_le_bytes());
        let layer_npy = synth_npy("<i4", &[], &(21i32).to_le_bytes());

        let archive_bytes = synth_npz(&[
            ("D.npy", &d_npy),
            ("n_embd.npy", &n_embd_npy),
            ("layer.npy", &layer_npy),
        ]);

        // Materialise to a tempfile so we can exercise the public
        // load-from-path API end-to-end.
        let mut tmp = std::env::temp_dir();
        tmp.push(format!(
            "activation_projector_test_{}.npz",
            std::process::id()
        ));
        std::fs::write(&tmp, &archive_bytes).unwrap();
        let proj = ActivationProjector::load(&tmp, DEFAULT_VAD_LABELS).unwrap();
        std::fs::remove_file(&tmp).ok();

        assert_eq!(proj.hidden_size, 2);
        assert_eq!(proj.layer, 21);
        assert_eq!(proj.axes.len(), 3);
        assert_eq!(proj.axes[0].label, "valence");
        assert!((proj.axes[0].raw_norm - 5.0).abs() < 1e-5);
        assert!((proj.axes[1].raw_norm - 1.0).abs() < 1e-5);
        assert!((proj.axes[2].raw_norm - 2.0).abs() < 1e-5);

        // Project a hidden state aligned to valence direction.
        // valence raw = [3,4]; unit = [0.6, 0.8]. dot([6, 8], [0.6, 0.8]) = 10.
        let scores = proj.project(&[6.0, 8.0]).unwrap();
        assert!((scores["valence"] - 10.0).abs() < 1e-4);
    }

    #[test]
    fn load_missing_directions_entry_errors() {
        let n_embd_npy = synth_npy("<i4", &[], &(2i32).to_le_bytes());
        let layer_npy = synth_npy("<i4", &[], &(0i32).to_le_bytes());
        let archive_bytes = synth_npz(&[("n_embd.npy", &n_embd_npy), ("layer.npy", &layer_npy)]);
        let mut tmp = std::env::temp_dir();
        tmp.push(format!(
            "activation_projector_test_missing_d_{}.npz",
            std::process::id()
        ));
        std::fs::write(&tmp, &archive_bytes).unwrap();
        let err = ActivationProjector::load(&tmp, &["x"]).unwrap_err();
        std::fs::remove_file(&tmp).ok();
        assert!(matches!(err, ActivationProjectorError::MissingEntry(name, _) if name == "D.npy"));
    }

    #[test]
    fn synth_npy_round_trips_through_local_parser() {
        // Sanity check on the inline parser independent of NPZ
        // wrapping — same shape as audio2face/npy.rs::tests.
        let payload = f32_payload(&[1.0, 2.5, -3.7, 4.0]);
        let bytes = synth_npy("<f4", &[4], &payload);
        let m = read_npy_f32(&bytes, "test").unwrap();
        assert_eq!(m.shape, vec![4]);
        assert_eq!(m.data, vec![1.0, 2.5, -3.7, 4.0]);
    }
}
