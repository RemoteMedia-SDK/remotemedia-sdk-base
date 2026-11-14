//! A fix to Kokoro's vocoder graph so it compiles on LiteRT 2.3.2's GPU
//! delegate (ML Drift).
//!
//! The harmonic source ends in a FULLY_CONNECTED from `[1, 230400, 9]` to
//! `[230400, 1]`, then TANH. ML Drift takes the 230400 as the batch and
//! fails to fuse the TANH ("Unable to parse bc coord for BATCH axis"). With
//! `keep_num_dims` the output is `[1, 230400, 1]`, which it handles; the
//! RESHAPE after it gives the same tensor either way.
//!
//! The model is copied, the new `FullyConnectedOptions` table and shape
//! vectors are appended after it (FlatBuffer offsets only point forward, so
//! everything already there stays valid), and the offsets are rewritten.

use crate::container::{u32_at, Table};

const FULLY_CONNECTED: i32 = 9;
const LOGISTIC: i32 = 14;
const TANH: i32 = 28;

fn align(buf: &mut Vec<u8>, n: usize) {
    while buf.len() % n != 0 {
        buf.push(0);
    }
}

fn append_i32s(buf: &mut Vec<u8>, values: &[i32]) -> usize {
    align(buf, 4);
    let pos = buf.len();
    buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
    for v in values {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    pos
}

/// A `FullyConnectedOptions` table with `keep_num_dims` set.
fn append_fc_options(buf: &mut Vec<u8>, activation: u8, weights_format: u8) -> usize {
    align(buf, 4);
    let vtable = buf.len();
    // vtable size, table size, then field offsets: activation 4, format 5, keep 6.
    for v in [10u16, 8, 4, 5, 6] {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    align(buf, 4);
    let table = buf.len();
    buf.extend_from_slice(&((table - vtable) as i32).to_le_bytes());
    buf.extend_from_slice(&[activation, weights_format, 1, 0]);
    table
}

fn repoint(buf: &mut [u8], slot: usize, target: usize) {
    buf[slot..slot + 4].copy_from_slice(&((target - slot) as u32).to_le_bytes());
}

/// The patched model, and how many ops changed (0: returned unchanged).
pub fn keep_fc_dims(model: &[u8]) -> Result<(Vec<u8>, usize), String> {
    let root = Table::at(model, u32_at(model, 0)? as usize)?;
    let codes: Vec<i32> = root
        .tables(1)?
        .iter()
        .map(|c| Ok((c.u8(0)? as i8 as i32).max(c.u32(3)? as i32)))
        .collect::<Result<_, String>>()?;
    let subgraph = root
        .tables(2)?
        .into_iter()
        .next()
        .ok_or("model has no subgraph")?;
    let tensors = subgraph.tables(0)?;
    let ops = subgraph.tables(3)?;
    let shapes: Vec<Vec<i32>> = tensors.iter().map(|t| t.i32s(0)).collect::<Result<_, _>>()?;
    let mut consumers: std::collections::HashMap<i32, Vec<usize>> = Default::default();
    for (k, op) in ops.iter().enumerate() {
        for i in op.i32s(1)? {
            consumers.entry(i).or_default().push(k);
        }
    }
    let code = |op: &Table| -> Result<i32, String> {
        codes
            .get(op.u32(0)? as usize)
            .copied()
            .ok_or_else(|| "bad opcode index".to_string())
    };

    let mut out = model.to_vec();
    let mut patched = 0;
    for op in &ops {
        if code(op)? != FULLY_CONNECTED {
            continue;
        }
        let (inputs, outputs) = (op.i32s(1)?, op.i32s(2)?);
        let (Some(&x), Some(&y)) = (inputs.first(), outputs.first()) else {
            continue;
        };
        let (xs, ys) = (&shapes[x as usize], &shapes[y as usize]);
        if xs.len() != 3 || ys.len() != 2 {
            continue;
        }
        let Some(options_slot) = op.field(4)? else {
            continue;
        };
        let (activation, format) = match op.table(4)? {
            Some(o) => (o.u8(0)?, o.u8(1)?),
            None => (0, 0),
        };
        let table = append_fc_options(&mut out, activation, format);
        repoint(&mut out, options_slot, table);

        // The output, and single-consumer TANH / LOGISTIC chains after it.
        let mut chain = vec![y];
        let mut t = y;
        while let Some([next]) = consumers.get(&t).map(Vec::as_slice) {
            let next = &ops[*next];
            if !matches!(code(next)?, TANH | LOGISTIC) {
                break;
            }
            t = next.i32s(2)?[0];
            chain.push(t);
        }
        let shape = [xs[0], xs[1], ys[1]];
        for t in chain {
            let tensor = &tensors[t as usize];
            if let Some(slot) = tensor.field(0)? {
                let v = append_i32s(&mut out, &shape);
                repoint(&mut out, slot, v);
            }
            if let Some(slot) = tensor.field(7)? {
                let v = append_i32s(&mut out, &shape);
                repoint(&mut out, slot, v);
            }
        }
        patched += 1;
    }
    Ok((out, patched))
}
