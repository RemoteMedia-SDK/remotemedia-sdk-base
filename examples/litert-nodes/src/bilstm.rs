//! `litert_custom_op.bidirectional_lstm`: a whole bidirectional LSTM layer as
//! one custom op, which Kokoro's acoustic model uses (LiteRT's
//! `litert/experimental/custom_ops/bidirectional_lstm`, ported). Stock LiteRT
//! runtimes do not ship the kernel; `compiled.rs` and `litert.rs` register
//! this one with LiteRT's CompiledModel and the TFLite interpreter.
//!
//! Nine inputs: x `[B, T, D]`, seq_lengths `[B]` (i32), mask `[B, T]` (1.0 on
//! padding), then for the forward and backward directions w_ih `[4H, D]`,
//! w_hh `[4H, H]`, bias `[4H]` (PyTorch gate order i, f, g, o). One output
//! `[B, T, 2H]`: forward in `[0, H)`, backward in `[H, 2H)`.

pub const OP_NAME: &str = "litert_custom_op.bidirectional_lstm";
pub const INPUTS: usize = 9;

fn sigmoid(x: f32) -> f32 {
    1.0 / (1.0 + (-x).exp())
}

/// `out[T, H]` of one LSTM direction over `x[T, D]`, zero initial state.
fn unidirectional(
    x: &[f32],
    w_ih: &[f32],
    w_hh: &[f32],
    bias: &[f32],
    out: &mut [f32],
    t_steps: usize,
    d: usize,
    h: usize,
) {
    let g = 4 * h;
    // gates_all[T, 4H] = x[T, D] · w_ih[4H, D]ᵀ, the time-independent part.
    let mut gates_all = vec![0f32; t_steps * g];
    unsafe {
        matrixmultiply::sgemm(
            t_steps,
            d,
            g,
            1.0,
            x.as_ptr(),
            d as isize,
            1,
            w_ih.as_ptr(),
            1,
            d as isize,
            0.0,
            gates_all.as_mut_ptr(),
            g as isize,
            1,
        );
    }
    let mut state = vec![0f32; h];
    let mut cell = vec![0f32; h];
    let mut gates = vec![0f32; g];
    for t in 0..t_steps {
        // gates = w_hh · state + gates_all[t] + bias
        let row = &gates_all[t * g..(t + 1) * g];
        for (j, out) in gates.iter_mut().enumerate() {
            let w = &w_hh[j * h..(j + 1) * h];
            let mut acc = 0f32;
            for (a, b) in w.iter().zip(&state) {
                acc += a * b;
            }
            *out = acc + row[j] + bias[j];
        }
        for k in 0..h {
            let i = sigmoid(gates[k]);
            let f = sigmoid(gates[h + k]);
            let c = gates[2 * h + k].tanh();
            let o = sigmoid(gates[3 * h + k]);
            cell[k] = f * cell[k] + i * c;
            state[k] = o * cell[k].tanh();
        }
        out[t * h..(t + 1) * h].copy_from_slice(&state);
    }
}

fn reverse_index(length: i32, t: usize, t_steps: usize) -> usize {
    (length - 1 - t as i32).clamp(0, t_steps as i32 - 1) as usize
}

/// The whole layer (`ComputeBidirectionalLstm`): the backward direction
/// starts at each sequence's last real step; padded steps are zeroed.
#[allow(clippy::too_many_arguments)]
pub fn compute(
    x: &[f32],
    seq_lengths: &[i32],
    mask: &[f32],
    fwd: (&[f32], &[f32], &[f32]),
    bwd: (&[f32], &[f32], &[f32]),
    out: &mut [f32],
    batch: usize,
    t_steps: usize,
    d: usize,
    h: usize,
) {
    let mut fwd_out = vec![0f32; t_steps * h];
    let mut reversed_in = vec![0f32; t_steps * d];
    let mut reversed_out = vec![0f32; t_steps * h];
    for b in 0..batch {
        let x_b = &x[b * t_steps * d..(b + 1) * t_steps * d];
        let mask_b = &mask[b * t_steps..(b + 1) * t_steps];
        let out_b = &mut out[b * t_steps * 2 * h..(b + 1) * t_steps * 2 * h];
        let length = seq_lengths[b];
        let active = (length.max(1) as usize).min(t_steps);
        fwd_out.fill(0.0);
        unidirectional(x_b, fwd.0, fwd.1, fwd.2, &mut fwd_out, active, d, h);
        for t in 0..active {
            let keep = 1.0 - mask_b[t];
            let src = &x_b[reverse_index(length, t, t_steps) * d..][..d];
            for (dst, s) in reversed_in[t * d..(t + 1) * d].iter_mut().zip(src) {
                *dst = s * keep;
            }
        }
        unidirectional(&reversed_in, bwd.0, bwd.1, bwd.2, &mut reversed_out, active, d, h);
        for t in 0..t_steps {
            let keep = 1.0 - mask_b[t];
            let r = reverse_index(length, t, t_steps);
            let dst = &mut out_b[t * 2 * h..(t + 1) * 2 * h];
            for i in 0..h {
                dst[i] = fwd_out[t * h + i] * keep;
                dst[h + i] = reversed_out[r * h + i] * keep;
            }
        }
    }
}

/// Runs the op on nine input slices, checking their sizes against the
/// shapes (`x_dims` = `[B, T, D]`, `hidden` = H).
pub fn run(inputs: [&[u8]; INPUTS], out: &mut [u8], x_dims: [usize; 3], hidden: usize) -> Result<(), String> {
    let [batch, t_steps, d] = x_dims;
    let h = hidden;
    let f = |bytes: &[u8], n: usize, what: &str| -> Result<&[f32], String> {
        if bytes.len() < n * 4 || bytes.as_ptr() as usize % 4 != 0 {
            return Err(format!("bidirectional_lstm: {what} is {} bytes, need {}", bytes.len(), n * 4));
        }
        Ok(unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const f32, n) })
    };
    let x = f(inputs[0], batch * t_steps * d, "x")?;
    if inputs[1].len() < batch * 4 {
        return Err("bidirectional_lstm: seq_lengths too short".into());
    }
    let lengths = unsafe { std::slice::from_raw_parts(inputs[1].as_ptr() as *const i32, batch) };
    let mask = f(inputs[2], batch * t_steps, "mask")?;
    let fwd = (
        f(inputs[3], 4 * h * d, "w_ih_fwd")?,
        f(inputs[4], 4 * h * h, "w_hh_fwd")?,
        f(inputs[5], 4 * h, "bias_fwd")?,
    );
    let bwd = (
        f(inputs[6], 4 * h * d, "w_ih_bwd")?,
        f(inputs[7], 4 * h * h, "w_hh_bwd")?,
        f(inputs[8], 4 * h, "bias_bwd")?,
    );
    let n = batch * t_steps * 2 * h;
    if out.len() < n * 4 {
        return Err("bidirectional_lstm: output too small".into());
    }
    let out = unsafe { std::slice::from_raw_parts_mut(out.as_mut_ptr() as *mut f32, n) };
    compute(x, lengths, mask, fwd, bwd, out, batch, t_steps, d, h);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One step of a 1-unit LSTM by hand against the kernel.
    #[test]
    fn single_unit_matches_hand_computation() {
        let (d, h) = (1, 1);
        let w_ih = [0.5, -0.25, 1.0, 0.75]; // i f g o
        let w_hh = [0.1, 0.2, 0.3, 0.4];
        let bias = [0.0, 0.1, -0.1, 0.2];
        let x = [2.0];
        let mut out = [0f32];
        unidirectional(&x, &w_ih, &w_hh, &bias, &mut out, 1, d, h);
        let i = sigmoid(1.0);
        let c = (2.0f32 - 0.1).tanh();
        let o = sigmoid(1.5 + 0.2);
        let expected = o * (i * c).tanh();
        assert!((out[0] - expected).abs() < 1e-6, "{} vs {expected}", out[0]);
    }

    #[test]
    fn padding_is_zeroed_and_backward_starts_at_last_real_step() {
        let (b, t, d, h) = (1, 3, 1, 1);
        let w = ([1.0f32, 1.0, 1.0, 1.0], [0.0f32; 4], [0.0f32; 4]);
        let x = [1.0, 2.0, 99.0];
        let mask = [0.0, 0.0, 1.0];
        let mut out = [0f32; 6];
        compute(&x, &[2], &mask, (&w.0, &w.1, &w.2), (&w.0, &w.1, &w.2), &mut out, b, t, d, h);
        assert_eq!(&out[4..], &[0.0, 0.0]);
        // Backward at t = 1 sees only x[1] (the first step of its scan).
        let mut first = [0f32];
        unidirectional(&[2.0], &w.0, &w.1, &w.2, &mut first, 1, d, h);
        assert!((out[3] - first[0]).abs() < 1e-6);
    }
}
