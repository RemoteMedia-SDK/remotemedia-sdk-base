//! What the Whisper engine needs from a LiteRT model: named signatures with
//! named inputs and outputs, written and read as host memory. Two
//! implementations: the TFLite interpreter on XNNPACK (`litert`, CPU) and
//! LiteRT's CompiledModel API (`compiled`, GPU / NPU / CPU accelerators).

use std::ops::Range;

pub trait Backend {
    /// Elements of an input (f32 or i32: four bytes each).
    fn input_len(&self, signature: &str, input: &str) -> Result<usize, String>;
    /// Elements of an output (f32).
    fn output_len(&self, signature: &str, output: &str) -> Result<usize, String>;
    fn write_f32(&mut self, signature: &str, input: &str, data: &[f32]) -> Result<(), String>;
    fn write_i32(&mut self, signature: &str, input: &str, data: &[i32]) -> Result<(), String>;
    fn invoke(&mut self, signature: &str) -> Result<(), String>;
    /// `range` (in elements) of an output, after `invoke`.
    fn read_f32(
        &mut self,
        signature: &str,
        output: &str,
        range: Range<usize>,
        out: &mut Vec<f32>,
    ) -> Result<(), String>;
    /// What runs the model, for logs: library and accelerator.
    fn describe(&self) -> String;
    /// Bytes of an input (`output` false) or output, and its element type
    /// (TFLite numbering: 1 f32, 2 i32, 4 i64).
    fn tensor_info(&self, signature: &str, name: &str, output: bool) -> Result<(usize, u32), String>;
    /// Writes `data` at the start of an input and zeroes the rest.
    fn write_bytes(&mut self, signature: &str, input: &str, data: &[u8]) -> Result<(), String>;
    /// A whole output's bytes, after `invoke`.
    fn read_bytes(&mut self, signature: &str, output: &str, out: &mut Vec<u8>) -> Result<(), String>;
    /// Gives inputs new shapes and reallocates (outputs follow). Not every
    /// backend or model can.
    fn resize_inputs(&mut self, _signature: &str, _shapes: &[(&str, &[i32])]) -> Result<(), String> {
        Err("this backend cannot resize inputs".into())
    }
}
