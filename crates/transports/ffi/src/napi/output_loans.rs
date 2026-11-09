//! Exclusive producers without escaped writable JS aliases.
use super::pipeline::NapiRuntimeData;
use napi::{Error, Result};
use napi_derive::napi;
use remotemedia_types::{BufferLoan, OutputAllocator, RuntimeData};
use std::sync::Arc;

#[napi]
pub struct NapiOutputAllocator {
    pub(crate) inner: Arc<dyn OutputAllocator>,
    pub(crate) copies: remotemedia_types::CopyContext,
}

#[napi]
impl NapiOutputAllocator {
    #[napi]
    pub fn allocate_bytes(&self, length: u32) -> Result<NapiByteLoan> {
        let loan = self
            .inner
            .allocate_bytes(length as usize)
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(NapiByteLoan {
            loan: Some(loan),
            copies: self.copies.clone(),
        })
    }

    #[napi]
    pub fn allocate_audio(
        &self,
        samples: u32,
        sample_rate: u32,
        channels: u32,
    ) -> Result<NapiAudioLoan> {
        if sample_rate == 0 || channels == 0 || samples % channels != 0 {
            return Err(Error::from_reason(
                "audio requires a positive rate/channels and whole frames",
            ));
        }
        let loan = self
            .inner
            .allocate_audio(samples as usize)
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(NapiAudioLoan {
            copies: self.copies.clone(),
            loan: Some(loan),
            sample_rate,
            channels,
        })
    }
}

fn open<T>(loan: &mut Option<BufferLoan<T>>) -> Result<&mut BufferLoan<T>> {
    loan.as_mut()
        .ok_or_else(|| Error::from_reason("output loan has already been published"))
}

fn set<T>(loan: &mut Option<BufferLoan<T>>, index: u32, value: T) -> Result<()> {
    let target = open(loan)?
        .as_mut_slice()
        .get_mut(index as usize)
        .ok_or_else(|| Error::from_reason("output index out of range"))?;
    *target = value;
    Ok(())
}

/// Generates bytes directly in the selected provider. No JS Buffer is exported.
#[napi]
pub struct NapiByteLoan {
    copies: remotemedia_types::CopyContext,
    loan: Option<BufferLoan<u8>>,
}

#[napi]
impl NapiByteLoan {
    #[napi]
    pub fn set(&mut self, index: u32, value: u32) -> Result<()> {
        let value =
            u8::try_from(value).map_err(|_| Error::from_reason("byte value exceeds 255"))?;
        set(&mut self.loan, index, value)
    }

    #[napi]
    pub fn fill(&mut self, value: u32) -> Result<()> {
        let value =
            u8::try_from(value).map_err(|_| Error::from_reason("byte value exceeds 255"))?;
        open(&mut self.loan)?.as_mut_slice().fill(value);
        Ok(())
    }

    #[napi]
    pub fn publish(&mut self) -> Result<NapiRuntimeData> {
        open(&mut self.loan)?;
        let loan = self.loan.take().expect("loan checked above");
        Ok(NapiRuntimeData::from_runtime_with_context(
            RuntimeData::Binary(loan.freeze()),
            &self.copies,
        ))
    }
}

/// Generates f32 samples directly in the selected provider.
#[napi]
pub struct NapiAudioLoan {
    copies: remotemedia_types::CopyContext,
    loan: Option<BufferLoan<f32>>,
    sample_rate: u32,
    channels: u32,
}

#[napi]
impl NapiAudioLoan {
    #[napi]
    pub fn set(&mut self, index: u32, value: f64) -> Result<()> {
        set(&mut self.loan, index, value as f32)
    }

    #[napi]
    pub fn fill(&mut self, value: f64) -> Result<()> {
        open(&mut self.loan)?.as_mut_slice().fill(value as f32);
        Ok(())
    }

    #[napi]
    pub fn publish(&mut self) -> Result<NapiRuntimeData> {
        open(&mut self.loan)?;
        let loan = self.loan.take().expect("loan checked above");
        Ok(NapiRuntimeData::from_runtime_with_context(
            RuntimeData::Audio {
                samples: loan.freeze().into(),
                sample_rate: self.sample_rate,
                channels: self.channels,
                stream_id: None,
                timestamp_us: None,
                arrival_ts_us: None,
                metadata: None,
            },
            &self.copies,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use remotemedia_types::{BoundOutputAllocator, BufferBudget, HeapOutputAllocator};

    fn allocator() -> (NapiOutputAllocator, BufferBudget) {
        let budget = BufferBudget::new(1, 16);
        let inner =
            BoundOutputAllocator::new(Arc::new(HeapOutputAllocator)).with_budget(budget.clone());
        (
            NapiOutputAllocator {
                inner: Arc::new(inner),
                copies: remotemedia_types::CopyContext::legacy().clone(),
            },
            budget,
        )
    }

    #[test]
    fn publication_retains_exact_pointer_and_budget_through_slice() {
        let (allocator, budget) = allocator();
        let mut loan = allocator.allocate_bytes(16).unwrap();
        let pointer = loan.loan.as_ref().unwrap().as_ptr();
        loan.fill(7).unwrap();
        let output = loan.publish().unwrap();
        assert!(loan.fill(8).is_err());
        assert!(loan.publish().is_err());
        let RuntimeData::Binary(bytes) = output.get_inner() else {
            unreachable!()
        };
        assert_eq!(bytes.as_ptr(), pointer);
        let slice = output.slice_binary(4, 8).unwrap();
        assert!(output.shares_buffer_with(&slice).unwrap());
        let RuntimeData::Binary(bytes) = slice.get_inner() else {
            unreachable!()
        };
        assert_eq!(bytes.as_ptr(), pointer.wrapping_add(4));
        drop(output);
        drop(allocator);
        assert_eq!(budget.usage().bytes, 16);
        assert_eq!(slice.read_byte(0).unwrap(), 7);
        drop(slice);
        assert_eq!(budget.usage().bytes, 0);
    }

    #[test]
    fn abandoned_and_published_audio_loans_restore_capacity() {
        let (allocator, budget) = allocator();
        let loan = allocator.allocate_bytes(16).unwrap();
        assert!(allocator.allocate_bytes(1).is_err());
        drop(loan);
        assert_eq!(budget.usage().owners, 0);
        let mut loan = allocator.allocate_audio(4, 48000, 1).unwrap();
        let pointer = loan.loan.as_ref().unwrap().as_ptr();
        loan.set(1, -0.5).unwrap();
        let output = loan.publish().unwrap();
        let RuntimeData::Audio { samples, .. } = output.get_inner() else {
            unreachable!()
        };
        assert_eq!(samples.as_ptr(), pointer);
        assert_eq!(output.read_audio_sample(1).unwrap(), -0.5);
        assert!(loan.set(0, 1.0).is_err());
        drop(output);
        assert_eq!(budget.usage().owners, 0);
    }

    #[test]
    fn strict_policy_rejects_before_accessing_or_allocating_output() {
        assert!(
            super::super::buffers::materialize(Some(true), 16, || -> Result<()> {
                panic!("strict rejection must precede operation")
            })
            .is_err()
        );
        super::super::buffers::materialize(Some(true), 0, || Ok(())).unwrap();
    }
}
