//! Select one live budget for admitted inputs and bound producer allocations.
use super::*;
use remotemedia_types::{BoundOutputAllocator, BufferBudget, BufferBudgetUsage};

pub(super) fn bind(
    contract: &BufferContract,
    allocator: Option<&BoundOutputAllocator>,
) -> Result<Option<BufferBudget>, Error> {
    let Some(limit) = contract.retention else {
        return Ok(None);
    };
    if let Some(budget) = allocator.and_then(BoundOutputAllocator::budget) {
        if budget.limits()
            != (BufferBudgetUsage {
                owners: limit.owners,
                bytes: limit.bytes,
            })
        {
            return Err(Error::Execution(
                "node allocator retention budget differs from its buffer contract".into(),
            ));
        }
        return Ok(Some(budget.clone()));
    }
    if matches!(
        contract.production,
        BufferProduction::OutputAllocator | BufferProduction::ForwardOrOutputAllocator
    ) {
        return Err(Error::Execution(
            "node output allocator requires the declared live retention budget".into(),
        ));
    }
    Ok(Some(BufferBudget::new(limit.owners, limit.bytes)))
}
