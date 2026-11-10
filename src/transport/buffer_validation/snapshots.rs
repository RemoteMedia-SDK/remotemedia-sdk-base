//! Portable snapshot boundary checks. Only metadata/proof owners are cloned.
use super::*;
use remotemedia_traits::ports::TimestampedSnapshot;
use std::any::Any;

pub(super) fn validate_binding(
    checks: &Checks,
    port: &str,
    reader: &dyn SnapshotPort,
    output: bool,
) -> Result<(), Error> {
    let ports = if output {
        &checks.contract.outputs
    } else {
        &checks.contract.inputs
    };
    if !reader.supports_runtime_data() || !ports.contains_key(port) {
        return Err(Error::Execution(format!(
            "buffer contract node '{}' snapshot port '{port}' requires portable RuntimeData and an exact port contract",
            checks.node_id,
        )));
    }
    Ok(())
}

pub(super) fn checked(
    checks: Arc<Checks>,
    port: String,
    inner: Arc<dyn SnapshotPort>,
    output: bool,
) -> Arc<dyn SnapshotPort> {
    Arc::new(CheckedSnapshot {
        checks,
        port,
        inner,
        output,
    })
}

struct CheckedSnapshot {
    checks: Arc<Checks>,
    port: String,
    inner: Arc<dyn SnapshotPort>,
    output: bool,
}
impl CheckedSnapshot {
    fn read(&self) -> Result<Option<Arc<TimestampedSnapshot<RuntimeData>>>, Error> {
        self.checks.snapshot_failure.result(Ok(()))?;
        validate_binding(&self.checks, &self.port, self.inner.as_ref(), self.output)?;
        let Some(snapshot) = self.inner.runtime_snapshot()? else {
            return Ok(None);
        };
        // An Arc publication cannot surrender a legacy Vec for promotion. Do not
        // clone it: require the producer to publish an owning shared view first.
        if matches!(
            &snapshot.value,
            RuntimeData::Audio {
                samples: remotemedia_types::AudioSamples::Vec(_),
                ..
            }
        ) {
            return Err(Error::Execution(
                "snapshot legacy audio requires owner-preserving publication".into(),
            ));
        }
        self.checks
            .check(&snapshot.value, &self.port, self.output)?;
        if self.output && self.checks.contract.production != BufferProduction::PrivateStorage {
            Ok(Some(snapshot))
        } else {
            // Retain credit on observed private publications, just as for input
            // snapshots. The raw producer's unpublished/internal storage is not
            // allocator-bound; this admission cannot bound those allocations.
            Ok(Some(Arc::new(TimestampedSnapshot {
                value: self.checks.admit(snapshot.value.clone())?,
                pts_us: snapshot.pts_us,
                written_at_us: snapshot.written_at_us,
                seq: snapshot.seq,
            })))
        }
    }
}
impl SnapshotPort for CheckedSnapshot {
    fn supports_runtime_data(&self) -> bool {
        self.inner.supports_runtime_data()
    }
    fn runtime_snapshot(&self) -> Result<Option<Arc<TimestampedSnapshot<RuntimeData>>>, Error> {
        self.checks.snapshot_failure.capture(self.read())
    }
    fn snapshot_any(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        // Legacy callers cannot carry an error. runtime_snapshot latches it and
        // the enclosing checked invocation fails, including ignored read errors.
        self.runtime_snapshot()
            .ok()
            .flatten()
            .map(|v| v as Arc<dyn Any + Send + Sync>)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}
