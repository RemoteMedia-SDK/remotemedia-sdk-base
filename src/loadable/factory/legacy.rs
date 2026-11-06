//! Explicit compatibility adapter. Protocol 0 cannot carry the host ledger into
//! the plugin, so only host codec operations can be attributed here.
use super::*;
use crate::multiprocess::data_transfer::{
    from_bytes_with_copy_context, to_bytes_with_copy_context,
};
use remotemedia_types::CopyPolicy;

impl LoadableNodeAdapter {
    pub(super) fn legacy_copies(
        &self,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<CopyContext, Error> {
        let copies = ctx.copy_context().ok_or_else(|| {
            self.legacy_error("missing copy context; protocol 0 requires explicit allow_copies")
        })?;
        if copies.policy() != CopyPolicy::AllowCopies {
            return Err(self.legacy_error(
                "require_zero_copy rejects protocol 0; rebuild with shared buffer protocol 2",
            ));
        }
        Ok(copies)
    }

    fn legacy_error(&self, detail: &str) -> Error {
        Error::Execution(format!(
            "legacy plugin '{}' (id={}): {detail}",
            self.node_type, self.node_id,
        ))
    }

    fn legacy_encode(&self, data: &RuntimeData, copies: &CopyContext) -> Result<RVec<u8>, Error> {
        to_bytes_with_copy_context(data, copies)
            .map(Into::into)
            .map_err(|error| self.legacy_error(&error))
    }

    pub(super) fn legacy_decode(
        &self,
        bytes: &[u8],
        copies: &CopyContext,
    ) -> Result<RuntimeData, Error> {
        from_bytes_with_copy_context(bytes, copies).map_err(|error| self.legacy_error(&error))
    }

    pub(super) async fn legacy_process(
        &self,
        data: RuntimeData,
        copies: &CopyContext,
    ) -> Result<RuntimeData, Error> {
        let output = self
            .inner
            .process(self.legacy_encode(&data, copies)?)
            .await
            .into_result()
            .map_err(|e| Error::from_plugin(e.into_string(), &self.node_id, &self.node_type))?;
        self.legacy_decode(output.as_slice(), copies)
    }

    pub(super) async fn legacy_control(
        &self,
        message: RuntimeData,
        session: Option<String>,
        copies: &CopyContext,
    ) -> Result<bool, Error> {
        self.inner
            .process_control_message(
                self.legacy_encode(&message, copies)?,
                session.unwrap_or_default().into(),
            )
            .await
            .into_result()
            .map_err(|e| Error::from_plugin(e.into_string(), &self.node_id, &self.node_type))
    }

    pub(super) async fn legacy_stream<F>(
        &self,
        data: RuntimeData,
        session: Option<String>,
        copies: &CopyContext,
        callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let bytes = self.legacy_encode(&data, copies)?;
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let sink: OutputSinkBox = OutputSink_TO::from_value(ChannelSink { tx }, TD_Opaque);
        let future = self.inner.process_streaming_with_context(
            bytes,
            FfiCallContext {
                session_id: session.map(RString::from).into(),
                node_id: self.node_id.clone().into(),
            },
            sink,
        );
        self.drain_outputs(future, rx, Some(copies), callback).await
    }

    pub(super) async fn legacy_tick(
        &self,
        tick: Tick,
        session: Option<String>,
        copies: &CopyContext,
        callback: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<(), Error> {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let sink = OutputSink_TO::from_value(ChannelSink { tx }, TD_Opaque);
        let future = self.inner.tick(
            loadable_node_abi::FfiTick {
                clock_id: tick.clock_id.into(),
                pts_us: tick.pts_us,
                frame_idx: tick.frame_idx,
                deadline_us: tick.deadline_us,
            },
            session.unwrap_or_default().into(),
            sink,
        );
        self.drain_outputs(future, rx, Some(copies), callback)
            .await
            .map(|_| ())
    }
}
