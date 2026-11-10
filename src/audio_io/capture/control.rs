use super::*;

pub(super) struct CaptureRun {
    pub config: CaptureConfig,
    pub context: NodeRuntimeContext,
    pub audio: crate::rt_bridge::PreparedAudioConfig,
    pub stop: Arc<AtomicBool>,
    pub output: async_mpsc::Sender<RuntimeData>,
    pub recording: bool,
}

impl CaptureRun {
    pub fn run<S>(
        self,
        started: &mpsc::SyncSender<Result<String, String>>,
        open: impl FnOnce(&CaptureConfig, CaptureCallback, Arc<CaptureCounters>) -> Result<(S, String)>,
    ) -> Result<()> {
        let Self {
            config,
            context,
            audio,
            stop,
            output,
            recording,
        } = self;
        let complete = recording.then(|| stop.clone());
        let (mut worker, callback) = CaptureWorker::prepare(context, audio, complete)?;
        let (stream, name) = open(&config, callback, worker.counters.clone())?;
        let _ = started.send(Ok(name));
        let mut status = Ok(());
        while !stop.load(Ordering::Acquire) && !output.is_closed() {
            if let Err(error) = worker.pump(&output) {
                status = Err(error);
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        drop(stream); // callback returns its producer before the owner reclaims it
        let finished = worker.finish(&output);
        // Final counters include losses incurred while publishing/draining the
        // tail. Prefer that failure over an earlier, incomplete loss snapshot.
        finished.and(status)
    }
}
