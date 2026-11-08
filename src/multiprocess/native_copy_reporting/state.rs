use super::*;

pub(super) struct Active {
    call: u64,
    copies: CopyContext,
    pending: [bool; COPY_REASONS.len()],
    finished: Option<Result<(), String>>,
}

#[derive(Default)]
pub(super) struct State {
    next_call: u64,
    next_report: u64,
    active: Option<Active>,
    failure: Option<String>,
}

impl State {
    pub(super) fn begin(&mut self, copies: CopyContext) -> io::Result<u64> {
        self.check()?;
        if copies.policy() != CopyPolicy::AllowCopies {
            return Err(invalid(
                "native compatibility reporting requires allow_copies",
            ));
        }
        if self
            .active
            .as_ref()
            .is_some_and(|a| !matches!(a.finished, Some(Ok(()))))
        {
            return Err(invalid("native copy call already active or failed"));
        }
        self.next_call = self
            .next_call
            .checked_add(1)
            .ok_or_else(|| invalid("native copy call ids exhausted"))?;
        self.active = Some(Active {
            call: self.next_call,
            copies,
            pending: [false; COPY_REASONS.len()],
            finished: None,
        });
        Ok(self.next_call)
    }

    pub(super) fn apply(&mut self, report: Report) -> io::Result<()> {
        self.check()?;
        let next = self
            .next_report
            .checked_add(1)
            .ok_or_else(|| invalid("native copy sequence exhausted"))?;
        if report.seq != next {
            return Err(invalid("native copy sequence mismatch"));
        }
        let active = self
            .active
            .as_mut()
            .ok_or_else(|| invalid("native copy report outside a call"))?;
        if active.call != report.call || active.finished.is_some() {
            return Err(invalid("native copy report for wrong or completed call"));
        }
        match report.event {
            Event::Copy { reason, event } => active.record(reason, event)?,
            Event::Finished { error } => {
                if active.pending.iter().any(|pending| *pending) {
                    return Err(invalid("native call finished with copies in flight"));
                }
                active.finished = Some(error.map_or(Ok(()), Err));
            }
        }
        self.next_report = next;
        Ok(())
    }

    pub(super) fn status(&self, call: u64) -> io::Result<bool> {
        self.check()?;
        let active = self
            .active
            .as_ref()
            .filter(|a| a.call == call)
            .ok_or_else(|| invalid("unknown native copy call"))?;
        match &active.finished {
            None => Ok(false),
            Some(Ok(())) => Ok(true),
            Some(Err(error)) => Err(io::Error::other(error.clone())),
        }
    }

    pub(super) fn fail(&mut self, error: impl ToString) {
        if self.failure.is_some() {
            return;
        }
        if let Some(active) = &mut self.active {
            for (index, pending) in active.pending.iter_mut().enumerate() {
                if std::mem::take(pending) {
                    // Unknown bytes on disconnect are an explicitly failed operation,
                    // never evidence of zero transferred bytes.
                    active
                        .copies
                        .record(COPY_REASONS[index], CopyEvent::Failed(0));
                }
            }
        }
        self.failure = Some(
            match self.active.as_ref().and_then(|a| a.finished.as_ref()) {
                Some(Err(message)) => message.clone(),
                _ => error.to_string(),
            },
        );
    }

    fn check(&self) -> io::Result<()> {
        match &self.failure {
            Some(error) => Err(io::Error::other(error.clone())),
            None => Ok(()),
        }
    }
}

impl Active {
    fn record(&mut self, reason: CopyReason, event: WireCopy) -> io::Result<()> {
        let pending = &mut self.pending[reason as usize];
        let event = match event {
            WireCopy::Started if !*pending => {
                *pending = true;
                CopyEvent::Started
            }
            WireCopy::Completed { bytes } if *pending => {
                *pending = false;
                CopyEvent::Completed(bytes)
            }
            WireCopy::Failed { bytes } if *pending => {
                *pending = false;
                CopyEvent::Failed(bytes)
            }
            WireCopy::Rejected { bytes } if !*pending => CopyEvent::Rejected(bytes),
            _ => return Err(invalid("invalid native copy operation transition")),
        };
        self.copies.record(reason, event);
        Ok(())
    }
}
