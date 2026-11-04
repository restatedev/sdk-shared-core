use super::*;

pub(super) struct Fault {
    at: usize,
    seen: usize,
    after: bool,
    cancel: bool,
    reached: Option<futures::channel::oneshot::Sender<()>>,
}

impl KvIo {
    /// Open the namespace's state, and an adapter over its file.
    pub(crate) async fn mount(store: Arc<dyn crate::state::kv::KvStore>) -> io::Result<Arc<Self>> {
        Self::mount_with_options(store, crate::state::StateOptions::default()).await
    }

    /// [`Self::mount`] with explicit state options.
    pub(crate) async fn mount_with_options(
        store: Arc<dyn crate::state::kv::KvStore>,
        options: crate::state::StateOptions,
    ) -> io::Result<Arc<Self>> {
        let state = crate::state::State::open(store).options(options).await;
        let state = state.map_err(io::Error::other)?;
        let io = Self::new(
            state.file(),
            crate::sql::Limits::default().max_private_bytes,
        );
        *io.state.lock().unwrap() = Some(state);
        Ok(io)
    }

    /// Flush the private pages, in one mutation of the state of [`Self::mount`].
    pub(crate) async fn publish(&self) -> io::Result<crate::state::FlushReport> {
        let state = self.state.lock().unwrap().clone();
        let state = state.ok_or_else(|| invalid("not mounted"))?;
        let mut mutation = state.mutation();
        self.flush_into(&mut mutation)?;
        let report = mutation.flush().await.map_err(io::Error::other)?;
        self.settle_flushed();
        Ok(report)
    }

    pub(crate) fn arm_fault(
        &self,
        at: usize,
        after: bool,
        cancel: bool,
    ) -> futures::channel::oneshot::Receiver<()> {
        let (tx, rx) = futures::channel::oneshot::channel();
        *self.fault.lock().unwrap() = Some(Fault {
            at,
            seen: 0,
            after,
            cancel,
            reached: Some(tx),
        });
        rx
    }

    pub(crate) fn fault_ops(&self) -> usize {
        self.fault.lock().unwrap().as_ref().unwrap().seen
    }

    pub(super) fn fault_operation(&self) -> usize {
        let mut guard = self.fault.lock().unwrap();
        match guard.as_mut() {
            Some(fault) => {
                fault.seen += 1;
                fault.seen
            }
            None => 0,
        }
    }

    pub(super) async fn fault_point(&self, operation: usize, after: bool) -> io::Result<()> {
        let action = {
            let mut guard = self.fault.lock().unwrap();
            match guard.as_mut() {
                Some(fault) => {
                    if operation == fault.at && fault.after == after {
                        Some((fault.cancel, fault.reached.take()))
                    } else {
                        None
                    }
                }
                None => None,
            }
        };
        if let Some((cancel, signal)) = action {
            if let Some(signal) = signal {
                let _ = signal.send(());
            }
            if cancel {
                futures::future::pending::<()>().await;
            }
            return Err(io::Error::other("injected checkpoint I/O failure"));
        }
        Ok(())
    }
}
