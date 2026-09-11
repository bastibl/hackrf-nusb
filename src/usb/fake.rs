//! Deterministic USB backends selected only for unit and browser tests.
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

use nusb::MaybeFuture;

use super::{ControlBackend, VendorControlRequest};
use crate::commands::VendorRequest;
use crate::errors::{Error, Result};
use crate::maybe_future::ready;
use crate::streaming::{AsyncBulkInBackend, AsyncStreamingBackend, BulkInCompletion};
#[cfg(not(target_arch = "wasm32"))]
use crate::streaming::{BulkInBackend, StreamingBackend};

#[derive(Debug, Default)]
pub(crate) struct TestIo {
    pub events: Vec<String>,
    pub pending: usize,
    pub fail_rx: bool,
    pub fail_off: bool,
    pub pause_off: bool,
    pub fail_drain: bool,
    pub pause_drain: bool,
    off_waker: Option<Waker>,
    drain_waker: Option<Waker>,
}

impl TestIo {
    pub(crate) fn resume(io: &Mutex<Self>) {
        let (off, drain) = {
            let mut io = io.lock().unwrap();
            io.pause_off = false;
            io.pause_drain = false;
            (io.off_waker.take(), io.drain_waker.take())
        };
        for waker in [off, drain].into_iter().flatten() {
            waker.wake();
        }
    }

    pub(crate) fn backends() -> (FakeControl, FakeBulkOut, Arc<Mutex<Self>>) {
        let io = Arc::new(Mutex::new(Self::default()));
        (FakeControl(io.clone()), FakeBulkOut(io.clone()), io)
    }
}

#[derive(Debug)]
pub(crate) struct FakeControl(Arc<Mutex<TestIo>>);

impl FakeControl {
    pub(crate) fn new(_: nusb::Device, _: nusb::Interface) -> Self {
        panic!("unit tests must inject USB backends instead of opening hardware")
    }

    pub(crate) fn bulk_out(&self, _: u8) -> Result<FakeBulkOut> {
        Ok(FakeBulkOut(self.0.clone()))
    }
}

impl ControlBackend for FakeControl {
    fn control_in(
        &self,
        _: VendorControlRequest,
    ) -> impl MaybeFuture<Output = Result<Vec<u8>>> + use<> {
        // Gain requests acknowledge acceptance with a single nonzero byte.
        ready(Ok(vec![1]))
    }

    fn control_out(
        &self,
        request: VendorControlRequest,
    ) -> impl MaybeFuture<Output = Result<()>> + use<> {
        TestControlOperation {
            io: self.0.clone(),
            value: request.value,
            off: request.request == VendorRequest::SetTransceiverMode && request.value == 0,
        }
    }
}

struct TestControlOperation {
    io: Arc<Mutex<TestIo>>,
    value: u16,
    off: bool,
}

impl TestControlOperation {
    fn execute(&self) -> Result<()> {
        let mut io = self.io.lock().unwrap();
        io.events.push(format!("control:{}", self.value));
        if self.off && io.fail_off {
            Err(Error::Busy)
        } else {
            Ok(())
        }
    }
}

impl std::future::IntoFuture for TestControlOperation {
    type Output = Result<()>;
    type IntoFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send>>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(std::future::poll_fn(move |cx| {
            if self.off && self.io.lock().unwrap().pause_off {
                self.io.lock().unwrap().off_waker = Some(cx.waker().clone());
                Poll::Pending
            } else {
                Poll::Ready(self.execute())
            }
        }))
    }
}

impl MaybeFuture for TestControlOperation {
    #[cfg(not(target_arch = "wasm32"))]
    fn wait(self) -> Result<()> {
        self.execute()
    }
}

#[derive(Debug)]
pub(crate) struct FakeBulkOut(Arc<Mutex<TestIo>>);

impl FakeBulkOut {
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn clear_halt_blocking(&mut self) -> Result<()> {
        Ok(())
    }
    pub(crate) async fn clear_halt_async(&mut self) -> Result<()> {
        Ok(())
    }
    pub(crate) fn submit(&mut self, bytes: Vec<u8>) {
        let mut io = self.0.lock().unwrap();
        io.events.push(format!("submit:{}", bytes.len()));
        io.pending += 1;
    }
    pub(crate) fn pending(&self) -> usize {
        self.0.lock().unwrap().pending
    }
    fn complete(&mut self) -> Result<()> {
        let mut io = self.0.lock().unwrap();
        io.events.push("complete".into());
        if io.fail_drain {
            return Err(Error::Busy);
        }
        io.pending -= 1;
        Ok(())
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn wait_next_complete(&mut self, _: Duration) -> Option<Result<()>> {
        Some(self.complete())
    }
    pub(crate) async fn next_complete(&mut self) -> Result<()> {
        std::future::poll_fn(|cx| {
            if self.0.lock().unwrap().pause_drain {
                self.0.lock().unwrap().drain_waker = Some(cx.waker().clone());
                Poll::Pending
            } else {
                Poll::Ready(self.complete())
            }
        })
        .await
    }
}

#[derive(Debug)]
pub(crate) struct FakeBulkIn {
    io: Arc<Mutex<TestIo>>,
    buffers: VecDeque<Vec<u8>>,
}

impl FakeBulkIn {
    fn complete(&mut self) -> BulkInCompletion<Vec<u8>> {
        self.buffers.pop_front().unwrap();
        BulkInCompletion {
            buffer: vec![1; 512],
            actual_len: 512,
            status: if self.io.lock().unwrap().fail_rx {
                Err(Error::Busy)
            } else {
                Ok(())
            },
        }
    }

    fn retire(&mut self) {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.io.lock().unwrap().events.push("rx:retire".into());
            self.buffers.clear();
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl StreamingBackend for FakeControl {
    type BulkIn = FakeBulkIn;
    fn bulk_in(&self, endpoint: u8) -> Result<FakeBulkIn> {
        AsyncStreamingBackend::bulk_in(self, endpoint)
    }
}

impl AsyncStreamingBackend for FakeControl {
    type BulkIn = FakeBulkIn;
    fn bulk_in(&self, _: u8) -> Result<FakeBulkIn> {
        Ok(FakeBulkIn {
            io: self.0.clone(),
            buffers: VecDeque::new(),
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl BulkInBackend for FakeBulkIn {
    type Buffer = Vec<u8>;
    fn clear_halt(&mut self) -> Result<()> {
        Ok(())
    }
    fn allocate(&self, len: usize) -> Vec<u8> {
        vec![0; len]
    }
    fn submit(&mut self, buffer: Vec<u8>) {
        self.buffers.push_back(buffer);
    }
    fn pending(&self) -> usize {
        self.buffers.len()
    }
    fn wait_next_complete(&mut self, _: Duration) -> Option<BulkInCompletion<Vec<u8>>> {
        Some(self.complete())
    }
    fn cancel_all(&mut self) {
        self.retire();
    }
}

impl AsyncBulkInBackend for FakeBulkIn {
    type Buffer = Vec<u8>;
    async fn clear_halt_async(&mut self) -> Result<()> {
        Ok(())
    }
    fn allocate(&self, len: usize) -> Vec<u8> {
        vec![0; len]
    }
    fn submit(&mut self, buffer: Vec<u8>) {
        self.buffers.push_back(buffer);
    }
    fn pending(&self) -> usize {
        self.buffers.len()
    }
    fn poll_next_complete(&mut self, _: &mut Context<'_>) -> Poll<BulkInCompletion<Vec<u8>>> {
        Poll::Ready(self.complete())
    }
    fn cancel_all(&mut self) {
        self.retire();
    }
}
