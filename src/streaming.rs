//! Persistent mutex-free HackRF bulk-IN streaming.

use std::future::Future;
use std::ops::Deref;
use std::task::{Context, Poll};
#[cfg(not(target_arch = "wasm32"))]
use std::time::{Duration, Instant};

use crate::Complex32;
use crate::constants::{TRANSFER_COUNT, TRANSFER_SIZE};
use crate::errors::{Error, Result};

/// Counters collected during a receive-stream lifetime.
///
/// HackRF sample transfers have no sequence number, so these counters cannot
/// detect samples lost inside the device before a USB completion reaches the
/// host.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StreamingStats {
    /// USB completions consumed by the driver.
    pub buffers_received: u64,
    /// Successful completions accepted for sample conversion.
    pub buffers_processed: u64,
    /// Completions discarded because of errors or controlled restart.
    pub buffers_dropped: u64,
    /// Old submissions drained before exposing samples after a restart.
    pub buffers_discarded_on_restart: u64,
}

impl StreamingStats {
    pub(crate) fn accumulate(&mut self, other: Self) {
        self.buffers_received += other.buffers_received;
        self.buffers_processed += other.buffers_processed;
        self.buffers_dropped += other.buffers_dropped;
        self.buffers_discarded_on_restart += other.buffers_discarded_on_restart;
    }

    pub(crate) fn combined(mut self, other: Self) -> Self {
        self.accumulate(other);
        self
    }
}

#[derive(Debug)]
pub(crate) struct BulkInCompletion<B> {
    pub(crate) buffer: B,
    pub(crate) actual_len: usize,
    pub(crate) status: Result<()>,
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) trait BulkInBackend: std::fmt::Debug {
    type Buffer: Deref<Target = [u8]>;

    fn clear_halt(&mut self) -> Result<()>;
    fn allocate(&self, len: usize) -> Self::Buffer;
    fn submit(&mut self, buffer: Self::Buffer);
    fn pending(&self) -> usize;
    fn wait_next_complete(&mut self, timeout: Duration) -> Option<BulkInCompletion<Self::Buffer>>;
    fn cancel_all(&mut self);
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) trait StreamingBackend: std::fmt::Debug {
    type BulkIn: BulkInBackend;

    fn bulk_in(&self, endpoint: u8) -> Result<Self::BulkIn>;
}

pub(crate) trait AsyncBulkInBackend: std::fmt::Debug {
    type Buffer: Deref<Target = [u8]>;

    fn clear_halt_async(&mut self) -> impl Future<Output = Result<()>> + '_;
    fn allocate(&self, len: usize) -> Self::Buffer;
    fn submit(&mut self, buffer: Self::Buffer);
    fn pending(&self) -> usize;
    fn poll_next_complete(&mut self, cx: &mut Context<'_>) -> Poll<BulkInCompletion<Self::Buffer>>;
    fn cancel_all(&mut self);
}

pub(crate) trait AsyncStreamingBackend: std::fmt::Debug {
    type BulkIn: AsyncBulkInBackend;

    fn bulk_in(&self, endpoint: u8) -> Result<Self::BulkIn>;
}

#[derive(Debug)]
pub(crate) struct PreparedBulkIn<B, T> {
    bulk_in: B,
    buffers: Vec<T>,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub(crate) struct DirectRxStream<B: BulkInBackend> {
    bulk_in: Option<B>,
    current: Option<B::Buffer>,
    current_len: usize,
    current_offset: usize,
    pending_i: Option<i8>,
    stats: StreamingStats,
    closed: bool,
}

#[derive(Debug)]
pub(crate) struct AsyncDirectRxStream<B: AsyncBulkInBackend> {
    bulk_in: Option<B>,
    current: Option<B::Buffer>,
    current_len: usize,
    current_offset: usize,
    pending_i: Option<i8>,
    discard_remaining: usize,
    stats: StreamingStats,
    closed: bool,
}

#[cfg(not(target_arch = "wasm32"))]
impl<B: BulkInBackend> DirectRxStream<B> {
    pub(crate) fn prepare(bulk_in: B) -> PreparedBulkIn<B, B::Buffer> {
        let missing = TRANSFER_COUNT.saturating_sub(bulk_in.pending());
        let buffers = (0..missing)
            .map(|_| bulk_in.allocate(TRANSFER_SIZE))
            .collect();
        PreparedBulkIn { bulk_in, buffers }
    }

    pub(crate) fn read_complex(
        &mut self,
        out: &mut [Complex32],
        timeout: Duration,
    ) -> Result<usize> {
        if self.closed {
            return Err(Error::stream_closed("RX stream is closed"));
        }
        if out.is_empty() {
            return Ok(0);
        }

        let deadline = (timeout != Duration::MAX).then(|| Instant::now() + timeout);
        let mut written = 0;
        loop {
            written += self.drain_current(&mut out[written..])?;
            if written == out.len() {
                self.recycle_current_if_consumed()?;
                return Ok(written);
            }

            self.recycle_current_if_consumed()?;
            let wait = remaining_timeout(deadline, timeout);
            if wait.is_zero() {
                return Ok(written);
            }
            let completion = match self.bulk_in_mut()?.wait_next_complete(wait) {
                Some(completion) => completion,
                None => return Ok(written),
            };
            self.accept_completion(completion)?;
        }
    }

    pub(crate) fn close(&mut self) -> StreamingStats {
        if !self.closed {
            if let Some(bulk_in) = self.bulk_in.as_mut() {
                bulk_in.cancel_all();
            }
            self.bulk_in = None;
            self.current = None;
            self.current_len = 0;
            self.current_offset = 0;
            self.pending_i = None;
            self.closed = true;
        }
        self.stats
    }

    fn accept_completion(&mut self, completion: BulkInCompletion<B::Buffer>) -> Result<()> {
        self.stats.buffers_received += 1;
        let (buffer, actual_len) = match checked_completion(completion) {
            Ok(value) => value,
            Err(error) => {
                self.stats.buffers_dropped += 1;
                self.close();
                return Err(error);
            }
        };
        self.stats.buffers_processed += 1;
        self.current = Some(buffer);
        self.current_len = actual_len;
        self.current_offset = 0;
        Ok(())
    }

    fn drain_current(&mut self, out: &mut [Complex32]) -> Result<usize> {
        let Some(buffer) = self.current.as_ref() else {
            return Ok(0);
        };
        Ok(convert_cs8(
            &buffer[self.current_offset..self.current_len],
            out,
            &mut self.current_offset,
            &mut self.pending_i,
        ))
    }

    fn recycle_current_if_consumed(&mut self) -> Result<()> {
        if self.current.is_some() && self.current_offset == self.current_len {
            let buffer = self.current.take().expect("current buffer checked");
            self.current_len = 0;
            self.current_offset = 0;
            self.bulk_in_mut()?.submit(buffer);
        }
        Ok(())
    }

    fn bulk_in_mut(&mut self) -> Result<&mut B> {
        self.bulk_in
            .as_mut()
            .ok_or(Error::stream_closed("RX stream is closed"))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<B: BulkInBackend> Drop for DirectRxStream<B> {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl<B: AsyncBulkInBackend> AsyncDirectRxStream<B> {
    pub(crate) fn prepare(bulk_in: B) -> PreparedBulkIn<B, B::Buffer> {
        let missing = TRANSFER_COUNT.saturating_sub(bulk_in.pending());
        let buffers = (0..missing)
            .map(|_| bulk_in.allocate(TRANSFER_SIZE))
            .collect();
        PreparedBulkIn { bulk_in, buffers }
    }

    pub(crate) fn poll_read_complex(
        &mut self,
        out: &mut [Complex32],
        cx: &mut Context<'_>,
    ) -> Poll<Result<usize>> {
        if self.closed {
            return Poll::Ready(Err(Error::stream_closed("async RX stream is closed")));
        }
        if out.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let written = match self.drain_current(out) {
            Ok(written) => written,
            Err(error) => return Poll::Ready(Err(error)),
        };
        if written != 0 {
            if let Err(error) = self.recycle_current_if_consumed() {
                return Poll::Ready(Err(error));
            }
            return Poll::Ready(Ok(written));
        }
        if let Err(error) = self.recycle_current_if_consumed() {
            return Poll::Ready(Err(error));
        }

        loop {
            let completion = {
                let bulk_in = match self.bulk_in_mut() {
                    Ok(bulk_in) => bulk_in,
                    Err(error) => return Poll::Ready(Err(error)),
                };
                match bulk_in.poll_next_complete(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(completion) => completion,
                }
            };

            if self.discard_remaining != 0 {
                self.discard_remaining -= 1;
                self.stats.buffers_received += 1;
                self.stats.buffers_dropped += 1;
                self.stats.buffers_discarded_on_restart += 1;
                let buffer = completion.buffer;
                self.bulk_in_mut()
                    .expect("bulk endpoint exists")
                    .submit(buffer);
                continue;
            }

            self.stats.buffers_received += 1;
            let (buffer, actual_len) = match checked_completion(completion) {
                Ok(value) => value,
                Err(error) => {
                    self.stats.buffers_dropped += 1;
                    self.close();
                    return Poll::Ready(Err(error));
                }
            };
            self.stats.buffers_processed += 1;
            self.current = Some(buffer);
            self.current_len = actual_len;
            self.current_offset = 0;

            let written = match self.drain_current(out) {
                Ok(written) => written,
                Err(error) => return Poll::Ready(Err(error)),
            };
            if let Err(error) = self.recycle_current_if_consumed() {
                return Poll::Ready(Err(error));
            }
            if written != 0 {
                return Poll::Ready(Ok(written));
            }
        }
    }

    pub(crate) fn pause(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        if let Some(buffer) = self.current.take() {
            self.current_len = 0;
            self.current_offset = 0;
            self.bulk_in_mut()?.submit(buffer);
        }
        self.pending_i = None;
        let pending = self.bulk_in_mut()?.pending();
        self.discard_remaining = pending;
        self.bulk_in_mut()?.cancel_all();
        Ok(())
    }

    pub(crate) fn stats(&self) -> StreamingStats {
        self.stats
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed
    }

    pub(crate) fn close(&mut self) -> StreamingStats {
        if !self.closed {
            if let Some(bulk_in) = self.bulk_in.as_mut() {
                bulk_in.cancel_all();
            }
            self.bulk_in = None;
            self.current = None;
            self.current_len = 0;
            self.current_offset = 0;
            self.pending_i = None;
            self.closed = true;
        }
        self.stats
    }

    fn drain_current(&mut self, out: &mut [Complex32]) -> Result<usize> {
        let Some(buffer) = self.current.as_ref() else {
            return Ok(0);
        };
        Ok(convert_cs8(
            &buffer[self.current_offset..self.current_len],
            out,
            &mut self.current_offset,
            &mut self.pending_i,
        ))
    }

    fn recycle_current_if_consumed(&mut self) -> Result<()> {
        if self.current.is_some() && self.current_offset == self.current_len {
            let buffer = self.current.take().expect("current buffer checked");
            self.current_len = 0;
            self.current_offset = 0;
            self.bulk_in_mut()?.submit(buffer);
        }
        Ok(())
    }

    fn bulk_in_mut(&mut self) -> Result<&mut B> {
        self.bulk_in
            .as_mut()
            .ok_or(Error::stream_closed("async RX stream is closed"))
    }
}

impl<B: AsyncBulkInBackend> Drop for AsyncDirectRxStream<B> {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<B: BulkInBackend> PreparedBulkIn<B, B::Buffer> {
    pub(crate) fn start_blocking(mut self) -> Result<DirectRxStream<B>> {
        self.bulk_in.clear_halt()?;
        for buffer in self.buffers {
            self.bulk_in.submit(buffer);
        }
        Ok(DirectRxStream {
            bulk_in: Some(self.bulk_in),
            current: None,
            current_len: 0,
            current_offset: 0,
            pending_i: None,
            stats: StreamingStats::default(),
            closed: false,
        })
    }
}

impl<B: AsyncBulkInBackend> PreparedBulkIn<B, B::Buffer> {
    pub(crate) async fn start_async(mut self) -> Result<AsyncDirectRxStream<B>> {
        self.bulk_in.clear_halt_async().await?;
        for buffer in self.buffers {
            self.bulk_in.submit(buffer);
        }
        Ok(AsyncDirectRxStream {
            bulk_in: Some(self.bulk_in),
            current: None,
            current_len: 0,
            current_offset: 0,
            pending_i: None,
            discard_remaining: 0,
            stats: StreamingStats::default(),
            closed: false,
        })
    }
}

fn checked_completion<B: Deref<Target = [u8]>>(
    completion: BulkInCompletion<B>,
) -> Result<(B, usize)> {
    completion.status?;
    if completion.actual_len > completion.buffer.len() || completion.actual_len > TRANSFER_SIZE {
        return Err(Error::protocol(
            "receive HackRF USB transfer",
            "completion length exceeds the submitted buffer",
        ));
    }
    Ok((completion.buffer, completion.actual_len))
}

fn convert_cs8(
    bytes: &[u8],
    out: &mut [Complex32],
    absolute_offset: &mut usize,
    pending_i: &mut Option<i8>,
) -> usize {
    let mut input = 0;
    let mut written = 0;

    if let Some(i) = pending_i.take() {
        if let Some(&q) = bytes.first() {
            out[0] = Complex32::new(i as f32 / 128.0, (q as i8) as f32 / 128.0);
            input = 1;
            written = 1;
        } else {
            *pending_i = Some(i);
            return 0;
        }
    }

    while written < out.len() && input + 1 < bytes.len() {
        let i = bytes[input] as i8;
        let q = bytes[input + 1] as i8;
        out[written] = Complex32::new(i as f32 / 128.0, q as f32 / 128.0);
        input += 2;
        written += 1;
    }

    if input < bytes.len() && written < out.len() {
        *pending_i = Some(bytes[input] as i8);
        input += 1;
    }
    *absolute_offset += input;
    written
}

#[cfg(not(target_arch = "wasm32"))]
fn remaining_timeout(deadline: Option<Instant>, fallback: Duration) -> Duration {
    deadline.map_or(fallback, |deadline| {
        deadline.saturating_duration_since(Instant::now())
    })
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Debug, Default)]
    struct FakeState {
        allocations: usize,
        clear_halt_calls: usize,
        cancel_calls: usize,
        pending: VecDeque<Vec<u8>>,
        ready: VecDeque<Vec<u8>>,
    }

    #[derive(Clone, Debug, Default)]
    struct FakeBackend {
        state: Arc<Mutex<FakeState>>,
    }

    impl FakeBackend {
        fn push_completion(&self, bytes: impl Into<Vec<u8>>) {
            self.state.lock().unwrap().ready.push_back(bytes.into());
        }

        fn complete(&mut self) -> Option<BulkInCompletion<Vec<u8>>> {
            let mut state = self.state.lock().unwrap();
            let data = state.ready.pop_front()?;
            let mut buffer = state.pending.pop_front().expect("submitted test buffer");
            buffer[..data.len()].copy_from_slice(&data);
            Some(BulkInCompletion {
                buffer,
                actual_len: data.len(),
                status: Ok(()),
            })
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    impl BulkInBackend for FakeBackend {
        type Buffer = Vec<u8>;

        fn clear_halt(&mut self) -> Result<()> {
            self.state.lock().unwrap().clear_halt_calls += 1;
            Ok(())
        }

        fn allocate(&self, len: usize) -> Self::Buffer {
            self.state.lock().unwrap().allocations += 1;
            vec![0; len]
        }

        fn submit(&mut self, buffer: Self::Buffer) {
            self.state.lock().unwrap().pending.push_back(buffer);
        }

        fn pending(&self) -> usize {
            self.state.lock().unwrap().pending.len()
        }

        fn wait_next_complete(
            &mut self,
            _timeout: Duration,
        ) -> Option<BulkInCompletion<Self::Buffer>> {
            self.complete()
        }

        fn cancel_all(&mut self) {
            self.state.lock().unwrap().cancel_calls += 1;
        }
    }

    impl AsyncBulkInBackend for FakeBackend {
        type Buffer = Vec<u8>;

        async fn clear_halt_async(&mut self) -> Result<()> {
            self.state.lock().unwrap().clear_halt_calls += 1;
            Ok(())
        }

        fn allocate(&self, len: usize) -> Self::Buffer {
            self.state.lock().unwrap().allocations += 1;
            vec![0; len]
        }

        fn submit(&mut self, buffer: Self::Buffer) {
            self.state.lock().unwrap().pending.push_back(buffer);
        }

        fn pending(&self) -> usize {
            self.state.lock().unwrap().pending.len()
        }

        fn poll_next_complete(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<BulkInCompletion<Self::Buffer>> {
            self.complete().map_or(Poll::Pending, Poll::Ready)
        }

        fn cancel_all(&mut self) {
            self.state.lock().unwrap().cancel_calls += 1;
        }
    }

    #[test]
    fn cs8_conversion_is_signed_and_normalized() {
        let bytes = [0x80, 0x00, 0x7f, 0xff];
        let mut out = [Complex32::default(); 2];
        let mut offset = 0;
        let mut pending = None;
        assert_eq!(convert_cs8(&bytes, &mut out, &mut offset, &mut pending), 2);
        assert_eq!(out[0], Complex32::new(-1.0, 0.0));
        assert_eq!(out[1], Complex32::new(127.0 / 128.0, -1.0 / 128.0));
        assert_eq!(offset, 4);
        assert_eq!(pending, None);
    }

    #[test]
    fn cs8_conversion_carries_i_across_buffers() {
        let mut out = [Complex32::default(); 1];
        let mut offset = 0;
        let mut pending = None;
        assert_eq!(convert_cs8(&[64], &mut out, &mut offset, &mut pending), 0);
        assert_eq!(pending, Some(64));
        let mut next_offset = 0;
        assert_eq!(
            convert_cs8(&[192], &mut out, &mut next_offset, &mut pending),
            1
        );
        assert_eq!(out[0], Complex32::new(0.5, -0.5));
        assert_eq!(pending, None);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn blocking_stream_queues_sixteen_buffers_and_resubmits_consumed_data() {
        let backend = FakeBackend::default();
        let state = Arc::clone(&backend.state);
        backend.push_completion([0x80, 0x00, 0x7f, 0xff]);
        let mut stream = DirectRxStream::prepare(backend).start_blocking().unwrap();

        {
            let state = state.lock().unwrap();
            assert_eq!(state.allocations, TRANSFER_COUNT);
            assert_eq!(state.pending.len(), TRANSFER_COUNT);
            assert_eq!(state.clear_halt_calls, 1);
        }

        let mut out = [Complex32::default(); 2];
        assert_eq!(stream.read_complex(&mut out, Duration::MAX).unwrap(), 2);
        assert_eq!(out[0], Complex32::new(-1.0, 0.0));
        assert_eq!(out[1], Complex32::new(127.0 / 128.0, -1.0 / 128.0));
        assert_eq!(state.lock().unwrap().pending.len(), TRANSFER_COUNT);
        assert_eq!(stream.stats.buffers_processed, 1);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn blocking_stream_carries_an_odd_i_byte_between_completions() {
        let backend = FakeBackend::default();
        backend.push_completion([64]);
        backend.push_completion([192]);
        let mut stream = DirectRxStream::prepare(backend).start_blocking().unwrap();
        let mut out = [Complex32::default(); 1];
        assert_eq!(stream.read_complex(&mut out, Duration::MAX).unwrap(), 1);
        assert_eq!(out[0], Complex32::new(0.5, -0.5));
        assert_eq!(stream.stats.buffers_processed, 2);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn blocking_timeout_returns_samples_already_converted() {
        let backend = FakeBackend::default();
        backend.push_completion([1, 2]);
        let mut stream = DirectRxStream::prepare(backend).start_blocking().unwrap();
        let mut out = [Complex32::default(); 2];
        assert_eq!(stream.read_complex(&mut out, Duration::ZERO).unwrap(), 0);
        assert_eq!(stream.read_complex(&mut out, Duration::MAX).unwrap(), 1);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn blocking_transfer_error_is_counted_and_closes_the_queue() {
        let backend = FakeBackend::default();
        let state = Arc::clone(&backend.state);
        let mut stream = DirectRxStream::prepare(backend).start_blocking().unwrap();
        let error = stream
            .accept_completion(BulkInCompletion {
                buffer: vec![0; TRANSFER_SIZE],
                actual_len: 0,
                status: Err(Error::from(nusb::transfer::TransferError::Fault)),
            })
            .unwrap_err();

        assert_eq!(error.kind(), crate::ErrorKind::Usb);
        assert!(stream.closed);
        assert_eq!(stream.stats.buffers_received, 1);
        assert_eq!(stream.stats.buffers_dropped, 1);
        assert_eq!(state.lock().unwrap().cancel_calls, 1);
    }

    #[test]
    fn async_restart_discards_every_retained_submission() {
        futures_lite::future::block_on(async {
            let backend = FakeBackend::default();
            let state = Arc::clone(&backend.state);
            let mut stream = AsyncDirectRxStream::prepare(backend.clone())
                .start_async()
                .await
                .unwrap();
            stream.pause().unwrap();
            for _ in 0..TRANSFER_COUNT {
                backend.push_completion([0, 0]);
            }
            backend.push_completion([64, 192]);

            let mut out = [Complex32::default(); 1];
            let count = std::future::poll_fn(|cx| stream.poll_read_complex(&mut out, cx))
                .await
                .unwrap();
            assert_eq!(count, 1);
            assert_eq!(out[0], Complex32::new(0.5, -0.5));
            assert_eq!(
                stream.stats().buffers_discarded_on_restart,
                TRANSFER_COUNT as u64
            );
            assert_eq!(state.lock().unwrap().pending.len(), TRANSFER_COUNT);
        });
    }
}
