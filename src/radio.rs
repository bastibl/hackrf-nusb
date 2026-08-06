//! Explicit half-duplex direction control and the TX bulk-OUT transport.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
#[cfg(not(target_arch = "wasm32"))]
use std::time::{Duration, Instant};

use crate::Complex32;
use crate::commands::TransceiverMode;
use crate::constants::{TRANSFER_COUNT, TRANSFER_SIZE};
use crate::device::HackRf;
use crate::errors::{Error, Result};
use crate::usb::{NusbBulkOut, NusbControl};
#[cfg(not(target_arch = "wasm32"))]
use nusb::MaybeFuture;

/// The physical direction selected by a stream operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Direction {
    /// Receive from the radio.
    Rx,
    /// Transmit to the radio.
    Tx,
}

impl Direction {
    fn transceiver_mode(self) -> TransceiverMode {
        match self {
            Self::Rx => TransceiverMode::Receive,
            Self::Tx => TransceiverMode::Transmit,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeviceState {
    Open,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Off,
    Starting(Direction),
    Active(Direction),
    Stopping(Direction),
    Recovery(Direction),
}

#[derive(Debug)]
struct ControllerState {
    device: DeviceState,
    phase: Phase,
    control_in_progress: bool,
    style: Option<ExecutionStyle>,
    rx_stream_claimed: bool,
    tx_stream_claimed: bool,
    // WebUSB cannot cancel the browser's pending bulk-IN promises. Once the
    // queue-owning RX stream is dropped, reopening a queue on the same USB
    // device would leave two independent queues behind.
    #[cfg(target_arch = "wasm32")]
    rx_queue_abandoned: bool,
}

impl Default for ControllerState {
    fn default() -> Self {
        Self {
            device: DeviceState::Open,
            phase: Phase::Off,
            control_in_progress: false,
            style: None,
            rx_stream_claimed: false,
            tx_stream_claimed: false,
            #[cfg(target_arch = "wasm32")]
            rx_queue_abandoned: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExecutionStyle {
    #[cfg(not(target_arch = "wasm32"))]
    Blocking,
    Async,
}

/// Counters collected during one TX `start`/`stop` period.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TxStreamingStats {
    /// Complex application samples accepted by the driver.
    pub samples_accepted: u64,
    /// Bulk-OUT transfers submitted to the USB backend.
    pub buffers_submitted: u64,
    /// Bulk-OUT transfers completed successfully.
    pub buffers_completed: u64,
    /// Zero-filled terminal flush transfers submitted.
    ///
    /// A flush is submitted explicitly with `end_burst` or automatically by
    /// `TxStream::stop` when accepted samples still need a boundary.
    pub flush_buffers: u64,
}

#[derive(Debug, Default)]
struct TxTail {
    flush_required: bool,
    terminal_flush_pending: bool,
}

impl TxTail {
    fn submitted_samples(&mut self) {
        self.flush_required = true;
    }

    fn submitted_flush(&mut self) {
        self.flush_required = false;
        self.terminal_flush_pending = false;
    }

    fn flush_required(&self) -> bool {
        self.flush_required
    }

    fn require_terminal_flush(&mut self) {
        self.flush_required = true;
        self.terminal_flush_pending = true;
    }

    fn terminal_flush_pending(&self) -> bool {
        self.terminal_flush_pending
    }
}

/// The TX endpoint and its in-flight transfers.
///
/// This lives in `TxStream` between successful `start` and `stop` calls, so
/// the TX write path has no controller lock.
#[derive(Debug)]
pub(crate) struct TxTransport {
    endpoint: NusbBulkOut,
    flush_size: usize,
    stats: TxStreamingStats,
    tail: TxTail,
    unusable: bool,
}

impl TxTransport {
    fn new(endpoint: NusbBulkOut, flush_size: usize) -> Self {
        Self {
            endpoint,
            flush_size,
            stats: TxStreamingStats::default(),
            tail: TxTail::default(),
            unusable: false,
        }
    }

    fn reset(&mut self) {
        self.stats = TxStreamingStats::default();
        self.tail = TxTail::default();
    }

    pub(crate) fn stats(&self) -> TxStreamingStats {
        self.stats
    }

    /// A drain failure leaves outstanding endpoint transfers with no safe
    /// cancellation path. Such a transport must never be reused.
    pub(crate) fn mark_unusable(&mut self) {
        self.unusable = true;
    }

    pub(crate) fn is_reusable(&self) -> bool {
        !self.unusable
    }

    fn submit_samples(&mut self, samples: &[Complex32]) {
        let mut bytes = Vec::with_capacity((samples.len() * 2).next_multiple_of(512));
        for sample in samples {
            bytes.push(f32_to_cs8(sample.re) as u8);
            bytes.push(f32_to_cs8(sample.im) as u8);
        }
        bytes.resize(bytes.len().next_multiple_of(512), 0);
        self.endpoint.submit(bytes);
        self.tail.submitted_samples();
        self.stats.samples_accepted += samples.len() as u64;
        self.stats.buffers_submitted += 1;
    }

    fn submit_flush(&mut self) {
        self.endpoint.submit(vec![0; self.flush_size]);
        self.tail.submitted_flush();
        self.stats.buffers_submitted += 1;
        self.stats.flush_buffers += 1;
    }

    fn flush_if_needed(&mut self) {
        if self.tail.flush_required() {
            self.submit_flush();
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn complete_one_blocking(&mut self, timeout: Duration) -> Result<bool> {
        match self.endpoint.wait_next_complete(timeout) {
            Some(result) => {
                result?;
                self.stats.buffers_completed += 1;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    async fn complete_one_async(&mut self) -> Result<()> {
        self.endpoint.next_complete().await?;
        self.stats.buffers_completed += 1;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn clear_halt_blocking(&mut self) -> Result<()> {
        self.endpoint.clear_halt_blocking()
    }

    pub(crate) async fn clear_halt_async(&mut self) -> Result<()> {
        self.endpoint.clear_halt_async().await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn write_blocking(
        &mut self,
        samples: &[Complex32],
        timeout: Duration,
        end_burst: bool,
    ) -> Result<usize> {
        let deadline = (timeout != Duration::MAX).then(|| Instant::now() + timeout);
        let had_pending_terminal_flush = self.tail.terminal_flush_pending();
        if had_pending_terminal_flush {
            while self.endpoint.pending() >= TRANSFER_COUNT {
                if !self.complete_one_blocking(remaining(deadline))? {
                    return Ok(0);
                }
            }
            self.submit_flush();
        }
        if samples.is_empty() && had_pending_terminal_flush {
            return Ok(0);
        }

        let mut accepted = 0;
        for chunk in samples.chunks(TRANSFER_SIZE / 2) {
            while self.endpoint.pending() >= TRANSFER_COUNT {
                if !self.complete_one_blocking(remaining(deadline))? {
                    if end_burst && accepted != 0 {
                        self.tail.require_terminal_flush();
                    }
                    return Ok(accepted);
                }
            }
            self.submit_samples(chunk);
            accepted += chunk.len();
        }
        if end_burst && (accepted != 0 || samples.is_empty()) {
            self.tail.require_terminal_flush();
            while self.endpoint.pending() >= TRANSFER_COUNT {
                if !self.complete_one_blocking(remaining(deadline))? {
                    return Ok(accepted);
                }
            }
            self.submit_flush();
        }
        Ok(accepted)
    }

    pub(crate) async fn write_async(
        &mut self,
        samples: &[Complex32],
        end_burst: bool,
    ) -> Result<usize> {
        let had_pending_terminal_flush = self.tail.terminal_flush_pending();
        if had_pending_terminal_flush {
            while self.endpoint.pending() >= TRANSFER_COUNT {
                self.complete_one_async().await?;
            }
            self.submit_flush();
        }
        if samples.is_empty() && had_pending_terminal_flush {
            return Ok(0);
        }

        let mut accepted = 0;
        for chunk in samples.chunks(TRANSFER_SIZE / 2) {
            while self.endpoint.pending() >= TRANSFER_COUNT {
                self.complete_one_async().await?;
            }
            self.submit_samples(chunk);
            accepted += chunk.len();
            // From this point cancellation must leave an explicit boundary
            // behind: the next write or stop flushes it before accepting
            // anything new. Mark it immediately after the first accepted
            // samples, before the next await can observe cancellation.
            if end_burst && accepted == chunk.len() {
                self.tail.require_terminal_flush();
            }
        }
        if end_burst && (accepted != 0 || samples.is_empty()) {
            // An empty terminal write has no accepted sample after which to
            // record the boundary, so record it before its possible await.
            if samples.is_empty() {
                self.tail.require_terminal_flush();
            }
            while self.endpoint.pending() >= TRANSFER_COUNT {
                self.complete_one_async().await?;
            }
            self.submit_flush();
        }
        Ok(accepted)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn flush_and_drain_blocking(&mut self) -> Result<()> {
        // Keep the terminal flush adjacent to the queued burst. This may
        // temporarily exceed the normal TRANSFER_COUNT write water mark by
        // one transfer before the drain below reclaims completions.
        self.flush_if_needed();
        while self.endpoint.pending() != 0 {
            self.complete_one_blocking(Duration::MAX)?;
        }
        Ok(())
    }

    pub(crate) async fn flush_and_drain_async(&mut self) -> Result<()> {
        // See the blocking counterpart for why the terminal flush is queued
        // before draining, even when the normal water mark is full.
        self.flush_if_needed();
        while self.endpoint.pending() != 0 {
            self.complete_one_async().await?;
        }
        Ok(())
    }
}

fn f32_to_cs8(value: f32) -> i8 {
    if !value.is_finite() {
        0
    } else {
        (value * 128.0).round().clamp(-128.0, 127.0) as i8
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn remaining(deadline: Option<Instant>) -> Duration {
    deadline.map_or(Duration::MAX, |value| {
        value.saturating_duration_since(Instant::now())
    })
}

/// Shared slow-path direction controller.
///
/// Its mutex is taken only to claim or finish `start`, `stop`, and
/// configuration operations. The controller deliberately never changes the
/// selected direction on behalf of a read or write.
#[derive(Debug)]
pub(crate) struct LifecycleController {
    direct: HackRf<NusbControl>,
    bias_tee: AtomicBool,
    state: Mutex<ControllerState>,
    transport: Mutex<Option<TxTransport>>,
}

/// Keeps one logical stream object claimed for its lifetime.
#[derive(Debug)]
pub(crate) struct StreamClaim {
    radio: Arc<LifecycleController>,
    direction: Direction,
}

impl Drop for StreamClaim {
    fn drop(&mut self) {
        let mut state = lock(&self.radio.state);
        match self.direction {
            Direction::Rx => state.rx_stream_claimed = false,
            Direction::Tx => state.tx_stream_claimed = false,
        }
    }
}

/// Owns a claimed direction until hardware start commits or the attempt is
/// abandoned. An abandoned operation requires an explicit stream `stop`.
#[derive(Debug)]
pub(crate) struct StartTransition {
    radio: Arc<LifecycleController>,
    direction: Direction,
    complete: bool,
}

impl StartTransition {
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn finish_blocking(&mut self) -> Result<()> {
        self.radio
            .direct
            .set_transceiver_mode(self.direction.transceiver_mode())
            .wait()?;
        self.radio
            .direct
            .set_bias_tee(self.radio.bias_tee.load(Ordering::Acquire))
            .wait()?;
        self.finish_success()
    }

    pub(crate) async fn finish_async(&mut self) -> Result<()> {
        self.radio
            .direct
            .set_transceiver_mode(self.direction.transceiver_mode())
            .await?;
        self.radio
            .direct
            .set_bias_tee(self.radio.bias_tee.load(Ordering::Acquire))
            .await?;
        self.finish_success()
    }

    fn finish_success(&mut self) -> Result<()> {
        let mut state = lock(&self.radio.state);
        if state.phase != Phase::Starting(self.direction) {
            return Err(Error::Busy);
        }
        state.phase = Phase::Active(self.direction);
        self.complete = true;
        Ok(())
    }
}

impl Drop for StartTransition {
    fn drop(&mut self) {
        if !self.complete {
            let mut state = lock(&self.radio.state);
            if state.phase == Phase::Starting(self.direction) {
                state.phase = Phase::Recovery(self.direction);
            }
        }
    }
}

/// Owns a claimed stop/recovery attempt until off mode commits.
#[derive(Debug)]
pub(crate) struct StopTransition {
    radio: Arc<LifecycleController>,
    direction: Direction,
    complete: bool,
}

impl StopTransition {
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn finish_blocking(&mut self) -> Result<()> {
        self.radio
            .direct
            .set_transceiver_mode(TransceiverMode::Off)
            .wait()?;
        self.finish_success()
    }

    pub(crate) async fn finish_async(&mut self) -> Result<()> {
        self.radio
            .direct
            .set_transceiver_mode(TransceiverMode::Off)
            .await?;
        self.finish_success()
    }

    fn finish_success(&mut self) -> Result<()> {
        let mut state = lock(&self.radio.state);
        if state.phase != Phase::Stopping(self.direction) {
            return Err(Error::Busy);
        }
        state.phase = Phase::Off;
        self.complete = true;
        Ok(())
    }
}

impl Drop for StopTransition {
    fn drop(&mut self) {
        if !self.complete {
            let mut state = lock(&self.radio.state);
            if state.phase == Phase::Stopping(self.direction) {
                state.phase = Phase::Recovery(self.direction);
            }
        }
    }
}

/// Holds the TX transport while its initial hardware start is in progress.
#[derive(Debug)]
pub(crate) struct TxStartTransition {
    start: StartTransition,
    transport: Option<TxTransport>,
}

impl TxStartTransition {
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn finish_blocking(&mut self) -> Result<TxTransport> {
        self.transport
            .as_mut()
            .expect("TX start owns transport")
            .clear_halt_blocking()?;
        self.start.finish_blocking()?;
        let transport = self.transport.as_mut().expect("TX start owns transport");
        transport.reset();
        Ok(self.transport.take().expect("TX start owns transport"))
    }

    pub(crate) async fn finish_async(&mut self) -> Result<TxTransport> {
        self.transport
            .as_mut()
            .expect("TX start owns transport")
            .clear_halt_async()
            .await?;
        self.start.finish_async().await?;
        let transport = self.transport.as_mut().expect("TX start owns transport");
        transport.reset();
        Ok(self.transport.take().expect("TX start owns transport"))
    }
}

impl Drop for TxStartTransition {
    fn drop(&mut self) {
        if let Some(transport) = self.transport.take() {
            self.start.radio.put_transport(transport);
        }
    }
}

/// Serializes configuration with start and stop operations.
#[derive(Debug)]
pub(crate) struct ReconfigureGuard {
    radio: Arc<LifecycleController>,
}

impl Drop for ReconfigureGuard {
    fn drop(&mut self) {
        let mut state = lock(&self.radio.state);
        // Configuration does not change stream direction. The driver only
        // caches settings after a successful request, so an error or a
        // cancelled future may simply leave the cache unchanged.
        state.control_in_progress = false;
    }
}

impl LifecycleController {
    #[cfg_attr(target_arch = "wasm32", allow(clippy::arc_with_non_send_sync))]
    pub(crate) fn new(
        direct: HackRf<NusbControl>,
        endpoint: NusbBulkOut,
        flush_size: usize,
        bias_tee: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            direct,
            bias_tee: AtomicBool::new(bias_tee),
            state: Mutex::new(ControllerState::default()),
            transport: Mutex::new(Some(TxTransport::new(endpoint, flush_size))),
        })
    }

    pub(crate) fn set_bias_tee(&self, enabled: bool) {
        self.bias_tee.store(enabled, Ordering::Release);
    }

    pub(crate) fn claim_stream(self: &Arc<Self>, direction: Direction) -> Result<StreamClaim> {
        let mut state = lock(&self.state);
        if state.device != DeviceState::Open {
            return Err(Error::DeviceClosed);
        }
        #[cfg(target_arch = "wasm32")]
        if direction == Direction::Rx && state.rx_queue_abandoned {
            return Err(Error::stream_closed(
                "WebUSB RX queue was dropped; reopen the device before creating another RX stream",
            ));
        }
        let claimed = match direction {
            Direction::Rx => &mut state.rx_stream_claimed,
            Direction::Tx => &mut state.tx_stream_claimed,
        };
        if *claimed {
            return Err(Error::Busy);
        }
        *claimed = true;
        Ok(StreamClaim {
            radio: Arc::clone(self),
            direction,
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn begin_rx_start_blocking(self: &Arc<Self>) -> Result<StartTransition> {
        self.begin_start(Direction::Rx, ExecutionStyle::Blocking)
    }

    pub(crate) async fn begin_rx_start_async(self: &Arc<Self>) -> Result<StartTransition> {
        self.begin_start(Direction::Rx, ExecutionStyle::Async)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn begin_tx_start_blocking(self: &Arc<Self>) -> Result<TxStartTransition> {
        self.begin_tx_start(ExecutionStyle::Blocking)
    }

    pub(crate) async fn begin_tx_start_async(self: &Arc<Self>) -> Result<TxStartTransition> {
        self.begin_tx_start(ExecutionStyle::Async)
    }

    fn begin_tx_start(self: &Arc<Self>, style: ExecutionStyle) -> Result<TxStartTransition> {
        let transport = lock(&self.transport).take().ok_or(Error::stream_closed(
            "TX transport has unresolved transfers; reopen the device",
        ))?;
        let start = match self.begin_start(Direction::Tx, style) {
            Ok(start) => start,
            Err(error) => {
                self.put_transport(transport);
                return Err(error);
            }
        };
        Ok(TxStartTransition {
            start,
            transport: Some(transport),
        })
    }

    fn begin_start(
        self: &Arc<Self>,
        direction: Direction,
        style: ExecutionStyle,
    ) -> Result<StartTransition> {
        let mut state = lock(&self.state);
        if state.device != DeviceState::Open {
            return Err(Error::DeviceClosed);
        }
        if state.control_in_progress || state.phase != Phase::Off {
            return Err(Error::Busy);
        }
        match state.style {
            Some(selected) if selected != style => return Err(Error::Busy),
            Some(_) => {}
            None => state.style = Some(style),
        }
        state.phase = Phase::Starting(direction);
        Ok(StartTransition {
            radio: Arc::clone(self),
            direction,
            complete: false,
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn begin_stop_blocking(
        self: &Arc<Self>,
        direction: Direction,
    ) -> Result<Option<StopTransition>> {
        self.begin_stop(direction, Some(ExecutionStyle::Blocking), false)
    }

    pub(crate) async fn begin_stop_async(
        self: &Arc<Self>,
        direction: Direction,
    ) -> Result<Option<StopTransition>> {
        self.begin_stop(direction, Some(ExecutionStyle::Async), false)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn begin_stop_on_drop_blocking(
        self: &Arc<Self>,
        direction: Direction,
    ) -> Result<Option<StopTransition>> {
        self.begin_stop(direction, None, true)
    }

    fn begin_stop(
        self: &Arc<Self>,
        direction: Direction,
        style: Option<ExecutionStyle>,
        allow_closed_device: bool,
    ) -> Result<Option<StopTransition>> {
        let mut state = lock(&self.state);
        if !allow_closed_device && state.device != DeviceState::Open {
            return Err(Error::DeviceClosed);
        }
        if let Some(style) = style {
            match state.style {
                Some(selected) if selected != style => return Err(Error::Busy),
                Some(_) | None => {}
            }
        }
        match state.phase {
            Phase::Off => Ok(None),
            Phase::Active(active) if active == direction => {
                state.phase = Phase::Stopping(direction);
                Ok(Some(StopTransition {
                    radio: Arc::clone(self),
                    direction,
                    complete: false,
                }))
            }
            Phase::Recovery(owner) if owner == direction => {
                state.phase = Phase::Stopping(direction);
                Ok(Some(StopTransition {
                    radio: Arc::clone(self),
                    direction,
                    complete: false,
                }))
            }
            _ => Err(Error::Busy),
        }
    }

    pub(crate) fn mark_stream_failed(&self, direction: Direction) {
        let mut state = lock(&self.state);
        if state.phase == Phase::Active(direction) {
            state.phase = Phase::Recovery(direction);
        }
    }

    /// WebUSB cannot cancel the browser-owned transfer promises that back an
    /// RX queue. Prevent a replacement stream from creating a second queue
    /// on this device session after the first queue owner has been dropped.
    #[cfg(target_arch = "wasm32")]
    pub(crate) fn mark_rx_queue_abandoned(&self) {
        lock(&self.state).rx_queue_abandoned = true;
    }

    pub(crate) fn begin_reconfigure(
        self: &Arc<Self>,
        style: ExecutionStyle,
    ) -> Result<ReconfigureGuard> {
        let mut state = lock(&self.state);
        if state.device != DeviceState::Open {
            return Err(Error::DeviceClosed);
        }
        if state.control_in_progress || !matches!(state.phase, Phase::Off | Phase::Active(_)) {
            return Err(Error::Busy);
        }
        match state.style {
            Some(selected) if selected != style => return Err(Error::Busy),
            Some(_) | None => {}
        }
        state.control_in_progress = true;
        Ok(ReconfigureGuard {
            radio: Arc::clone(self),
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn shutdown_blocking(&self) -> Result<()> {
        self.begin_device_shutdown()?;
        self.direct
            .set_transceiver_mode(TransceiverMode::Off)
            .wait()
    }

    pub(crate) async fn shutdown_async(&self) -> Result<()> {
        self.begin_device_shutdown()?;
        self.direct.set_transceiver_mode(TransceiverMode::Off).await
    }

    fn begin_device_shutdown(&self) -> Result<()> {
        let mut state = lock(&self.state);
        if state.device == DeviceState::Closed {
            return Ok(());
        }
        if state.control_in_progress || state.phase != Phase::Off {
            return Err(Error::Busy);
        }
        state.device = DeviceState::Closed;
        Ok(())
    }

    pub(crate) fn close_on_drop(&self) {
        lock(&self.state).device = DeviceState::Closed;
    }

    fn put_transport(&self, transport: TxTransport) {
        let previous = lock(&self.transport).replace(transport);
        debug_assert!(previous.is_none());
    }

    pub(crate) fn return_tx_transport(&self, transport: TxTransport) {
        self.put_transport(transport);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tx_tail_requires_one_flush_after_samples() {
        let mut tail = TxTail::default();
        tail.submitted_samples();
        assert!(tail.flush_required());
        tail.require_terminal_flush();
        assert!(tail.terminal_flush_pending());
        tail.submitted_flush();
        assert!(!tail.flush_required());
        assert!(!tail.terminal_flush_pending());
    }

    #[test]
    fn directions_are_distinct() {
        assert_ne!(Direction::Rx, Direction::Tx);
        assert_ne!(Phase::Active(Direction::Rx), Phase::Active(Direction::Tx));
        assert_ne!(
            Phase::Recovery(Direction::Rx),
            Phase::Recovery(Direction::Tx)
        );
    }
}
