//! High-level owned HackRF device and receive stream.

use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use nusb::MaybeFuture;

use crate::Complex32;
use crate::config::{
    Config, ConfigBuilder, validate_frequency, validate_lna_gain, validate_sample_rate,
    validate_vga_gain,
};
use crate::device::{HackRf, shutdown_hardware};
use crate::errors::{Error, Result};
use crate::maybe_future::{MaybeFutureExt, ready};
use crate::streaming::{AsyncDirectRxStream, AsyncStreamingBackend, StreamingStats};
#[cfg(not(target_arch = "wasm32"))]
use crate::streaming::{DirectRxStream, StreamingBackend};
use crate::types::DeviceInfo;
use crate::usb::{ControlBackend, NusbControl};

#[cfg(not(target_arch = "wasm32"))]
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
#[cfg(target_arch = "wasm32")]
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeviceLifecycle {
    Open,
    Closing,
    DropCleanupPending,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReceiverSlot {
    Idle,
    Held,
    Orphaned,
}

#[derive(Debug)]
struct SharedDeviceState {
    device: DeviceLifecycle,
    receiver: ReceiverSlot,
    stream_claimed: bool,
    bias_tee_enabled: bool,
}

impl SharedDeviceState {
    fn new(bias_tee_enabled: bool) -> Self {
        Self {
            device: DeviceLifecycle::Open,
            receiver: ReceiverSlot::Idle,
            stream_claimed: false,
            bias_tee_enabled,
        }
    }
}

type SharedState = Arc<Mutex<SharedDeviceState>>;

fn lock_shared(state: &SharedState) -> MutexGuard<'_, SharedDeviceState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

fn ensure_device_open(state: &SharedState) -> Result<()> {
    if lock_shared(state).device == DeviceLifecycle::Open {
        Ok(())
    } else {
        Err(Error::DeviceClosed)
    }
}

fn desired_bias_tee(state: &SharedState) -> bool {
    lock_shared(state).bias_tee_enabled
}

fn begin_shutdown(state: &SharedState) -> Result<bool> {
    let mut shared = lock_shared(state);
    match shared.device {
        DeviceLifecycle::Closed | DeviceLifecycle::DropCleanupPending => Ok(false),
        DeviceLifecycle::Closing => Ok(true),
        DeviceLifecycle::Open => {
            if shared.receiver == ReceiverSlot::Held {
                return Err(Error::Busy);
            }
            shared.device = DeviceLifecycle::Closing;
            Ok(true)
        }
    }
}

fn complete_shutdown(state: &SharedState, result: Result<()>) -> Result<()> {
    if result.is_ok() {
        let mut shared = lock_shared(state);
        shared.device = DeviceLifecycle::Closed;
        shared.receiver = ReceiverSlot::Idle;
    }
    result
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeviceDropAction {
    None,
    Immediate,
    Deferred,
}

fn begin_device_drop(state: &SharedState) -> DeviceDropAction {
    let mut shared = lock_shared(state);
    match shared.device {
        DeviceLifecycle::Closed => DeviceDropAction::None,
        DeviceLifecycle::DropCleanupPending => DeviceDropAction::Deferred,
        DeviceLifecycle::Open | DeviceLifecycle::Closing => {
            if shared.receiver == ReceiverSlot::Held {
                shared.device = DeviceLifecycle::DropCleanupPending;
                DeviceDropAction::Deferred
            } else {
                shared.device = DeviceLifecycle::Closed;
                shared.receiver = ReceiverSlot::Idle;
                DeviceDropAction::Immediate
            }
        }
    }
}

#[derive(Debug)]
struct DeviceInner<C: ControlBackend = NusbControl> {
    direct: HackRf<C>,
    info: DeviceInfo,
    #[cfg(not(target_arch = "wasm32"))]
    shutdown_on_drop: bool,
}

impl<C: ControlBackend> DeviceInner<C> {
    fn stream_handle(&self) -> Self {
        Self {
            direct: self.direct.stream_handle(),
            info: self.info.clone(),
            #[cfg(not(target_arch = "wasm32"))]
            shutdown_on_drop: false,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<C: ControlBackend> Drop for DeviceInner<C> {
    fn drop(&mut self) {
        if self.shutdown_on_drop {
            let _ = shutdown_hardware(&self.direct).wait();
        }
    }
}

/// High-level owned HackRF device handle.
#[derive(Debug)]
pub struct Device {
    inner: DeviceInner<NusbControl>,
    config: Config,
    shared: SharedState,
}

impl Device {
    /// List visible normal-mode HackRF devices without claiming them.
    pub fn list() -> impl MaybeFuture<Output = Result<Vec<crate::DeviceDescriptor>>> {
        crate::discovery::list_devices()
    }

    /// Start building and opening a HackRF.
    pub fn builder() -> DeviceBuilder {
        DeviceBuilder::default()
    }

    /// Open and configure the first visible HackRF.
    pub fn open() -> impl MaybeFuture<Output = Result<Self>> {
        Self::builder().open()
    }

    /// Open and configure a HackRF by its exact 128-bit serial.
    pub fn open_serial(serial: u128) -> impl MaybeFuture<Output = Result<Self>> {
        Self::builder().serial(serial).open()
    }

    /// Ask a browser user to grant WebUSB access to any supported HackRF.
    ///
    /// This must be called from a browser-window user gesture.
    #[cfg(target_arch = "wasm32")]
    pub async fn request_permission() -> Result<()> {
        Self::builder().request_permission().await
    }

    /// Return metadata collected while opening the device.
    pub fn info(&self) -> &DeviceInfo {
        &self.inner.info
    }

    /// Return the configuration last successfully applied by this driver.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Apply a complete validated RX configuration.
    pub fn configure<'a>(
        &'a mut self,
        config: &'a Config,
    ) -> impl MaybeFuture<Output = Result<()>> + 'a {
        let lifecycle = ensure_device_open(&self.shared);
        let applied = config.clone();
        let active = &mut self.config;
        let shared = Arc::clone(&self.shared);
        let operation = self.inner.direct.configure(config);
        ready(lifecycle)
            .and_then(move |()| operation)
            .map(move |result| {
                if result.is_ok() {
                    *active = applied;
                    lock_shared(&shared).bias_tee_enabled = active.bias_tee_enabled();
                }
                result
            })
    }

    /// Change the center frequency while stopped or receiving.
    pub fn set_frequency_hz(
        &mut self,
        frequency_hz: u64,
    ) -> impl MaybeFuture<Output = Result<()>> + '_ {
        let validation =
            ensure_device_open(&self.shared).and_then(|()| validate_frequency(frequency_hz));
        let config = &mut self.config;
        let operation = self.inner.direct.set_frequency(frequency_hz);
        ready(validation)
            .and_then(move |()| operation)
            .map(move |result| {
                if result.is_ok() {
                    config.set_frequency_hz_internal(frequency_hz);
                }
                result
            })
    }

    /// Change the complex sample rate while stopped or receiving.
    ///
    /// The driver also requests a baseband filter bandwidth equal to this rate.
    pub fn set_sample_rate_hz(
        &mut self,
        sample_rate_hz: u32,
    ) -> impl MaybeFuture<Output = Result<()>> + '_ {
        let validation =
            ensure_device_open(&self.shared).and_then(|()| validate_sample_rate(sample_rate_hz));
        let config = &mut self.config;
        let operation = self.inner.direct.set_sample_rate(sample_rate_hz);
        ready(validation)
            .and_then(move |()| operation)
            .map(move |result| {
                if result.is_ok() {
                    config.set_sample_rate_hz_internal(sample_rate_hz);
                }
                result
            })
    }

    /// Set RX IF/LNA gain in dB.
    pub fn set_lna_gain_db(&mut self, gain_db: u8) -> impl MaybeFuture<Output = Result<()>> + '_ {
        let validation = ensure_device_open(&self.shared).and_then(|()| validate_lna_gain(gain_db));
        let config = &mut self.config;
        let operation = self.inner.direct.set_lna_gain(gain_db);
        ready(validation)
            .and_then(move |()| operation)
            .map(move |result| {
                if result.is_ok() {
                    config.set_lna_gain_db_internal(gain_db);
                }
                result
            })
    }

    /// Set baseband/VGA gain in dB.
    pub fn set_vga_gain_db(&mut self, gain_db: u8) -> impl MaybeFuture<Output = Result<()>> + '_ {
        let validation = ensure_device_open(&self.shared).and_then(|()| validate_vga_gain(gain_db));
        let config = &mut self.config;
        let operation = self.inner.direct.set_vga_gain(gain_db);
        ready(validation)
            .and_then(move |()| operation)
            .map(move |result| {
                if result.is_ok() {
                    config.set_vga_gain_db_internal(gain_db);
                }
                result
            })
    }

    /// Enable or disable the RF amplifier.
    pub fn set_amp_enable(&mut self, enabled: bool) -> impl MaybeFuture<Output = Result<()>> + '_ {
        let lifecycle = ensure_device_open(&self.shared);
        let config = &mut self.config;
        let operation = self.inner.direct.set_amp(enabled);
        ready(lifecycle)
            .and_then(move |()| operation)
            .map(move |result| {
                if result.is_ok() {
                    config.set_amp_enabled_internal(enabled);
                }
                result
            })
    }

    /// Enable or disable antenna-port bias power.
    ///
    /// HackRF firmware disables this when reception stops. A later stream start
    /// automatically reapplies the last successful setting.
    pub fn set_bias_tee(&mut self, enabled: bool) -> impl MaybeFuture<Output = Result<()>> + '_ {
        let lifecycle = ensure_device_open(&self.shared);
        let config = &mut self.config;
        let shared = Arc::clone(&self.shared);
        let operation = self.inner.direct.set_bias_tee(enabled);
        ready(lifecycle)
            .and_then(move |()| operation)
            .map(move |result| {
                if result.is_ok() {
                    config.set_bias_tee_enabled_internal(enabled);
                    lock_shared(&shared).bias_tee_enabled = enabled;
                }
                result
            })
    }

    /// Claim the device's single owned receive stream.
    ///
    /// The stream starts dormant. The device remains usable for live setters
    /// while reception is active. Dropping the stream releases the claim.
    pub fn rx_stream(&self) -> Result<RxStream> {
        let claim = RxStreamClaim::acquire(&self.shared)?;
        Ok(RxStream::new(
            self.inner.stream_handle(),
            Arc::clone(&self.shared),
            claim,
        ))
    }

    /// Put the transceiver in off mode and close the logical device session.
    ///
    /// Returns [`Error::Busy`] while a receive stream is running. A stopped but
    /// still-owned stream does not prevent shutdown, but cannot be restarted.
    #[must_use = "shutdown must be waited or awaited to observe hardware cleanup"]
    pub fn shutdown(&mut self) -> impl MaybeFuture<Output = Result<()>> + '_ {
        let decision = begin_shutdown(&self.shared);
        let shared = Arc::clone(&self.shared);
        let operation = shutdown_hardware(&self.inner.direct);
        ready(decision).and_then(move |run| {
            if run {
                crate::maybe_future::Either::left(
                    operation.map(move |result| complete_shutdown(&shared, result)),
                )
            } else {
                crate::maybe_future::Either::right(ready(Ok(())))
            }
        })
    }
}

impl Drop for Device {
    fn drop(&mut self) {
        let action = begin_device_drop(&self.shared);
        #[cfg(not(target_arch = "wasm32"))]
        if action != DeviceDropAction::Immediate {
            self.inner.shutdown_on_drop = false;
        }
        #[cfg(target_arch = "wasm32")]
        if action == DeviceDropAction::Immediate {
            let direct = self.inner.direct.stream_handle();
            wasm_bindgen_futures::spawn_local(async move {
                let _ = shutdown_hardware(&direct).await;
            });
        }
    }
}

/// Builder for opening and initially configuring a [`Device`].
#[derive(Clone, Debug, Default)]
pub struct DeviceBuilder {
    serial: Option<u128>,
    config: ConfigBuilder,
}

impl DeviceBuilder {
    /// Select a device by its exact full 128-bit serial.
    pub fn serial(mut self, serial: u128) -> Self {
        self.serial = Some(serial);
        self
    }

    /// Set the initial center frequency.
    pub fn frequency_hz(mut self, value: u64) -> Self {
        self.config = self.config.frequency_hz(value);
        self
    }

    /// Set the initial complex sample rate.
    pub fn sample_rate_hz(mut self, value: u32) -> Self {
        self.config = self.config.sample_rate_hz(value);
        self
    }

    /// Set the initial RX IF/LNA gain.
    pub fn lna_gain_db(mut self, value: u8) -> Self {
        self.config = self.config.lna_gain_db(value);
        self
    }

    /// Set the initial baseband/VGA gain.
    pub fn vga_gain_db(mut self, value: u8) -> Self {
        self.config = self.config.vga_gain_db(value);
        self
    }

    /// Set the initial RF amplifier state.
    pub fn amp_enable(mut self, enabled: bool) -> Self {
        self.config = self.config.amp_enable(enabled);
        self
    }

    /// Set the initial antenna-port bias-power state.
    pub fn bias_tee(mut self, enabled: bool) -> Self {
        self.config = self.config.bias_tee(enabled);
        self
    }

    /// Open the selected device, query metadata, and apply the configuration.
    pub fn open(self) -> impl MaybeFuture<Output = Result<Device>> {
        let config = self.config.build();
        let serial = self.serial;
        ready(config).and_then(move |config| {
            HackRf::open(serial).and_then(move |(direct, usb_api_version)| {
                let info = direct.fetch_device_info(usb_api_version);
                info.and_then(move |info| {
                    let applied = config.clone();
                    direct.configure(&config).map(move |result| {
                        result?;
                        Ok(Device {
                            inner: DeviceInner {
                                direct,
                                info,
                                #[cfg(not(target_arch = "wasm32"))]
                                shutdown_on_drop: true,
                            },
                            shared: Arc::new(Mutex::new(SharedDeviceState::new(
                                applied.bias_tee_enabled(),
                            ))),
                            config: applied,
                        })
                    })
                })
            })
        })
    }

    /// Ask a browser user to grant WebUSB access to this builder's selector.
    #[cfg(target_arch = "wasm32")]
    pub async fn request_permission(&self) -> Result<()> {
        crate::discovery::request_device_permission(self.serial).await
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReceiverState {
    Stopped,
    CleanupRequired,
    Running,
}

#[cfg(not(target_arch = "wasm32"))]
struct BlockingRxInner<C: ControlBackend + StreamingBackend> {
    device: DeviceInner<C>,
    stream: Option<DirectRxStream<C::BulkIn>>,
    stats: StreamingStats,
    state: ReceiverState,
}

#[cfg(not(target_arch = "wasm32"))]
impl<C> BlockingRxInner<C>
where
    C: ControlBackend + StreamingBackend,
{
    fn new(device: DeviceInner<C>) -> Self {
        Self {
            device,
            stream: None,
            stats: StreamingStats::default(),
            state: ReceiverState::Stopped,
        }
    }

    fn start(&mut self, bias_tee: bool) -> Result<()> {
        match self.state {
            ReceiverState::Running => return Ok(()),
            ReceiverState::CleanupRequired => return Err(Error::Busy),
            ReceiverState::Stopped => {}
        }
        self.state = ReceiverState::CleanupRequired;
        self.stream = Some(self.device.direct.start_rx_blocking(bias_tee)?);
        self.state = ReceiverState::Running;
        Ok(())
    }

    fn read(&mut self, out: &mut [Complex32], timeout: Duration) -> Result<usize> {
        if self.state != ReceiverState::Running {
            return Err(Error::stream_closed("RX stream is stopped"));
        }
        let result = self
            .stream
            .as_mut()
            .ok_or(Error::stream_closed("RX stream has no USB queue"))?
            .read_complex(out, timeout);
        if result.is_err() {
            self.state = ReceiverState::CleanupRequired;
        }
        result
    }

    fn stop(&mut self) -> Result<StreamingStats> {
        if self.state == ReceiverState::Stopped {
            return Ok(self.stats);
        }
        let result = if let Some(stream) = self.stream.take() {
            let (stats, result) = self.device.direct.stop_rx_blocking(stream);
            self.stats.accumulate(stats);
            result
        } else {
            shutdown_hardware(&self.device.direct).wait()
        };
        match result {
            Ok(()) => {
                self.state = ReceiverState::Stopped;
                Ok(self.stats)
            }
            Err(error) => {
                self.state = ReceiverState::CleanupRequired;
                Err(error)
            }
        }
    }
}

struct AsyncRxInner<C: ControlBackend + AsyncStreamingBackend> {
    device: DeviceInner<C>,
    stream: Option<AsyncDirectRxStream<C::BulkIn>>,
    stats: StreamingStats,
    state: ReceiverState,
}

impl<C> AsyncRxInner<C>
where
    C: ControlBackend + AsyncStreamingBackend,
{
    fn new(device: DeviceInner<C>) -> Self {
        Self {
            device,
            stream: None,
            stats: StreamingStats::default(),
            state: ReceiverState::Stopped,
        }
    }

    async fn start(&mut self, bias_tee: bool) -> Result<()> {
        match self.state {
            ReceiverState::Running => return Ok(()),
            ReceiverState::CleanupRequired => return Err(Error::Busy),
            ReceiverState::Stopped => {}
        }
        self.state = ReceiverState::CleanupRequired;
        if self.stream.is_some() {
            self.device.direct.restart_rx_async(bias_tee).await?;
        } else {
            self.stream = Some(self.device.direct.start_rx_async(bias_tee).await?);
        }
        self.state = ReceiverState::Running;
        Ok(())
    }

    fn poll_read(&mut self, out: &mut [Complex32], cx: &mut Context<'_>) -> Poll<Result<usize>> {
        if self.state != ReceiverState::Running {
            return Poll::Ready(Err(Error::stream_closed("async RX stream is stopped")));
        }
        let result = match self.stream.as_mut() {
            Some(stream) => stream.poll_read_complex(out, cx),
            None => Poll::Ready(Err(Error::stream_closed(
                "async RX stream has no USB queue",
            ))),
        };
        if matches!(result, Poll::Ready(Err(_))) {
            self.state = ReceiverState::CleanupRequired;
        }
        result
    }

    async fn stop(&mut self) -> Result<StreamingStats> {
        if self.state == ReceiverState::Stopped {
            return Ok(self.current_stats());
        }
        if let Some(stream) = self.stream.as_mut() {
            self.device.direct.stop_rx_async(stream).await?;
        } else {
            shutdown_hardware(&self.device.direct).await?;
        }
        if self
            .stream
            .as_ref()
            .is_some_and(AsyncDirectRxStream::is_closed)
        {
            let mut stream = self.stream.take().expect("closed stream checked");
            self.stats.accumulate(stream.close());
        }
        self.state = ReceiverState::Stopped;
        Ok(self.current_stats())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn stop_on_drop(&mut self) -> Result<StreamingStats> {
        if self.state == ReceiverState::Stopped {
            return Ok(self.current_stats());
        }
        shutdown_hardware(&self.device.direct).wait()?;
        if let Some(stream) = self.stream.as_mut() {
            stream.pause()?;
        }
        self.state = ReceiverState::Stopped;
        Ok(self.current_stats())
    }

    fn current_stats(&self) -> StreamingStats {
        self.stream
            .as_ref()
            .map_or(self.stats, |stream| self.stats.combined(stream.stats()))
    }
}

enum RxStreamState {
    Dormant(DeviceInner<NusbControl>),
    Poisoned,
    #[cfg(not(target_arch = "wasm32"))]
    Blocking(BlockingRxInner<NusbControl>),
    Async(AsyncRxInner<NusbControl>),
}

#[derive(Debug)]
struct RxStreamClaim {
    shared: SharedState,
}

impl RxStreamClaim {
    fn acquire(shared: &SharedState) -> Result<Self> {
        let mut state = lock_shared(shared);
        if state.device != DeviceLifecycle::Open {
            return Err(Error::DeviceClosed);
        }
        if state.stream_claimed || state.receiver != ReceiverSlot::Idle {
            return Err(Error::Busy);
        }
        state.stream_claimed = true;
        Ok(Self {
            shared: Arc::clone(shared),
        })
    }
}

impl Drop for RxStreamClaim {
    fn drop(&mut self) {
        lock_shared(&self.shared).stream_claimed = false;
    }
}

#[derive(Debug)]
struct ReceiverLease {
    shared: SharedState,
    armed: bool,
}

impl ReceiverLease {
    fn acquire(shared: &SharedState) -> Result<Self> {
        let mut state = lock_shared(shared);
        if state.device != DeviceLifecycle::Open {
            return Err(Error::DeviceClosed);
        }
        if state.receiver != ReceiverSlot::Idle {
            return Err(Error::Busy);
        }
        state.receiver = ReceiverSlot::Held;
        Ok(Self {
            shared: Arc::clone(shared),
            armed: true,
        })
    }

    fn release(mut self) {
        let mut state = lock_shared(&self.shared);
        if state.receiver == ReceiverSlot::Held {
            state.receiver = ReceiverSlot::Idle;
        }
        if state.device == DeviceLifecycle::DropCleanupPending {
            state.device = DeviceLifecycle::Closed;
        }
        self.armed = false;
    }
}

impl Drop for ReceiverLease {
    fn drop(&mut self) {
        if self.armed {
            let mut state = lock_shared(&self.shared);
            if state.receiver == ReceiverSlot::Held {
                state.receiver = ReceiverSlot::Orphaned;
            }
        }
    }
}

/// Owned HackRF receive stream.
///
/// Waiting the first [`RxStream::start`] chooses native blocking USB; awaiting
/// it chooses asynchronous USB. Do not mix the two execution styles on one
/// stream. The steady-state read path owns its endpoint and never takes the
/// device lifecycle mutex.
#[must_use = "RX streams retain the device's exclusive stream claim until dropped"]
pub struct RxStream {
    state: RxStreamState,
    shared: SharedState,
    receiver: Option<ReceiverLease>,
    _claim: RxStreamClaim,
}

impl RxStream {
    fn new(device: DeviceInner<NusbControl>, shared: SharedState, claim: RxStreamClaim) -> Self {
        Self {
            state: RxStreamState::Dormant(device),
            shared,
            receiver: None,
            _claim: claim,
        }
    }

    /// Start reception and the persistent USB transfer queue.
    pub fn start(&mut self) -> impl MaybeFuture<Output = Result<()>> + '_ {
        StartOperation { stream: self }
    }

    /// Read normalized complex IQ samples.
    ///
    /// Blocking operation fills the output until its total timeout expires;
    /// `None` waits indefinitely. Awaited operation ignores the timeout and
    /// consumes buffered data or one new USB completion.
    pub fn read<'a>(
        &'a mut self,
        out: &'a mut [Complex32],
        timeout: Option<Duration>,
    ) -> impl MaybeFuture<Output = Result<usize>> + 'a {
        ReadOperation {
            stream: self,
            out,
            timeout,
        }
    }

    /// Stop reception and return accumulated stream counters.
    pub fn stop(&mut self) -> impl MaybeFuture<Output = Result<StreamingStats>> + '_ {
        StopOperation { stream: self }
    }

    fn receiver_state(&self) -> ReceiverState {
        match &self.state {
            RxStreamState::Dormant(_) => ReceiverState::Stopped,
            RxStreamState::Poisoned => ReceiverState::CleanupRequired,
            #[cfg(not(target_arch = "wasm32"))]
            RxStreamState::Blocking(stream) => stream.state,
            RxStreamState::Async(stream) => stream.state,
        }
    }

    fn begin_start(&mut self) -> Result<bool> {
        ensure_device_open(&self.shared)?;
        match (self.receiver.is_some(), self.receiver_state()) {
            (true, ReceiverState::Running) => Ok(false),
            (true, ReceiverState::Stopped | ReceiverState::CleanupRequired)
            | (false, ReceiverState::Running | ReceiverState::CleanupRequired) => Err(Error::Busy),
            (false, ReceiverState::Stopped) => {
                self.receiver = Some(ReceiverLease::acquire(&self.shared)?);
                Ok(true)
            }
        }
    }

    fn finish_stop(&mut self, result: Result<StreamingStats>) -> Result<StreamingStats> {
        let stats = result?;
        if let Some(receiver) = self.receiver.take() {
            receiver.release();
        }
        Ok(stats)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn initialize_blocking(&mut self) -> Result<()> {
        if matches!(self.state, RxStreamState::Blocking(_)) {
            return Ok(());
        }
        if matches!(self.state, RxStreamState::Async(_)) {
            return Err(Error::Busy);
        }
        let state = core::mem::replace(&mut self.state, RxStreamState::Poisoned);
        match state {
            RxStreamState::Dormant(device) => {
                self.state = RxStreamState::Blocking(BlockingRxInner::new(device));
                Ok(())
            }
            other => {
                self.state = other;
                Err(Error::stream_closed("RX stream has no device"))
            }
        }
    }

    fn initialize_async(&mut self) -> Result<()> {
        if matches!(self.state, RxStreamState::Async(_)) {
            return Ok(());
        }
        #[cfg(not(target_arch = "wasm32"))]
        if matches!(self.state, RxStreamState::Blocking(_)) {
            return Err(Error::Busy);
        }
        let state = core::mem::replace(&mut self.state, RxStreamState::Poisoned);
        match state {
            RxStreamState::Dormant(device) => {
                self.state = RxStreamState::Async(AsyncRxInner::new(device));
                Ok(())
            }
            other => {
                self.state = other;
                Err(Error::stream_closed("RX stream has no device"))
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn start_blocking(&mut self) -> Result<()> {
        self.initialize_blocking()?;
        if !self.begin_start()? {
            return Ok(());
        }
        let bias = desired_bias_tee(&self.shared);
        match &mut self.state {
            RxStreamState::Blocking(stream) => stream.start(bias),
            _ => Err(Error::Busy),
        }
    }

    async fn start_async(&mut self) -> Result<()> {
        self.initialize_async()?;
        if !self.begin_start()? {
            return Ok(());
        }
        let bias = desired_bias_tee(&self.shared);
        match &mut self.state {
            RxStreamState::Async(stream) => stream.start(bias).await,
            _ => Err(Error::Busy),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn read_blocking(&mut self, out: &mut [Complex32], timeout: Duration) -> Result<usize> {
        match &mut self.state {
            RxStreamState::Blocking(stream) => stream.read(out, timeout),
            _ => Err(Error::stream_closed(
                "RX stream is not running synchronously",
            )),
        }
    }

    fn begin_read_async(&mut self) -> Result<()> {
        match &self.state {
            RxStreamState::Async(stream) if stream.state == ReceiverState::Running => Ok(()),
            _ => Err(Error::stream_closed(
                "RX stream is not running asynchronously",
            )),
        }
    }

    fn poll_read_async(
        &mut self,
        out: &mut [Complex32],
        cx: &mut Context<'_>,
    ) -> Poll<Result<usize>> {
        match &mut self.state {
            RxStreamState::Async(stream) => stream.poll_read(out, cx),
            _ => Poll::Ready(Err(Error::stream_closed(
                "RX stream is not running asynchronously",
            ))),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn stop_blocking(&mut self) -> Result<StreamingStats> {
        let result = match &mut self.state {
            RxStreamState::Dormant(_) => Ok(StreamingStats::default()),
            RxStreamState::Blocking(stream) => stream.stop(),
            RxStreamState::Async(_) => Err(Error::Busy),
            RxStreamState::Poisoned => Err(Error::stream_closed("RX stream has no device")),
        };
        self.finish_stop(result)
    }

    async fn stop_async(&mut self) -> Result<StreamingStats> {
        let result = match &mut self.state {
            RxStreamState::Dormant(_) => Ok(StreamingStats::default()),
            RxStreamState::Async(stream) => stream.stop().await,
            #[cfg(not(target_arch = "wasm32"))]
            RxStreamState::Blocking(_) => Err(Error::Busy),
            RxStreamState::Poisoned => Err(Error::stream_closed("RX stream has no device")),
        };
        self.finish_stop(result)
    }

    #[cfg(target_arch = "wasm32")]
    fn control_handle(&self) -> Option<HackRf<NusbControl>> {
        match &self.state {
            RxStreamState::Dormant(device) => Some(device.direct.stream_handle()),
            #[cfg(not(target_arch = "wasm32"))]
            RxStreamState::Blocking(stream) => Some(stream.device.direct.stream_handle()),
            RxStreamState::Async(stream) => Some(stream.device.direct.stream_handle()),
            RxStreamState::Poisoned => None,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn cleanup_on_drop(&mut self) -> Result<()> {
        match &mut self.state {
            RxStreamState::Dormant(_) => Ok(()),
            RxStreamState::Blocking(stream) => stream.stop().map(|_| ()),
            RxStreamState::Async(stream) => stream.stop_on_drop().map(|_| ()),
            RxStreamState::Poisoned => Err(Error::stream_closed("RX stream has no device")),
        }
    }
}

impl Drop for RxStream {
    fn drop(&mut self) {
        #[cfg(not(target_arch = "wasm32"))]
        if self.receiver.is_some()
            && self.cleanup_on_drop().is_ok()
            && let Some(receiver) = self.receiver.take()
        {
            receiver.release();
        }
        #[cfg(target_arch = "wasm32")]
        if let Some(direct) = self.control_handle()
            && let Some(receiver) = self.receiver.take()
        {
            wasm_bindgen_futures::spawn_local(async move {
                if shutdown_hardware(&direct).await.is_ok() {
                    receiver.release();
                }
            });
        }
    }
}

struct StartOperation<'a> {
    stream: &'a mut RxStream,
}

impl<'a> IntoFuture for StartOperation<'a> {
    type Output = Result<()>;
    type IntoFuture = BoxFuture<'a, Result<()>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.stream.start_async().await })
    }
}

impl MaybeFuture for StartOperation<'_> {
    #[cfg(not(target_arch = "wasm32"))]
    fn wait(self) -> Self::Output {
        self.stream.start_blocking()
    }
}

struct StopOperation<'a> {
    stream: &'a mut RxStream,
}

impl<'a> IntoFuture for StopOperation<'a> {
    type Output = Result<StreamingStats>;
    type IntoFuture = BoxFuture<'a, Result<StreamingStats>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.stream.stop_async().await })
    }
}

impl MaybeFuture for StopOperation<'_> {
    #[cfg(not(target_arch = "wasm32"))]
    fn wait(self) -> Self::Output {
        self.stream.stop_blocking()
    }
}

struct ReadOperation<'a> {
    stream: &'a mut RxStream,
    out: &'a mut [Complex32],
    timeout: Option<Duration>,
}

impl<'a> IntoFuture for ReadOperation<'a> {
    type Output = Result<usize>;
    type IntoFuture = ReadFuture<'a>;

    fn into_future(self) -> Self::IntoFuture {
        let _ = self.timeout;
        ReadFuture {
            stream: self.stream,
            out: self.out,
            initialized: false,
            completed: false,
        }
    }
}

impl MaybeFuture for ReadOperation<'_> {
    #[cfg(not(target_arch = "wasm32"))]
    fn wait(self) -> Self::Output {
        self.stream
            .read_blocking(self.out, self.timeout.unwrap_or(Duration::MAX))
    }
}

struct ReadFuture<'a> {
    stream: &'a mut RxStream,
    out: &'a mut [Complex32],
    initialized: bool,
    completed: bool,
}

impl Future for ReadFuture<'_> {
    type Output = Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        assert!(!this.completed, "read future polled after completion");
        if !this.initialized {
            if let Err(error) = this.stream.begin_read_async() {
                this.completed = true;
                return Poll::Ready(Err(error));
            }
            this.initialized = true;
        }
        match this.stream.poll_read_async(this.out, cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                this.completed = true;
                Poll::Ready(result)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shared_state() -> SharedState {
        Arc::new(Mutex::new(SharedDeviceState::new(false)))
    }

    #[test]
    fn stream_claim_is_exclusive_and_released_on_drop() {
        let shared = shared_state();
        let claim = RxStreamClaim::acquire(&shared).unwrap();
        assert_eq!(
            RxStreamClaim::acquire(&shared).unwrap_err().kind(),
            crate::ErrorKind::Busy
        );
        drop(claim);
        assert!(RxStreamClaim::acquire(&shared).is_ok());
    }

    #[test]
    fn shutdown_is_busy_only_while_receiver_lease_is_held() {
        let shared = shared_state();
        let receiver = ReceiverLease::acquire(&shared).unwrap();
        assert_eq!(
            begin_shutdown(&shared).unwrap_err().kind(),
            crate::ErrorKind::Busy
        );
        receiver.release();
        assert!(begin_shutdown(&shared).unwrap());
    }

    #[test]
    fn device_drop_defers_closed_state_to_active_receiver() {
        let shared = shared_state();
        let receiver = ReceiverLease::acquire(&shared).unwrap();
        assert_eq!(begin_device_drop(&shared), DeviceDropAction::Deferred);
        assert_eq!(
            lock_shared(&shared).device,
            DeviceLifecycle::DropCleanupPending
        );
        receiver.release();
        assert_eq!(lock_shared(&shared).device, DeviceLifecycle::Closed);
    }
}
