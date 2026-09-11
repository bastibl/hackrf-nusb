//! High-level owned HackRF device and explicit half-duplex streams.

use std::future::{Future, IntoFuture, poll_fn};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use nusb::MaybeFuture;

use crate::Complex32;
use crate::config::{
    Config, ConfigBuilder, validate_frequency, validate_lna_gain, validate_sample_rate,
    validate_tx_vga_gain, validate_vga_gain,
};
use crate::device::{HackRf, shutdown_hardware};
use crate::errors::{Error, Result};
use crate::maybe_future::{MaybeFutureExt, ready};
use crate::radio::{
    Direction, ExecutionStyle, LifecycleController, StreamClaim, TxStreamingStats, TxTransport,
};
use crate::streaming::{AsyncDirectRxStream, AsyncStreamingBackend, StreamingStats};
#[cfg(not(target_arch = "wasm32"))]
use crate::streaming::{DirectRxStream, StreamingBackend};
use crate::types::DeviceInfo;
use crate::usb::{ControlBackend, NusbControl};

#[cfg(not(target_arch = "wasm32"))]
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
#[cfg(target_arch = "wasm32")]
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

#[derive(Debug)]
struct DeviceInner {
    direct: HackRf<NusbControl>,
    info: DeviceInfo,
}

/// Ensures a partially initialized device is returned to off mode.
///
/// It owns a second handle to the same control interface, leaving the primary
/// handle available to the opening operation. A completed open consumes this
/// guard; an error or cancelled future drops it and triggers best-effort
/// cleanup instead.
struct OpeningCleanup {
    direct: Option<HackRf<NusbControl>>,
}

impl OpeningCleanup {
    fn new(direct: HackRf<NusbControl>) -> Self {
        Self {
            direct: Some(direct),
        }
    }

    fn disarm(mut self) {
        self.direct = None;
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for OpeningCleanup {
    fn drop(&mut self) {
        if let Some(direct) = self.direct.as_ref() {
            let _ = shutdown_hardware(direct).wait();
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl Drop for OpeningCleanup {
    fn drop(&mut self) {
        if let Some(direct) = self.direct.take() {
            wasm_bindgen_futures::spawn_local(async move {
                let _ = shutdown_hardware(&direct).await;
            });
        }
    }
}

/// High-level owned HackRF device handle.
///
/// Streams share the hardware lifetime and remain usable after this handle
/// is dropped. Close all streams before explicitly shutting down the device.
#[derive(Debug)]
pub struct Device {
    inner: DeviceInner,
    config: Config,
    radio: std::sync::Arc<LifecycleController>,
}

#[derive(Clone, Debug)]
enum ConfigOperationKind {
    Configure(Config),
    Frequency(u64),
    SampleRate(u32),
    LnaGain(u8),
    VgaGain(u8),
    TxVgaGain(u8),
    Amp(bool),
    BiasTee(bool),
}

struct ConfigOperation<'a> {
    device: &'a mut Device,
    kind: ConfigOperationKind,
}

impl ConfigOperation<'_> {
    fn begin(&self, style: ExecutionStyle) -> Result<crate::radio::ReconfigureGuard> {
        match self.kind {
            ConfigOperationKind::Configure(_)
            | ConfigOperationKind::Amp(_)
            | ConfigOperationKind::BiasTee(_) => {}
            ConfigOperationKind::Frequency(value) => validate_frequency(value)?,
            ConfigOperationKind::SampleRate(value) => validate_sample_rate(value)?,
            ConfigOperationKind::LnaGain(value) => validate_lna_gain(value)?,
            ConfigOperationKind::VgaGain(value) => validate_vga_gain(value)?,
            ConfigOperationKind::TxVgaGain(value) => validate_tx_vga_gain(value)?,
        }
        self.device.radio.begin_reconfigure(style)
    }

    fn record_success(&mut self) {
        match &self.kind {
            ConfigOperationKind::Configure(config) => {
                self.device.config = config.clone();
                self.device
                    .radio
                    .set_bias_tee(self.device.config.bias_tee_enabled());
            }
            ConfigOperationKind::Frequency(value) => {
                self.device.config.set_frequency_hz_internal(*value)
            }
            ConfigOperationKind::SampleRate(value) => {
                self.device.config.set_sample_rate_hz_internal(*value)
            }
            ConfigOperationKind::LnaGain(value) => {
                self.device.config.set_lna_gain_db_internal(*value)
            }
            ConfigOperationKind::VgaGain(value) => {
                self.device.config.set_vga_gain_db_internal(*value)
            }
            ConfigOperationKind::TxVgaGain(value) => {
                self.device.config.set_tx_vga_gain_db_internal(*value)
            }
            ConfigOperationKind::Amp(value) => self.device.config.set_amp_enabled_internal(*value),
            ConfigOperationKind::BiasTee(value) => {
                self.device.config.set_bias_tee_enabled_internal(*value);
                self.device.radio.set_bias_tee(*value);
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn wait(mut self) -> Result<()> {
        let _guard = self.begin(ExecutionStyle::Blocking)?;
        let result = match &self.kind {
            ConfigOperationKind::Configure(config) => {
                self.device.inner.direct.configure(config).wait()
            }
            ConfigOperationKind::Frequency(value) => {
                self.device.inner.direct.set_frequency(*value).wait()
            }
            ConfigOperationKind::SampleRate(value) => {
                self.device.inner.direct.set_sample_rate(*value).wait()
            }
            ConfigOperationKind::LnaGain(value) => {
                self.device.inner.direct.set_lna_gain(*value).wait()
            }
            ConfigOperationKind::VgaGain(value) => {
                self.device.inner.direct.set_vga_gain(*value).wait()
            }
            ConfigOperationKind::TxVgaGain(value) => {
                self.device.inner.direct.set_tx_vga_gain(*value).wait()
            }
            ConfigOperationKind::Amp(value) => self.device.inner.direct.set_amp(*value).wait(),
            ConfigOperationKind::BiasTee(value) => {
                self.device.inner.direct.set_bias_tee(*value).wait()
            }
        };
        if result.is_ok() {
            self.record_success();
        }
        result
    }

    async fn run_async(mut self) -> Result<()> {
        let _guard = self.begin(ExecutionStyle::Async)?;
        let result = match &self.kind {
            ConfigOperationKind::Configure(config) => {
                self.device.inner.direct.configure(config).await
            }
            ConfigOperationKind::Frequency(value) => {
                self.device.inner.direct.set_frequency(*value).await
            }
            ConfigOperationKind::SampleRate(value) => {
                self.device.inner.direct.set_sample_rate(*value).await
            }
            ConfigOperationKind::LnaGain(value) => {
                self.device.inner.direct.set_lna_gain(*value).await
            }
            ConfigOperationKind::VgaGain(value) => {
                self.device.inner.direct.set_vga_gain(*value).await
            }
            ConfigOperationKind::TxVgaGain(value) => {
                self.device.inner.direct.set_tx_vga_gain(*value).await
            }
            ConfigOperationKind::Amp(value) => self.device.inner.direct.set_amp(*value).await,
            ConfigOperationKind::BiasTee(value) => {
                self.device.inner.direct.set_bias_tee(*value).await
            }
        };
        if result.is_ok() {
            self.record_success();
        }
        result
    }
}

impl<'a> IntoFuture for ConfigOperation<'a> {
    type Output = Result<()>;
    type IntoFuture = BoxFuture<'a, Result<()>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.run_async().await })
    }
}

impl MaybeFuture for ConfigOperation<'_> {
    #[cfg(not(target_arch = "wasm32"))]
    fn wait(self) -> Self::Output {
        self.wait()
    }
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

    /// Apply a complete validated transceiver configuration while off or streaming.
    ///
    /// A live configuration change has no sample-accurate application boundary.
    pub fn configure(&mut self, config: &Config) -> impl MaybeFuture<Output = Result<()>> + '_ {
        ConfigOperation {
            device: self,
            kind: ConfigOperationKind::Configure(config.clone()),
        }
    }

    /// Change the center frequency while the radio is off or streaming.
    ///
    /// A live retune has no sample-accurate application boundary.
    pub fn set_frequency_hz(
        &mut self,
        frequency_hz: u64,
    ) -> impl MaybeFuture<Output = Result<()>> + '_ {
        ConfigOperation {
            device: self,
            kind: ConfigOperationKind::Frequency(frequency_hz),
        }
    }

    /// Change the complex sample rate while the radio is off or streaming.
    ///
    /// The driver also requests a baseband filter bandwidth equal to this rate.
    /// A live rate change has no sample-accurate application boundary.
    pub fn set_sample_rate_hz(
        &mut self,
        sample_rate_hz: u32,
    ) -> impl MaybeFuture<Output = Result<()>> + '_ {
        ConfigOperation {
            device: self,
            kind: ConfigOperationKind::SampleRate(sample_rate_hz),
        }
    }

    /// Set RX IF/LNA gain in dB while the radio is off or streaming.
    ///
    /// A live gain change has no sample-accurate application boundary.
    pub fn set_lna_gain_db(&mut self, gain_db: u8) -> impl MaybeFuture<Output = Result<()>> + '_ {
        ConfigOperation {
            device: self,
            kind: ConfigOperationKind::LnaGain(gain_db),
        }
    }

    /// Set baseband/VGA gain in dB while the radio is off or streaming.
    ///
    /// A live gain change has no sample-accurate application boundary.
    pub fn set_vga_gain_db(&mut self, gain_db: u8) -> impl MaybeFuture<Output = Result<()>> + '_ {
        ConfigOperation {
            device: self,
            kind: ConfigOperationKind::VgaGain(gain_db),
        }
    }

    /// Set TX VGA gain in dB while the radio is off or streaming.
    ///
    /// A live gain change has no sample-accurate application boundary.
    pub fn set_tx_vga_gain_db(
        &mut self,
        gain_db: u8,
    ) -> impl MaybeFuture<Output = Result<()>> + '_ {
        ConfigOperation {
            device: self,
            kind: ConfigOperationKind::TxVgaGain(gain_db),
        }
    }

    /// Enable or disable the RF amplifier while the radio is off or streaming.
    ///
    /// A live amplifier change has no sample-accurate application boundary.
    pub fn set_amp_enable(&mut self, enabled: bool) -> impl MaybeFuture<Output = Result<()>> + '_ {
        ConfigOperation {
            device: self,
            kind: ConfigOperationKind::Amp(enabled),
        }
    }

    /// Enable or disable antenna-port bias power while the radio is off or streaming.
    ///
    /// A live bias-power change has no sample-accurate application boundary.
    pub fn set_bias_tee(&mut self, enabled: bool) -> impl MaybeFuture<Output = Result<()>> + '_ {
        ConfigOperation {
            device: self,
            kind: ConfigOperationKind::BiasTee(enabled),
        }
    }

    /// Claim the device's single owned receive stream.
    ///
    /// The stream is initially stopped. Call [`RxStream::start`] before
    /// reading it and [`RxStream::stop`] before starting TX.
    pub fn rx_stream(&self) -> Result<RxStream> {
        let claim = self.radio.claim_stream(Direction::Rx)?;
        Ok(RxStream::new(
            self.inner.direct.stream_handle(),
            std::sync::Arc::clone(&self.radio),
            claim,
        ))
    }

    /// Claim the device's single owned transmit stream.
    ///
    /// The stream is initially stopped. Call [`TxStream::start`] before
    /// writing it and [`TxStream::stop`] before starting RX.
    pub fn tx_stream(&self) -> Result<TxStream> {
        let claim = self.radio.claim_stream(Direction::Tx)?;
        Ok(TxStream::new(std::sync::Arc::clone(&self.radio), claim))
    }

    /// Put the transceiver in off mode and close the logical device session.
    ///
    /// Returns [`Error::Busy`] while any stream claim or configuration
    /// operation is held, including stopped streams and pending close cleanup.
    /// Close every stream before shutdown. Rejection leaves the device usable.
    /// Once execution begins, the session is terminal, but failed or cancelled
    /// shutdown can be retried. Successful repeated shutdown sends no commands.
    /// The final shared owner performs best-effort cleanup on drop.
    #[must_use = "shutdown must be waited or awaited to observe hardware cleanup"]
    pub fn shutdown(&mut self) -> impl MaybeFuture<Output = Result<()>> + '_ {
        DeviceShutdownOperation { device: self }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn shutdown_blocking(&mut self) -> Result<()> {
        self.radio.shutdown_blocking()
    }

    async fn shutdown_async(&mut self) -> Result<()> {
        self.radio.shutdown_async().await
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

    /// Set the initial TX VGA gain.
    pub fn tx_vga_gain_db(mut self, value: u8) -> Self {
        self.config = self.config.tx_vga_gain_db(value);
        self
    }

    /// Set the initial RF amplifier state.
    pub fn amp_enable(mut self, value: bool) -> Self {
        self.config = self.config.amp_enable(value);
        self
    }

    /// Set the initial antenna-port bias-power state.
    pub fn bias_tee(mut self, value: bool) -> Self {
        self.config = self.config.bias_tee(value);
        self
    }

    /// Open the selected device, query metadata, and apply the configuration.
    pub fn open(self) -> impl MaybeFuture<Output = Result<Device>> {
        let config = self.config.build();
        let serial = self.serial;
        ready(config).and_then(move |config| {
            HackRf::open(serial).and_then(move |(direct, usb_api_version)| {
                let cleanup = OpeningCleanup::new(direct.stream_handle());
                let info = direct.fetch_device_info(usb_api_version);
                info.and_then(move |info| {
                    let flush_size = direct.tx_flush_size(info.usb_api_version);
                    direct
                        .configure(&config)
                        .and_then(move |()| flush_size)
                        .map(move |flush_size| {
                            let flush_size = flush_size?;
                            let tx_endpoint = direct.bulk_out()?;
                            cleanup.disarm();
                            Ok(Device {
                                inner: DeviceInner {
                                    direct: direct.stream_handle(),
                                    info,
                                },
                                radio: LifecycleController::new(
                                    direct,
                                    tx_endpoint,
                                    flush_size,
                                    config.bias_tee_enabled(),
                                ),
                                config,
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
    Paused,
    Running,
}

#[cfg(not(target_arch = "wasm32"))]
struct BlockingRxInner<C: ControlBackend + StreamingBackend> {
    direct: HackRf<C>,
    stream: Option<DirectRxStream<C::BulkIn>>,
    stats: StreamingStats,
    state: ReceiverState,
}

#[cfg(not(target_arch = "wasm32"))]
impl<C> BlockingRxInner<C>
where
    C: ControlBackend + StreamingBackend,
{
    fn new(direct: HackRf<C>) -> Self {
        Self {
            direct,
            stream: None,
            stats: StreamingStats::default(),
            state: ReceiverState::Stopped,
        }
    }

    fn open_queue(&mut self) -> Result<()> {
        match self.state {
            ReceiverState::Running => return Ok(()),
            ReceiverState::CleanupRequired => return Err(Error::Busy),
            ReceiverState::Paused => {
                self.stats = StreamingStats::default();
                self.stream
                    .as_mut()
                    .ok_or(Error::stream_closed("RX stream has no USB queue"))?
                    .reset_stats();
                return Ok(());
            }
            ReceiverState::Stopped => {}
        }
        self.stats = StreamingStats::default();
        self.state = ReceiverState::CleanupRequired;
        self.stream = Some(self.direct.start_rx_blocking()?);
        Ok(())
    }

    fn commit_start(&mut self) {
        debug_assert!(self.stream.is_some());
        self.state = ReceiverState::Running;
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

    fn close_queue(&mut self) -> StreamingStats {
        if self.state == ReceiverState::Stopped {
            return self.stats;
        }
        if let Some(mut stream) = self.stream.take() {
            self.stats = stream.close();
        }
        self.state = ReceiverState::Stopped;
        self.stats
    }

    fn pause_queue(&mut self) -> StreamingStats {
        if self.state == ReceiverState::Running {
            if let Some(stream) = self.stream.as_mut() {
                self.stats = stream.stats();
            }
            self.state = ReceiverState::Paused;
        }
        self.stats
    }

    fn stop(&mut self) -> StreamingStats {
        if self.state == ReceiverState::CleanupRequired {
            self.close_queue()
        } else {
            self.pause_queue()
        }
    }
}

struct AsyncRxInner<C: ControlBackend + AsyncStreamingBackend> {
    direct: HackRf<C>,
    stream: Option<AsyncDirectRxStream<C::BulkIn>>,
    stats: StreamingStats,
    state: ReceiverState,
}

impl<C> AsyncRxInner<C>
where
    C: ControlBackend + AsyncStreamingBackend,
{
    fn new(direct: HackRf<C>) -> Self {
        Self {
            direct,
            stream: None,
            stats: StreamingStats::default(),
            state: ReceiverState::Stopped,
        }
    }

    async fn open_queue(&mut self) -> Result<()> {
        match self.state {
            ReceiverState::Running => return Ok(()),
            ReceiverState::CleanupRequired => return Err(Error::Busy),
            ReceiverState::Paused => {
                self.stats = StreamingStats::default();
                self.stream
                    .as_mut()
                    .ok_or(Error::stream_closed("async RX stream has no USB queue"))?
                    .reset_stats();
                return Ok(());
            }
            ReceiverState::Stopped => {}
        }
        self.stats = StreamingStats::default();
        self.state = ReceiverState::CleanupRequired;
        self.stream = Some(self.direct.start_rx_async().await?);
        Ok(())
    }

    fn commit_start(&mut self) {
        debug_assert!(self.stream.is_some());
        self.state = ReceiverState::Running;
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

    fn close_queue(&mut self) -> StreamingStats {
        if self.state == ReceiverState::Stopped {
            return self.stats;
        }
        if let Some(mut stream) = self.stream.take() {
            self.stats = stream.close();
        }
        self.state = ReceiverState::Stopped;
        self.stats
    }

    fn pause_queue(&mut self) -> StreamingStats {
        if self.state == ReceiverState::Running {
            if let Some(stream) = self.stream.as_mut() {
                self.stats = stream.stats();
            }
            self.state = ReceiverState::Paused;
        }
        self.stats
    }

    fn stop(&mut self) -> StreamingStats {
        if self.state == ReceiverState::CleanupRequired {
            self.close_queue()
        } else {
            self.pause_queue()
        }
    }
}

enum RxStreamState {
    Dormant(HackRf<NusbControl>),
    Poisoned,
    #[cfg(not(target_arch = "wasm32"))]
    Blocking(BlockingRxInner<NusbControl>),
    Async(AsyncRxInner<NusbControl>),
}

/// Owned HackRF receive stream.
///
/// `start` selects RX while the radio is off. `read` only consumes the owned
/// RX queue and never switches direction or takes the lifecycle mutex.
#[must_use = "RX streams retain the device's exclusive RX stream claim until dropped"]
pub struct RxStream {
    state: RxStreamState,
    radio: std::sync::Arc<LifecycleController>,
    failed: bool,
    #[cfg(target_arch = "wasm32")]
    queue_started: bool,
    claim: Option<StreamClaim>,
    cleanup_armed: bool,
}

impl RxStream {
    /// Consume this stream, clean up its transfers, and release its claim.
    ///
    /// Returns the current or most recently completed run's counters. The
    /// operation is lazy and owns the stream; cancellation or failure invokes
    /// best-effort cleanup. Native close supports either execution style.
    ///
    /// ```compile_fail
    /// use hackrf_nusb::{RxStream, MaybeFuture};
    /// async fn consumed(stream: RxStream) {
    ///     let _ = stream.close().await;
    ///     let _ = stream.close().await;
    /// }
    /// ```
    pub fn close(self) -> impl MaybeFuture<Output = Result<StreamingStats>> + 'static {
        RxCloseOperation { stream: self }
    }

    fn new(
        direct: HackRf<NusbControl>,
        radio: std::sync::Arc<LifecycleController>,
        claim: StreamClaim,
    ) -> Self {
        Self {
            state: RxStreamState::Dormant(direct),
            radio,
            failed: false,
            #[cfg(target_arch = "wasm32")]
            queue_started: false,
            claim: Some(claim),
            cleanup_armed: true,
        }
    }

    /// Start or resume RX while the radio is off.
    ///
    /// Returns [`Error::Busy`] when TX is active or any control transition is
    /// in progress. Calling `start` on an already-running RX stream succeeds
    /// without changing hardware state. A different execution style returns
    /// [`Error::Busy`], including when the stream is already running.
    pub fn start(&mut self) -> impl MaybeFuture<Output = Result<()>> + '_ {
        RxStartOperation { stream: self }
    }

    /// Read normalized complex IQ samples from a started RX stream.
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

    /// Stop RX, turn the radio off, and return counters for this run.
    ///
    /// A successful stop retains the RX queue so a later [`Self::start`] can
    /// resume it without submitting a second queue. Use [`Self::close`] for
    /// final queue cleanup and claim release. A stream I/O failure turns the radio off,
    /// closes the queue, and permanently invalidates this stream; drop it and
    /// create a new RX stream before starting again. On WebUSB, dropping an
    /// RX stream whose queue has started is terminal for RX on this device
    /// session; drop and reopen the device instead.
    pub fn stop(&mut self) -> impl MaybeFuture<Output = Result<StreamingStats>> + '_ {
        RxStopOperation { stream: self }
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
            RxStreamState::Dormant(direct) => {
                self.state = RxStreamState::Blocking(BlockingRxInner::new(direct));
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
            RxStreamState::Dormant(direct) => {
                self.state = RxStreamState::Async(AsyncRxInner::new(direct));
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
        if self.failed {
            return Err(self.failed_error());
        }
        if matches!(self.state, RxStreamState::Async(_)) {
            return Err(Error::Busy);
        }
        match self.receiver_state() {
            ReceiverState::Running => return Ok(()),
            ReceiverState::CleanupRequired => return Err(Error::Busy),
            ReceiverState::Stopped | ReceiverState::Paused => {}
        }
        let mut transition = self.radio.begin_rx_start_blocking()?;
        self.initialize_blocking()?;
        match &mut self.state {
            RxStreamState::Blocking(stream) => stream.open_queue()?,
            _ => return Err(Error::Busy),
        }
        transition.finish_blocking()?;
        if let RxStreamState::Blocking(stream) = &mut self.state {
            stream.commit_start();
        }
        Ok(())
    }

    async fn start_async(&mut self) -> Result<()> {
        if self.failed {
            return Err(self.failed_error());
        }
        #[cfg(not(target_arch = "wasm32"))]
        if matches!(self.state, RxStreamState::Blocking(_)) {
            return Err(Error::Busy);
        }
        match self.receiver_state() {
            ReceiverState::Running => return Ok(()),
            ReceiverState::CleanupRequired => return Err(Error::Busy),
            ReceiverState::Stopped | ReceiverState::Paused => {}
        }
        let mut transition = self.radio.begin_rx_start_async().await?;
        self.initialize_async()?;
        match &mut self.state {
            RxStreamState::Async(stream) => stream.open_queue().await?,
            _ => return Err(Error::Busy),
        }
        #[cfg(target_arch = "wasm32")]
        {
            // `open_queue` submits every browser transfer synchronously after
            // its final await, so a successful return proves this stream owns
            // the non-cancellable WebUSB queue.
            self.queue_started = true;
        }
        transition.finish_async().await?;
        if let RxStreamState::Async(stream) = &mut self.state {
            stream.commit_start();
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn read_blocking(&mut self, out: &mut [Complex32], timeout: Duration) -> Result<usize> {
        if self.receiver_state() != ReceiverState::Running {
            return Err(Error::stream_closed("RX stream is stopped"));
        }
        match &mut self.state {
            RxStreamState::Blocking(stream) => {
                let result = stream.read(out, timeout);
                if let Err(error) = result {
                    self.failed = true;
                    self.radio.mark_stream_failed(Direction::Rx);
                    return match self.stop_blocking() {
                        Ok(_) => Err(error),
                        Err(cleanup_error) => {
                            Err(cleanup_error.at("turn radio off after RX failure"))
                        }
                    };
                }
                result
            }
            RxStreamState::Async(_) => Err(Error::Busy),
            RxStreamState::Dormant(_) => Err(Error::stream_closed("RX stream is not started")),
            RxStreamState::Poisoned => Err(Error::stream_closed("RX stream requires stop")),
        }
    }

    async fn read_async(&mut self, out: &mut [Complex32]) -> Result<usize> {
        if self.receiver_state() != ReceiverState::Running {
            return Err(Error::stream_closed("RX stream is stopped"));
        }
        match &mut self.state {
            RxStreamState::Async(stream) => {
                let result = poll_fn(|cx| stream.poll_read(out, cx)).await;
                if let Err(error) = result {
                    self.failed = true;
                    self.radio.mark_stream_failed(Direction::Rx);
                    return match self.stop_async().await {
                        Ok(_) => Err(error),
                        Err(cleanup_error) => {
                            Err(cleanup_error.at("turn radio off after RX failure"))
                        }
                    };
                }
                result
            }
            #[cfg(not(target_arch = "wasm32"))]
            RxStreamState::Blocking(_) => Err(Error::Busy),
            RxStreamState::Dormant(_) => Err(Error::stream_closed("RX stream is not started")),
            RxStreamState::Poisoned => Err(Error::stream_closed("RX stream requires stop")),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn stop_blocking(&mut self) -> Result<StreamingStats> {
        if matches!(self.state, RxStreamState::Async(_)) {
            return Err(Error::Busy);
        }
        let transition = self.radio.begin_stop_blocking(Direction::Rx)?;
        let stats = match &mut self.state {
            RxStreamState::Dormant(_) | RxStreamState::Poisoned => StreamingStats::default(),
            RxStreamState::Blocking(stream) => stream.stop(),
            RxStreamState::Async(_) => return Err(Error::Busy),
        };
        if let Some(mut transition) = transition {
            transition.finish_blocking()?;
        }
        Ok(stats)
    }

    async fn stop_async(&mut self) -> Result<StreamingStats> {
        #[cfg(not(target_arch = "wasm32"))]
        if matches!(self.state, RxStreamState::Blocking(_)) {
            return Err(Error::Busy);
        }
        let transition = self.radio.begin_stop_async(Direction::Rx).await?;
        #[cfg(target_arch = "wasm32")]
        if self.receiver_state() == ReceiverState::CleanupRequired {
            // Closing the queue drops browser-owned transfer promises that
            // WebUSB cannot cancel. Do this before the close so the same
            // stream cannot create a replacement queue after recovery.
            self.abandon_webusb_queue();
        }
        let stats = match &mut self.state {
            RxStreamState::Dormant(_) | RxStreamState::Poisoned => StreamingStats::default(),
            RxStreamState::Async(stream) => stream.stop(),
            #[cfg(not(target_arch = "wasm32"))]
            RxStreamState::Blocking(_) => return Err(Error::Busy),
        };
        if let Some(mut transition) = transition {
            transition.finish_async().await?;
        }
        Ok(stats)
    }

    fn failed_error(&self) -> Error {
        #[cfg(not(target_arch = "wasm32"))]
        {
            Error::stream_closed("RX stream failed; drop it and create a new RX stream")
        }
        #[cfg(target_arch = "wasm32")]
        {
            Error::stream_closed("RX stream failed; drop and reopen the WebUSB device")
        }
    }

    #[cfg(target_arch = "wasm32")]
    fn abandon_webusb_queue(&mut self) {
        if self.queue_started {
            self.failed = true;
            self.radio.mark_rx_queue_abandoned();
        }
    }

    fn close_queue(&mut self) -> StreamingStats {
        #[cfg(target_arch = "wasm32")]
        self.abandon_webusb_queue();
        match &mut self.state {
            RxStreamState::Dormant(_) | RxStreamState::Poisoned => StreamingStats::default(),
            #[cfg(not(target_arch = "wasm32"))]
            RxStreamState::Blocking(stream) => stream.close_queue(),
            RxStreamState::Async(stream) => stream.close_queue(),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn close_blocking(&mut self) -> Result<StreamingStats> {
        let transition = self.radio.begin_close(Direction::Rx)?;
        let stats = self.close_queue();
        if let Some(mut transition) = transition {
            transition.finish_blocking()?;
        }
        self.cleanup_armed = false;
        Ok(stats)
    }

    async fn close_async(&mut self) -> Result<StreamingStats> {
        let transition = self.radio.begin_close(Direction::Rx)?;
        let stats = self.close_queue();
        if let Some(mut transition) = transition {
            transition.finish_async().await?;
        }
        self.cleanup_armed = false;
        Ok(stats)
    }
    // Move the claim with the cleanup attempt so shutdown cannot race the
    // browser's outstanding off request.
    #[cfg(any(test, target_arch = "wasm32"))]
    fn background_cleanup(&mut self) -> impl Future<Output = ()> + 'static {
        self.close_queue();
        self.cleanup_armed = false;
        let radio = std::sync::Arc::clone(&self.radio);
        let claim = self.claim.take();
        async move {
            if let Ok(Some(mut transition)) = radio.begin_close(Direction::Rx) {
                let _ = transition.finish_async().await;
            }
            drop(claim);
        }
    }
}

impl Drop for RxStream {
    fn drop(&mut self) {
        if !self.cleanup_armed {
            return;
        }
        self.radio.mark_stream_failed(Direction::Rx);
        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = self.close_blocking();
        }
        #[cfg(target_arch = "wasm32")]
        wasm_bindgen_futures::spawn_local(self.background_cleanup());
    }
}

/// Owned HackRF transmit stream.
///
/// `start` selects TX while the radio is off. `write` owns the TX transport
/// directly and never changes direction or takes the lifecycle mutex.
#[must_use = "TX streams retain the device's exclusive TX stream claim until dropped"]
pub struct TxStream {
    radio: std::sync::Arc<LifecycleController>,
    transport: Option<TxTransport>,
    stats: TxStreamingStats,
    style: Option<ExecutionStyle>,
    state: TxStreamState,
    failed: bool,
    claim: Option<StreamClaim>,
    cleanup_armed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TxStreamState {
    Dormant,
    Active,
    Recovery,
}

impl TxStream {
    /// Consume this stream, clean up its transfers, and release its claim.
    ///
    /// Returns the current or most recently completed run's counters. The
    /// operation is lazy and owns the stream; cancellation or failure invokes
    /// best-effort cleanup. Native close supports either execution style.
    ///
    /// ```compile_fail
    /// use hackrf_nusb::{TxStream, MaybeFuture};
    /// async fn consumed(stream: TxStream) {
    ///     let _ = stream.close().await;
    ///     let _ = stream.close().await;
    /// }
    /// ```
    pub fn close(self) -> impl MaybeFuture<Output = Result<TxStreamingStats>> + 'static {
        TxCloseOperation { stream: self }
    }

    fn new(radio: std::sync::Arc<LifecycleController>, claim: StreamClaim) -> Self {
        Self {
            radio,
            transport: None,
            stats: TxStreamingStats::default(),
            style: None,
            state: TxStreamState::Dormant,
            failed: false,
            claim: Some(claim),
            cleanup_armed: true,
        }
    }

    /// Start TX while the radio is off.
    ///
    /// Returns [`Error::Busy`] when RX is active or any control transition is
    /// in progress. Calling `start` on an already-running TX stream succeeds
    /// without changing hardware state. A different execution style returns
    /// [`Error::Busy`], including when the stream is already running.
    pub fn start(&mut self) -> impl MaybeFuture<Output = Result<()>> + '_ {
        TxStartOperation { stream: self }
    }

    /// Queue normalized complex IQ samples on a started TX stream.
    ///
    /// Blocking calls honor `timeout`; awaited calls use asynchronous USB and
    /// ignore it. `end_burst` appends the terminal zero-filled flush used by
    /// libhackrf. If a timeout leaves that flush pending, the next write or
    /// `stop` submits it before accepting a later burst.
    pub fn write<'a>(
        &'a mut self,
        samples: &'a [Complex32],
        timeout: Option<Duration>,
        end_burst: bool,
    ) -> impl MaybeFuture<Output = Result<usize>> + 'a {
        TxWriteOperation {
            stream: self,
            samples,
            timeout,
            end_burst,
        }
    }

    /// Flush and drain accepted samples, then turn the radio off.
    ///
    /// If the normal 16-transfer TX backpressure window is full, the terminal
    /// flush may temporarily submit one additional transfer before draining.
    /// This deliberately preserves the requested end-of-burst boundary.
    ///
    /// A successful stop leaves the stream reusable. After a stream I/O
    /// failure, `stop` still attempts to turn the radio off and permanently
    /// invalidates this stream. If draining the endpoint fails, reopen the
    /// device before transmitting again because its pending transfers cannot
    /// be safely reused.
    pub fn stop(&mut self) -> impl MaybeFuture<Output = Result<TxStreamingStats>> + '_ {
        TxStopOperation { stream: self }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn start_blocking(&mut self) -> Result<()> {
        if self.failed {
            return Err(Error::stream_closed(
                "TX stream failed; drop it and create a new TX stream",
            ));
        }
        if self
            .style
            .is_some_and(|selected| selected != ExecutionStyle::Blocking)
        {
            return Err(Error::Busy);
        }
        match self.state {
            TxStreamState::Active => return Ok(()),
            TxStreamState::Recovery => return Err(Error::Busy),
            TxStreamState::Dormant => {}
        }
        let mut transition = self.radio.begin_tx_start_blocking()?;
        let transport = transition.finish_blocking()?;
        self.transport = Some(transport);
        self.style = Some(ExecutionStyle::Blocking);
        self.state = TxStreamState::Active;
        Ok(())
    }

    async fn start_async(&mut self) -> Result<()> {
        if self.failed {
            return Err(Error::stream_closed(
                "TX stream failed; drop it and create a new TX stream",
            ));
        }
        if self
            .style
            .is_some_and(|selected| selected != ExecutionStyle::Async)
        {
            return Err(Error::Busy);
        }
        match self.state {
            TxStreamState::Active => return Ok(()),
            TxStreamState::Recovery => return Err(Error::Busy),
            TxStreamState::Dormant => {}
        }
        let mut transition = self.radio.begin_tx_start_async().await?;
        let transport = transition.finish_async().await?;
        self.transport = Some(transport);
        self.style = Some(ExecutionStyle::Async);
        self.state = TxStreamState::Active;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn write_blocking(
        &mut self,
        samples: &[Complex32],
        timeout: Duration,
        end_burst: bool,
    ) -> Result<usize> {
        match self.state {
            TxStreamState::Active => {}
            TxStreamState::Dormant | TxStreamState::Recovery => {
                return Err(Error::stream_closed("TX stream is not started"));
            }
        }
        if self.style != Some(ExecutionStyle::Blocking) {
            return Err(Error::Busy);
        }
        let result = self
            .transport
            .as_mut()
            .ok_or(Error::stream_closed("TX stream has no transport"))?
            .write_blocking(samples, timeout, end_burst);
        if let Err(error) = result {
            self.failed = true;
            self.state = TxStreamState::Recovery;
            self.radio.mark_stream_failed(Direction::Tx);
            return match self.stop_blocking() {
                Ok(_) => Err(error),
                Err(cleanup_error) => Err(cleanup_error.at("turn radio off after TX failure")),
            };
        }
        result
    }

    async fn write_async(&mut self, samples: &[Complex32], end_burst: bool) -> Result<usize> {
        match self.state {
            TxStreamState::Active => {}
            TxStreamState::Dormant | TxStreamState::Recovery => {
                return Err(Error::stream_closed("TX stream is not started"));
            }
        }
        if self.style != Some(ExecutionStyle::Async) {
            return Err(Error::Busy);
        }
        let result = match self.transport.as_mut() {
            Some(transport) => transport.write_async(samples, end_burst).await,
            None => Err(Error::stream_closed("TX stream has no transport")),
        };
        if let Err(error) = result {
            self.failed = true;
            self.state = TxStreamState::Recovery;
            self.radio.mark_stream_failed(Direction::Tx);
            return match self.stop_async().await {
                Ok(_) => Err(error),
                Err(cleanup_error) => Err(cleanup_error.at("turn radio off after TX failure")),
            };
        }
        result
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn stop_blocking(&mut self) -> Result<TxStreamingStats> {
        self.finish_blocking(false)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn finish_blocking(&mut self, closing: bool) -> Result<TxStreamingStats> {
        let transition = if closing {
            self.radio.begin_close(Direction::Tx)?
        } else {
            self.radio.begin_stop_blocking(Direction::Tx)?
        };
        self.state = TxStreamState::Recovery;
        let (stats, drain_error) =
            if let Some(transport) = self.transport.as_mut().filter(|t| t.is_reusable()) {
                match transport.flush_and_drain_blocking() {
                    Ok(()) => (transport.stats(), None),
                    Err(error) => {
                        self.failed = true;
                        transport.mark_unusable();
                        (transport.stats(), Some(error))
                    }
                }
            } else {
                (
                    self.transport
                        .as_ref()
                        .map_or(self.stats, TxTransport::stats),
                    None,
                )
            };
        if let Some(mut transition) = transition
            && let Err(error) = transition.finish_blocking()
        {
            self.state = TxStreamState::Recovery;
            return Err(match drain_error {
                Some(_) => error.at("turn radio off after TX drain failure"),
                None => error,
            });
        }
        self.stats = stats;
        if let Some(transport) = self.transport.take()
            && transport.is_reusable()
        {
            self.radio.return_tx_transport(transport);
        }
        self.state = TxStreamState::Dormant;
        drain_error.map_or(Ok(stats), Err)
    }

    async fn stop_async(&mut self) -> Result<TxStreamingStats> {
        self.finish_async(false).await
    }

    async fn finish_async(&mut self, closing: bool) -> Result<TxStreamingStats> {
        let transition = if closing {
            self.radio.begin_close(Direction::Tx)?
        } else {
            self.radio.begin_stop_async(Direction::Tx).await?
        };
        self.state = TxStreamState::Recovery;
        let (stats, drain_error) =
            if let Some(transport) = self.transport.as_mut().filter(|t| t.is_reusable()) {
                match transport.flush_and_drain_async().await {
                    Ok(()) => (transport.stats(), None),
                    Err(error) => {
                        self.failed = true;
                        transport.mark_unusable();
                        (transport.stats(), Some(error))
                    }
                }
            } else {
                (
                    self.transport
                        .as_ref()
                        .map_or(self.stats, TxTransport::stats),
                    None,
                )
            };
        if let Some(mut transition) = transition
            && let Err(error) = transition.finish_async().await
        {
            self.state = TxStreamState::Recovery;
            return Err(match drain_error {
                Some(_) => error.at("turn radio off after TX drain failure"),
                None => error,
            });
        }
        self.stats = stats;
        if let Some(transport) = self.transport.take()
            && transport.is_reusable()
        {
            self.radio.return_tx_transport(transport);
        }
        self.state = TxStreamState::Dormant;
        drain_error.map_or(Ok(stats), Err)
    }
    #[cfg(any(test, target_arch = "wasm32"))]
    fn background_cleanup(&mut self) -> impl Future<Output = ()> + 'static {
        self.cleanup_armed = false;
        let mut cleanup = Self {
            radio: std::sync::Arc::clone(&self.radio),
            transport: self.transport.take(),
            stats: self.stats,
            style: self.style,
            state: self.state,
            failed: self.failed,
            claim: self.claim.take(),
            cleanup_armed: false,
        };
        async move {
            let _ = cleanup.finish_async(true).await;
        }
    }
}

impl Drop for TxStream {
    fn drop(&mut self) {
        if !self.cleanup_armed {
            return;
        }
        self.radio.mark_stream_failed(Direction::Tx);
        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = self.finish_blocking(true);
        }
        #[cfg(target_arch = "wasm32")]
        wasm_bindgen_futures::spawn_local(self.background_cleanup());
    }
}

struct RxStartOperation<'a> {
    stream: &'a mut RxStream,
}

impl<'a> IntoFuture for RxStartOperation<'a> {
    type Output = Result<()>;
    type IntoFuture = BoxFuture<'a, Result<()>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.stream.start_async().await })
    }
}

impl MaybeFuture for RxStartOperation<'_> {
    #[cfg(not(target_arch = "wasm32"))]
    fn wait(self) -> Self::Output {
        self.stream.start_blocking()
    }
}

struct RxStopOperation<'a> {
    stream: &'a mut RxStream,
}

impl<'a> IntoFuture for RxStopOperation<'a> {
    type Output = Result<StreamingStats>;
    type IntoFuture = BoxFuture<'a, Result<StreamingStats>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.stream.stop_async().await })
    }
}

impl MaybeFuture for RxStopOperation<'_> {
    #[cfg(not(target_arch = "wasm32"))]
    fn wait(self) -> Self::Output {
        self.stream.stop_blocking()
    }
}

struct TxStartOperation<'a> {
    stream: &'a mut TxStream,
}

impl<'a> IntoFuture for TxStartOperation<'a> {
    type Output = Result<()>;
    type IntoFuture = BoxFuture<'a, Result<()>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.stream.start_async().await })
    }
}

impl MaybeFuture for TxStartOperation<'_> {
    #[cfg(not(target_arch = "wasm32"))]
    fn wait(self) -> Self::Output {
        self.stream.start_blocking()
    }
}

struct TxWriteOperation<'a> {
    stream: &'a mut TxStream,
    samples: &'a [Complex32],
    timeout: Option<Duration>,
    end_burst: bool,
}

impl<'a> IntoFuture for TxWriteOperation<'a> {
    type Output = Result<usize>;
    type IntoFuture = BoxFuture<'a, Result<usize>>;

    fn into_future(self) -> Self::IntoFuture {
        let _ = self.timeout;
        Box::pin(async move { self.stream.write_async(self.samples, self.end_burst).await })
    }
}

impl MaybeFuture for TxWriteOperation<'_> {
    #[cfg(not(target_arch = "wasm32"))]
    fn wait(self) -> Self::Output {
        self.stream.write_blocking(
            self.samples,
            self.timeout.unwrap_or(Duration::MAX),
            self.end_burst,
        )
    }
}

struct TxStopOperation<'a> {
    stream: &'a mut TxStream,
}

impl<'a> IntoFuture for TxStopOperation<'a> {
    type Output = Result<TxStreamingStats>;
    type IntoFuture = BoxFuture<'a, Result<TxStreamingStats>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.stream.stop_async().await })
    }
}

impl MaybeFuture for TxStopOperation<'_> {
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
    type IntoFuture = BoxFuture<'a, Result<usize>>;

    fn into_future(self) -> Self::IntoFuture {
        let _ = self.timeout;
        Box::pin(async move { self.stream.read_async(self.out).await })
    }
}

impl MaybeFuture for ReadOperation<'_> {
    #[cfg(not(target_arch = "wasm32"))]
    fn wait(self) -> Self::Output {
        self.stream
            .read_blocking(self.out, self.timeout.unwrap_or(Duration::MAX))
    }
}

struct DeviceShutdownOperation<'a> {
    device: &'a mut Device,
}

impl<'a> IntoFuture for DeviceShutdownOperation<'a> {
    type Output = Result<()>;
    type IntoFuture = BoxFuture<'a, Result<()>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.device.shutdown_async().await })
    }
}

impl MaybeFuture for DeviceShutdownOperation<'_> {
    #[cfg(not(target_arch = "wasm32"))]
    fn wait(self) -> Self::Output {
        self.device.shutdown_blocking()
    }
}

struct RxCloseOperation {
    stream: RxStream,
}

impl IntoFuture for RxCloseOperation {
    type Output = Result<StreamingStats>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            let stats = self.stream.close_async().await?;
            self.stream.cleanup_armed = false;
            self.stream.claim.take();
            drop(self);
            Ok(stats)
        })
    }
}

impl MaybeFuture for RxCloseOperation {
    #[cfg(not(target_arch = "wasm32"))]
    fn wait(mut self) -> Self::Output {
        let stats = self.stream.close_blocking()?;
        self.stream.cleanup_armed = false;
        self.stream.claim.take();
        drop(self);
        Ok(stats)
    }
}

struct TxCloseOperation {
    stream: TxStream,
}

impl IntoFuture for TxCloseOperation {
    type Output = Result<TxStreamingStats>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            let stats = self.stream.finish_async(true).await?;
            self.stream.cleanup_armed = false;
            self.stream.claim.take();
            drop(self);
            Ok(stats)
        })
    }
}

impl MaybeFuture for TxCloseOperation {
    #[cfg(not(target_arch = "wasm32"))]
    fn wait(mut self) -> Self::Output {
        let stats = self.stream.finish_blocking(true)?;
        self.stream.cleanup_armed = false;
        self.stream.claim.take();
        drop(self);
        Ok(stats)
    }
}

#[cfg(test)]
fn test_device() -> (Device, std::sync::Arc<std::sync::Mutex<crate::usb::TestIo>>) {
    let (control, endpoint, io) = crate::usb::TestIo::backends();
    let direct = HackRf::from_control(control);
    let radio = LifecycleController::new(direct.stream_handle(), endpoint, 512, false);
    (
        Device {
            inner: DeviceInner {
                direct,
                info: DeviceInfo {
                    board_id: crate::BoardId::HackRfOne,
                    firmware_version: String::new(),
                    usb_api_version: 0,
                    serial: None,
                },
            },
            config: Config::default(),
            radio,
        },
        io,
    )
}

#[cfg(all(test, target_arch = "wasm32"))]
#[path = "high_level/webusb_tests.rs"]
mod webusb_tests;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod lifecycle_tests {
    use super::*;
    use crate::usb::TestIo;
    use futures_lite::future::{block_on, poll_once};
    use std::sync::Arc;

    use super::test_device as device;

    fn run<O: MaybeFuture>(operation: O, asynchronous: bool) -> O::Output {
        if asynchronous {
            block_on(operation.into_future())
        } else {
            operation.wait()
        }
    }

    #[test]
    fn active_start_checks_execution_style_before_idempotent_success() {
        for asynchronous in [false, true] {
            let (mut device, io) = device();
            let mut rx = device.rx_stream().unwrap();
            run(rx.start(), asynchronous).unwrap();
            let before = io.lock().unwrap().events.len();
            run(rx.start(), asynchronous).unwrap();
            assert!(matches!(run(rx.start(), !asynchronous), Err(Error::Busy)));
            assert!(matches!(run(rx.stop(), !asynchronous), Err(Error::Busy)));
            assert!(matches!(
                run(rx.read(&mut [Complex32::default(); 1], None), !asynchronous),
                Err(Error::Busy)
            ));
            assert!(matches!(
                run(device.set_frequency_hz(100_000_000), !asynchronous),
                Err(Error::Busy)
            ));
            assert_eq!(io.lock().unwrap().events.len(), before);
            run(rx.close(), !asynchronous).unwrap();

            let mut tx = device.tx_stream().unwrap();
            run(tx.start(), asynchronous).unwrap();
            let before = io.lock().unwrap().events.len();
            run(tx.start(), asynchronous).unwrap();
            assert!(matches!(run(tx.start(), !asynchronous), Err(Error::Busy)));
            assert!(matches!(run(tx.stop(), !asynchronous), Err(Error::Busy)));
            assert!(matches!(
                run(
                    tx.write(&[Complex32::default(); 1], None, false),
                    !asynchronous
                ),
                Err(Error::Busy)
            ));
            assert_eq!(io.lock().unwrap().events.len(), before);
            run(tx.close(), !asynchronous).unwrap();
            device.shutdown().wait().unwrap();
        }
    }

    #[test]
    fn close_matrix_retains_per_run_stats_and_releases_all_owners() {
        for asynchronous in [false, true] {
            for close_async in [false, true] {
                for stopped in [false, true] {
                    let (mut device, io) = device();
                    let mut rx = device.rx_stream().unwrap();
                    let mut tx = device.tx_stream().unwrap();
                    run(rx.start(), asynchronous).unwrap();
                    let mut samples = [Complex32::default(); 64];
                    run(
                        rx.read(&mut samples, Some(Duration::from_secs(1))),
                        asynchronous,
                    )
                    .unwrap();
                    let rx_stats = run(rx.stop(), asynchronous).unwrap();
                    run(tx.start(), asynchronous).unwrap();
                    run(tx.write(&samples, None, false), asynchronous).unwrap();
                    let tx_stats = if stopped {
                        Some(run(tx.stop(), asynchronous).unwrap())
                    } else {
                        None
                    };
                    assert!(matches!(device.shutdown().wait(), Err(Error::Busy)));
                    let before = io.lock().unwrap().events.len();
                    let operation = rx.close();
                    assert_eq!(io.lock().unwrap().events.len(), before);
                    assert_eq!(run(operation, close_async).unwrap(), rx_stats);
                    let stats = run(tx.close(), close_async).unwrap();
                    assert_eq!(stats.samples_accepted, 64);
                    assert_eq!(stats.flush_buffers, 1);
                    assert_eq!(stats.buffers_completed, 2);
                    if let Some(previous) = tx_stats {
                        assert_eq!(stats, previous);
                    }
                    assert_eq!(Arc::strong_count(&device.radio), 1);
                    device.shutdown().wait().unwrap();
                    let before = io.lock().unwrap().events.len();
                    device.shutdown().wait().unwrap();
                    drop(device);
                    assert_eq!(io.lock().unwrap().events.len(), before);
                }
            }
        }
    }

    #[test]
    fn rx_active_close_retires_before_off_and_supports_replacement() {
        for asynchronous in [false, true] {
            let (mut device, io) = device();
            let mut rx = device.rx_stream().unwrap();
            run(rx.start(), asynchronous).unwrap();
            io.lock().unwrap().events.clear();
            run(rx.close(), !asynchronous).unwrap();
            assert_eq!(io.lock().unwrap().events, ["rx:retire", "control:0"]);
            let mut tx = device.tx_stream().unwrap();
            run(tx.start(), asynchronous).unwrap();
            run(tx.stop(), asynchronous).unwrap();
            let mut rx = device.rx_stream().unwrap();
            run(rx.start(), asynchronous).unwrap();
            let before = io.lock().unwrap().events.len();
            run(tx.close(), !asynchronous).unwrap();
            assert_eq!(io.lock().unwrap().events.len(), before);
            run(rx.close(), !asynchronous).unwrap();
            device.shutdown().wait().unwrap();
        }
    }

    #[test]
    fn dormant_and_unpolled_close_hold_claims_without_io() {
        let (mut device, io) = device();
        let rx = device.rx_stream().unwrap();
        let tx = device.tx_stream().unwrap();
        let rx_close = rx.close();
        let tx_close = tx.close().into_future();
        assert!(io.lock().unwrap().events.is_empty());
        assert!(matches!(device.shutdown().wait(), Err(Error::Busy)));
        assert!(matches!(device.rx_stream(), Err(Error::Busy)));
        drop(rx_close);
        drop(tx_close);
        assert!(io.lock().unwrap().events.is_empty());
        assert_eq!(
            device.rx_stream().unwrap().close().wait().unwrap(),
            StreamingStats::default()
        );
        assert_eq!(
            device.tx_stream().unwrap().close().wait().unwrap(),
            TxStreamingStats::default()
        );
        device.shutdown().wait().unwrap();
    }

    #[test]
    fn cancelled_close_restores_transition_before_fallback() {
        for rx_direction in [false, true] {
            for during_off in [false, true] {
                if rx_direction && !during_off {
                    continue;
                }
                let (mut device, io) = device();
                let operation: BoxFuture<'static, Result<()>> = if rx_direction {
                    let mut rx = device.rx_stream().unwrap();
                    block_on(rx.start().into_future()).unwrap();
                    Box::pin(async move { rx.close().await.map(|_| ()) })
                } else {
                    let mut tx = device.tx_stream().unwrap();
                    block_on(tx.start().into_future()).unwrap();
                    block_on(
                        tx.write(&[Complex32::default(); 1], None, false)
                            .into_future(),
                    )
                    .unwrap();
                    Box::pin(async move { tx.close().await.map(|_| ()) })
                };
                {
                    let mut io = io.lock().unwrap();
                    io.pause_off = during_off;
                    io.pause_drain = !during_off;
                }
                let mut operation = operation;
                assert!(block_on(poll_once(operation.as_mut())).is_none());
                assert!(matches!(device.shutdown().wait(), Err(Error::Busy)));
                drop(operation);
                assert_eq!(Arc::strong_count(&device.radio), 1);
                device.rx_stream().unwrap().close().wait().unwrap();
                device.tx_stream().unwrap().close().wait().unwrap();
                device.shutdown().wait().unwrap();
            }
        }
    }

    #[test]
    fn drain_failure_attempts_off_and_never_redrains_or_reuses_transport() {
        for asynchronous in [false, true] {
            let (mut device, io) = device();
            let mut tx = device.tx_stream().unwrap();
            run(tx.start(), asynchronous).unwrap();
            run(
                tx.write(&[Complex32::default(); 1], None, false),
                asynchronous,
            )
            .unwrap();
            {
                let mut io = io.lock().unwrap();
                io.events.clear();
                io.fail_drain = true;
                io.fail_off = true;
            }
            assert!(run(tx.close(), asynchronous).is_err());
            assert_eq!(
                io.lock().unwrap().events,
                ["submit:512", "complete", "control:0", "control:0"]
            );
            assert!(matches!(device.tx_stream(), Err(Error::Busy)));
            io.lock().unwrap().fail_off = false;
            device.shutdown().wait().unwrap();
        }
        let (device, io) = device();
        let mut tx = device.tx_stream().unwrap();
        tx.start().wait().unwrap();
        tx.write(&[Complex32::default(); 1], None, false)
            .wait()
            .unwrap();
        io.lock().unwrap().fail_drain = true;
        assert!(tx.close().wait().is_err());
        assert!(device.tx_stream().unwrap().start().wait().is_err());
    }

    #[test]
    fn failure_cleanup_releases_claim_and_shutdown_recovers() {
        for asynchronous in [false, true] {
            let (mut device, io) = device();
            let mut rx = device.rx_stream().unwrap();
            run(rx.start(), asynchronous).unwrap();
            io.lock().unwrap().fail_rx = true;
            assert!(
                run(
                    rx.read(&mut [Complex32::default(); 1], Some(Duration::from_secs(1))),
                    asynchronous
                )
                .is_err()
            );
            run(rx.close(), !asynchronous).unwrap();
            let mut rx = device.rx_stream().unwrap();
            run(rx.start(), asynchronous).unwrap();
            io.lock().unwrap().fail_off = true;
            assert!(run(rx.close(), !asynchronous).is_err());
            assert!(matches!(device.rx_stream(), Err(Error::Busy)));
            assert!(device.shutdown().wait().is_err());
            assert!(matches!(device.tx_stream(), Err(Error::DeviceClosed)));
            io.lock().unwrap().fail_off = false;
            device.shutdown().wait().unwrap();
        }
    }

    #[test]
    fn shutdown_is_lazy_retryable_and_configuration_gated() {
        let (mut device, io) = device();
        drop(device.shutdown().into_future());
        assert!(io.lock().unwrap().events.is_empty());
        let guard = device
            .radio
            .begin_reconfigure(ExecutionStyle::Async)
            .unwrap();
        assert!(matches!(device.shutdown().wait(), Err(Error::Busy)));
        drop(guard);
        io.lock().unwrap().pause_off = true;
        let mut operation = Box::pin(device.shutdown().into_future());
        assert!(block_on(poll_once(operation.as_mut())).is_none());
        drop(operation);
        assert!(matches!(device.rx_stream(), Err(Error::DeviceClosed)));
        TestIo::resume(&io);
        device.shutdown().wait().unwrap();
    }

    #[test]
    fn device_drop_preserves_streams_and_final_owner_cleans_up() {
        for asynchronous in [false, true] {
            let (device, io) = device();
            let mut rx = device.rx_stream().unwrap();
            let mut tx = device.tx_stream().unwrap();
            drop(device);
            assert!(io.lock().unwrap().events.is_empty());
            run(rx.start(), asynchronous).unwrap();
            run(rx.close(), !asynchronous).unwrap();
            run(tx.start(), asynchronous).unwrap();
            io.lock().unwrap().events.clear();
            run(tx.close(), !asynchronous).unwrap();
            assert_eq!(io.lock().unwrap().events, ["control:0", "control:0"]);
        }
    }

    #[test]
    fn full_tx_queue_flushes_before_drain_and_new_run_resets_stats() {
        for asynchronous in [false, true] {
            let (mut device, io) = device();
            let mut tx = device.tx_stream().unwrap();
            run(tx.start(), asynchronous).unwrap();
            for _ in 0..crate::constants::TRANSFER_COUNT {
                run(
                    tx.write(&[Complex32::default(); 1], None, false),
                    asynchronous,
                )
                .unwrap();
            }
            io.lock().unwrap().events.clear();
            let stats = run(tx.stop(), asynchronous).unwrap();
            let events = io.lock().unwrap().events.clone();
            assert_eq!(events.first().unwrap(), "submit:512");
            assert_eq!(events.last().unwrap(), "control:0");
            assert_eq!(
                stats.buffers_completed,
                crate::constants::TRANSFER_COUNT as u64 + 1
            );
            assert_eq!(stats.flush_buffers, 1);
            run(tx.start(), asynchronous).unwrap();
            assert_eq!(
                run(tx.close(), !asynchronous).unwrap(),
                TxStreamingStats::default()
            );
            device.shutdown().wait().unwrap();
        }
    }

    #[test]
    fn active_unpolled_operations_are_lazy_and_drop_cleans_up() {
        for asynchronous in [false, true] {
            for convert in [false, true] {
                let (mut device, io) = device();
                let mut rx = device.rx_stream().unwrap();
                run(rx.start(), asynchronous).unwrap();
                io.lock().unwrap().events.clear();
                let operation = rx.close();
                assert!(io.lock().unwrap().events.is_empty());
                if convert {
                    let future = operation.into_future();
                    assert!(io.lock().unwrap().events.is_empty());
                    drop(future);
                } else {
                    drop(operation);
                }
                assert_eq!(io.lock().unwrap().events, ["rx:retire", "control:0"]);
                let mut tx = device.tx_stream().unwrap();
                run(tx.start(), asynchronous).unwrap();
                run(
                    tx.write(&[Complex32::default(); 1], None, false),
                    asynchronous,
                )
                .unwrap();
                io.lock().unwrap().events.clear();
                let operation = tx.close();
                assert!(io.lock().unwrap().events.is_empty());
                if convert {
                    let future = operation.into_future();
                    assert!(io.lock().unwrap().events.is_empty());
                    drop(future);
                } else {
                    drop(operation);
                }
                assert_eq!(
                    io.lock().unwrap().events,
                    ["submit:512", "complete", "complete", "control:0"]
                );
                device.shutdown().wait().unwrap();
            }
        }
    }

    #[test]
    fn background_cleanup_holds_claim_through_completion_or_failure() {
        for rx_direction in [false, true] {
            for fail_off in [false, true] {
                let (mut device, io) = device();
                let mut cleanup: BoxFuture<'static, ()> = if rx_direction {
                    let mut rx = device.rx_stream().unwrap();
                    block_on(rx.start().into_future()).unwrap();
                    Box::pin(rx.background_cleanup())
                } else {
                    let mut tx = device.tx_stream().unwrap();
                    block_on(tx.start().into_future()).unwrap();
                    Box::pin(tx.background_cleanup())
                };
                assert!(matches!(device.shutdown().wait(), Err(Error::Busy)));
                io.lock().unwrap().pause_off = true;
                assert!(block_on(poll_once(cleanup.as_mut())).is_none());
                assert!(matches!(device.shutdown().wait(), Err(Error::Busy)));
                {
                    let mut io = io.lock().unwrap();
                    io.pause_off = false;
                    io.fail_off = fail_off;
                }
                block_on(cleanup);
                assert_eq!(Arc::strong_count(&device.radio), 1);
                if fail_off {
                    assert!(matches!(device.rx_stream(), Err(Error::Busy)));
                } else {
                    device.rx_stream().unwrap().close().wait().unwrap();
                    device.tx_stream().unwrap().close().wait().unwrap();
                }
                io.lock().unwrap().fail_off = false;
                device.shutdown().wait().unwrap();
            }
        }
    }

    #[test]
    fn close_operations_and_futures_are_send_static() {
        fn check<T: Send + 'static>(_: &T) {}
        let (device, _) = device();
        let rx = device.rx_stream().unwrap().close();
        let tx = device.tx_stream().unwrap().close();
        check(&rx);
        check(&tx);
        check(&rx.into_future());
        check(&tx.into_future());
    }
}
