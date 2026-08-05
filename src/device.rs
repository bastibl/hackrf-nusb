//! Direct HackRF control plane and stream construction.

use std::sync::Arc;

use nusb::MaybeFuture;

use crate::commands::TransceiverMode;
use crate::config::Config;
use crate::constants::{RX_ENDPOINT, USB_CONFIGURATION, USB_INTERFACE, VERSION_STRING_SIZE};
use crate::discovery;
use crate::errors::{Error, Result};
use crate::maybe_future::MaybeFutureExt;
use crate::streaming::{AsyncDirectRxStream, AsyncStreamingBackend};
#[cfg(not(target_arch = "wasm32"))]
use crate::streaming::{DirectRxStream, StreamingBackend, StreamingStats};
use crate::types::{BoardId, DeviceInfo, PartIdSerial};
use crate::usb::{ControlBackend, NusbControl, VendorControlRequest, decode_part_id_serial};

#[derive(Debug)]
pub(crate) struct HackRf<C = NusbControl> {
    control: Arc<C>,
}

impl<C> HackRf<C> {
    pub(crate) fn from_control(control: C) -> Self {
        Self {
            control: Arc::new(control),
        }
    }

    pub(crate) fn stream_handle(&self) -> Self {
        Self {
            control: Arc::clone(&self.control),
        }
    }
}

impl<C: ControlBackend> HackRf<C> {
    pub(crate) fn set_transceiver_mode(
        &self,
        mode: TransceiverMode,
    ) -> impl MaybeFuture<Output = Result<()>> + use<C> {
        self.control
            .control_out(VendorControlRequest::transceiver_mode(mode))
    }

    pub(crate) fn set_frequency(
        &self,
        frequency_hz: u64,
    ) -> impl MaybeFuture<Output = Result<()>> + use<C> {
        self.control
            .control_out(VendorControlRequest::set_frequency(frequency_hz))
    }

    pub(crate) fn set_sample_rate(
        &self,
        sample_rate_hz: u32,
    ) -> impl MaybeFuture<Output = Result<()>> + use<C> {
        let rate = self
            .control
            .control_out(VendorControlRequest::set_sample_rate(sample_rate_hz));
        let bandwidth = self
            .control
            .control_out(VendorControlRequest::set_baseband_bandwidth(sample_rate_hz));
        rate.and_then(move |()| bandwidth)
    }

    pub(crate) fn set_lna_gain(
        &self,
        gain_db: u8,
    ) -> impl MaybeFuture<Output = Result<()>> + use<C> {
        self.control_in_exact(VendorControlRequest::set_lna_gain(gain_db), 1)
            .map(|result| validate_gain_response(result?, "set LNA gain"))
    }

    pub(crate) fn set_vga_gain(
        &self,
        gain_db: u8,
    ) -> impl MaybeFuture<Output = Result<()>> + use<C> {
        self.control_in_exact(VendorControlRequest::set_vga_gain(gain_db), 1)
            .map(|result| validate_gain_response(result?, "set VGA gain"))
    }

    pub(crate) fn set_amp(&self, enabled: bool) -> impl MaybeFuture<Output = Result<()>> + use<C> {
        self.control
            .control_out(VendorControlRequest::set_amp(enabled))
    }

    pub(crate) fn set_bias_tee(
        &self,
        enabled: bool,
    ) -> impl MaybeFuture<Output = Result<()>> + use<C> {
        self.control
            .control_out(VendorControlRequest::set_bias_tee(enabled))
    }

    pub(crate) fn board_id_read(&self) -> impl MaybeFuture<Output = Result<BoardId>> + use<C> {
        self.control_in_exact(VendorControlRequest::board_id_read(), 1)
            .map(|result| result.map(|bytes| BoardId::from_raw(bytes[0])))
    }

    pub(crate) fn version_string_read(&self) -> impl MaybeFuture<Output = Result<String>> + use<C> {
        self.control
            .control_in(VendorControlRequest::version_string_read(
                VERSION_STRING_SIZE,
            ))
            .map(|result| result.map(|bytes| decode_c_string(&bytes)))
    }

    pub(crate) fn part_id_serial_read(
        &self,
    ) -> impl MaybeFuture<Output = Result<PartIdSerial>> + use<C> {
        self.control_in_exact(VendorControlRequest::part_id_serial_read(), 24)
            .map(|result| result.and_then(|bytes| decode_part_id_serial(&bytes)))
    }

    pub(crate) fn fetch_device_info(
        &self,
        usb_api_version: u16,
    ) -> impl MaybeFuture<Output = Result<DeviceInfo>> + use<C> {
        let board = self.board_id_read();
        let firmware = self.version_string_read();
        let serial = self.part_id_serial_read();
        board
            .and_then(move |board_id| firmware.map_ok(move |firmware| (board_id, firmware)))
            .and_then(move |(board_id, firmware)| {
                serial.map_ok(move |part_serial| DeviceInfo {
                    board_id,
                    firmware_version: firmware,
                    usb_api_version,
                    serial: part_serial.serial_u128(),
                })
            })
    }

    pub(crate) fn configure(
        &self,
        config: &Config,
    ) -> impl MaybeFuture<Output = Result<()>> + use<C> {
        let rate = self.set_sample_rate(config.sample_rate_hz());
        let frequency = self.set_frequency(config.frequency_hz());
        let lna = self.set_lna_gain(config.lna_gain_db());
        let vga = self.set_vga_gain(config.vga_gain_db());
        let amp = self.set_amp(config.amp_enabled());
        let bias = self.set_bias_tee(config.bias_tee_enabled());
        rate.and_then(move |()| frequency)
            .and_then(move |()| lna)
            .and_then(move |()| vga)
            .and_then(move |()| amp)
            .and_then(move |()| bias)
    }

    fn control_in_exact(
        &self,
        request: VendorControlRequest,
        expected: usize,
    ) -> impl MaybeFuture<Output = Result<Vec<u8>>> + use<C> {
        self.control.control_in(request).map(move |result| {
            let bytes = result?;
            if bytes.len() != expected {
                return Err(Error::protocol(
                    "read control response",
                    "response has an unexpected length",
                ));
            }
            Ok(bytes)
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<C> HackRf<C>
where
    C: ControlBackend + StreamingBackend,
{
    pub(crate) fn start_rx_blocking(&self, bias_tee: bool) -> Result<DirectRxStream<C::BulkIn>> {
        let bulk_in = self.control.as_ref().bulk_in(RX_ENDPOINT)?;
        let prepared = DirectRxStream::prepare(bulk_in);
        self.set_transceiver_mode(TransceiverMode::Receive).wait()?;
        if let Err(error) = self.set_bias_tee(bias_tee).wait() {
            let _ = self.set_transceiver_mode(TransceiverMode::Off).wait();
            return Err(error);
        }
        match prepared.start_blocking() {
            Ok(stream) => Ok(stream),
            Err(error) => {
                let _ = self.set_transceiver_mode(TransceiverMode::Off).wait();
                Err(error)
            }
        }
    }

    pub(crate) fn stop_rx_blocking(
        &self,
        mut stream: DirectRxStream<C::BulkIn>,
    ) -> (StreamingStats, Result<()>) {
        let stats = stream.close();
        let result = self.set_transceiver_mode(TransceiverMode::Off).wait();
        (stats, result)
    }
}

impl<C> HackRf<C>
where
    C: ControlBackend + AsyncStreamingBackend,
{
    pub(crate) async fn start_rx_async(
        &self,
        bias_tee: bool,
    ) -> Result<AsyncDirectRxStream<C::BulkIn>> {
        let bulk_in = self.control.as_ref().bulk_in(RX_ENDPOINT)?;
        let prepared = AsyncDirectRxStream::prepare(bulk_in);
        self.set_transceiver_mode(TransceiverMode::Receive).await?;
        if let Err(error) = self.set_bias_tee(bias_tee).await {
            let _ = self.set_transceiver_mode(TransceiverMode::Off).await;
            return Err(error);
        }
        match prepared.start_async().await {
            Ok(stream) => Ok(stream),
            Err(error) => {
                let _ = self.set_transceiver_mode(TransceiverMode::Off).await;
                Err(error)
            }
        }
    }

    pub(crate) async fn restart_rx_async(&self, bias_tee: bool) -> Result<()> {
        self.set_transceiver_mode(TransceiverMode::Receive).await?;
        if let Err(error) = self.set_bias_tee(bias_tee).await {
            let _ = self.set_transceiver_mode(TransceiverMode::Off).await;
            return Err(error);
        }
        Ok(())
    }

    pub(crate) async fn stop_rx_async(
        &self,
        stream: &mut AsyncDirectRxStream<C::BulkIn>,
    ) -> Result<()> {
        self.set_transceiver_mode(TransceiverMode::Off).await?;
        stream.pause()
    }
}

impl HackRf<NusbControl> {
    pub(crate) fn open(serial: Option<u128>) -> impl MaybeFuture<Output = Result<(Self, u16)>> {
        discovery::select_device(serial)
            .and_then(|info| {
                let usb_api_version = info.device_version();
                info.open()
                    .map_err(|error| Error::from(error).at("opening HackRF USB device"))
                    .map_ok(move |device| (device, usb_api_version))
            })
            .and_then(|(device, usb_api_version)| {
                device
                    .set_configuration(USB_CONFIGURATION)
                    .map(move |result| {
                        match result {
                            Ok(()) => {}
                            Err(error) if error.kind() == nusb::ErrorKind::Unsupported => {}
                            Err(error) => {
                                return Err(Error::from(error).at("selecting USB configuration 1"));
                            }
                        }
                        Ok((device, usb_api_version))
                    })
            })
            .and_then(|(device, usb_api_version)| {
                device
                    .detach_and_claim_interface(USB_INTERFACE)
                    .map_err(|error| Error::from(error).at("claiming HackRF USB interface 0"))
                    .map_ok(move |interface| (device, interface, usb_api_version))
            })
            .and_then(|(device, interface, usb_api_version)| {
                let direct = Self::from_control(NusbControl::new(device, interface));
                direct
                    .set_transceiver_mode(TransceiverMode::Off)
                    .map_ok(move |()| (direct, usb_api_version))
            })
    }
}

fn validate_gain_response(bytes: Vec<u8>, operation: &'static str) -> Result<()> {
    if bytes == [1] {
        Ok(())
    } else {
        Err(Error::protocol(
            operation,
            "firmware rejected the gain value",
        ))
    }
}

fn decode_c_string(bytes: &[u8]) -> String {
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

pub(crate) fn shutdown_hardware<C: ControlBackend>(
    direct: &HackRf<C>,
) -> impl MaybeFuture<Output = Result<()>> + use<C> {
    direct.set_transceiver_mode(TransceiverMode::Off)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::usb::{ControlDirection, VendorControlRequest};

    #[derive(Debug, Default)]
    struct FakeControl {
        requests: Mutex<Vec<VendorControlRequest>>,
    }

    impl ControlBackend for FakeControl {
        fn control_in(
            &self,
            request: VendorControlRequest,
        ) -> impl MaybeFuture<Output = Result<Vec<u8>>> + use<> {
            self.requests.lock().unwrap().push(request.clone());
            let data = match request.request {
                crate::commands::VendorRequest::SetLnaGain
                | crate::commands::VendorRequest::SetVgaGain => vec![1],
                _ => vec![0; request.length],
            };
            crate::maybe_future::ready(Ok(data))
        }

        fn control_out(
            &self,
            request: VendorControlRequest,
        ) -> impl MaybeFuture<Output = Result<()>> + use<> {
            self.requests.lock().unwrap().push(request);
            crate::maybe_future::ready(Ok(()))
        }
    }

    #[test]
    fn configuration_sets_full_bandwidth_immediately_after_rate() {
        let direct = HackRf::from_control(FakeControl::default());
        direct.configure(&Config::default()).wait().unwrap();
        let requests = direct.control.requests.lock().unwrap();
        assert_eq!(
            requests[0].request,
            crate::commands::VendorRequest::SampleRateSet
        );
        assert_eq!(
            requests[1].request,
            crate::commands::VendorRequest::BasebandFilterBandwidthSet
        );
        assert_eq!(requests[1].value, 10_000_000_u32 as u16);
        assert_eq!(requests[1].index, (10_000_000_u32 >> 16) as u16);
        assert!(requests.iter().all(|request| {
            request.direction == ControlDirection::In || request.direction == ControlDirection::Out
        }));
    }
}
