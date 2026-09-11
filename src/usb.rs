//! Internal nusb control-transfer and bulk endpoint backends.

use std::task::{Context, Poll};
use std::time::Duration;

use nusb::transfer::{
    Buffer as NusbBuffer, Bulk, ControlIn, ControlOut, ControlType, In, Out, Recipient,
};
use nusb::{Endpoint, MaybeFuture};

use crate::commands::{TransceiverMode, VendorRequest};
use crate::constants::CONTROL_TIMEOUT;
use crate::errors::{Error, Result};
#[cfg(all(test, not(target_arch = "wasm32")))]
use crate::maybe_future::Either;
use crate::maybe_future::{MaybeFutureExt, ready};
use crate::streaming::{AsyncBulkInBackend, AsyncStreamingBackend, BulkInCompletion};
#[cfg(not(target_arch = "wasm32"))]
use crate::streaming::{BulkInBackend, StreamingBackend};
use crate::types::PartIdSerial;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ControlDirection {
    In,
    Out,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VendorControlRequest {
    pub(crate) direction: ControlDirection,
    pub(crate) request: VendorRequest,
    pub(crate) value: u16,
    pub(crate) index: u16,
    pub(crate) length: usize,
    pub(crate) data: Vec<u8>,
    pub(crate) timeout: Duration,
}

impl VendorControlRequest {
    pub(crate) fn in_request(
        request: VendorRequest,
        value: u16,
        index: u16,
        length: usize,
    ) -> Self {
        Self {
            direction: ControlDirection::In,
            request,
            value,
            index,
            length,
            data: Vec::new(),
            timeout: CONTROL_TIMEOUT,
        }
    }

    pub(crate) fn out_request(
        request: VendorRequest,
        value: u16,
        index: u16,
        data: Vec<u8>,
    ) -> Self {
        let length = data.len();
        Self {
            direction: ControlDirection::Out,
            request,
            value,
            index,
            length,
            data,
            timeout: CONTROL_TIMEOUT,
        }
    }

    pub(crate) fn transceiver_mode(mode: TransceiverMode) -> Self {
        Self::out_request(
            VendorRequest::SetTransceiverMode,
            mode as u16,
            0,
            Vec::new(),
        )
    }

    pub(crate) fn set_frequency(frequency_hz: u64) -> Self {
        let mhz = (frequency_hz / 1_000_000) as u32;
        let remainder = (frequency_hz % 1_000_000) as u32;
        let mut data = Vec::with_capacity(8);
        data.extend_from_slice(&mhz.to_le_bytes());
        data.extend_from_slice(&remainder.to_le_bytes());
        Self::out_request(VendorRequest::SetFreq, 0, 0, data)
    }

    pub(crate) fn set_sample_rate(sample_rate_hz: u32) -> Self {
        let mut data = Vec::with_capacity(8);
        data.extend_from_slice(&sample_rate_hz.to_le_bytes());
        data.extend_from_slice(&1_u32.to_le_bytes());
        Self::out_request(VendorRequest::SampleRateSet, 0, 0, data)
    }

    pub(crate) fn set_baseband_bandwidth(bandwidth_hz: u32) -> Self {
        Self::out_request(
            VendorRequest::BasebandFilterBandwidthSet,
            bandwidth_hz as u16,
            (bandwidth_hz >> 16) as u16,
            Vec::new(),
        )
    }

    pub(crate) fn set_lna_gain(gain_db: u8) -> Self {
        Self::in_request(VendorRequest::SetLnaGain, 0, gain_db as u16, 1)
    }

    pub(crate) fn set_vga_gain(gain_db: u8) -> Self {
        Self::in_request(VendorRequest::SetVgaGain, 0, gain_db as u16, 1)
    }

    pub(crate) fn set_tx_vga_gain(gain_db: u8) -> Self {
        Self::in_request(VendorRequest::SetTxVgaGain, 0, gain_db as u16, 1)
    }

    pub(crate) fn set_amp(enabled: bool) -> Self {
        Self::out_request(VendorRequest::AmpEnable, enabled.into(), 0, Vec::new())
    }

    pub(crate) fn set_bias_tee(enabled: bool) -> Self {
        Self::out_request(VendorRequest::AntennaEnable, enabled.into(), 0, Vec::new())
    }

    pub(crate) fn board_id_read() -> Self {
        Self::in_request(VendorRequest::BoardIdRead, 0, 0, 1)
    }

    pub(crate) fn version_string_read(length: usize) -> Self {
        Self::in_request(VendorRequest::VersionStringRead, 0, 0, length)
    }

    pub(crate) fn part_id_serial_read() -> Self {
        Self::in_request(VendorRequest::BoardPartIdSerialNoRead, 0, 0, 24)
    }

    pub(crate) fn get_buffer_size() -> Self {
        Self::in_request(VendorRequest::GetBufferSize, 0, 0, 4)
    }

    fn nusb_control_in(&self) -> Result<ControlIn> {
        if self.direction != ControlDirection::In || self.length > u16::MAX as usize {
            return Err(Error::protocol(
                "encode control IN request",
                "invalid direction or length",
            ));
        }
        Ok(ControlIn {
            control_type: ControlType::Vendor,
            recipient: Recipient::Device,
            request: self.request as u8,
            value: self.value,
            index: self.index,
            length: self.length as u16,
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) trait BackendSafe: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync> BackendSafe for T {}

#[cfg(target_arch = "wasm32")]
pub(crate) trait BackendSafe {}
#[cfg(target_arch = "wasm32")]
impl<T> BackendSafe for T {}

pub(crate) trait ControlBackend: std::fmt::Debug + BackendSafe {
    fn control_in(
        &self,
        request: VendorControlRequest,
    ) -> impl MaybeFuture<Output = Result<Vec<u8>>> + use<Self>;

    fn control_out(
        &self,
        request: VendorControlRequest,
    ) -> impl MaybeFuture<Output = Result<()>> + use<Self>;
}

#[derive(Debug)]
pub(crate) struct NusbControl {
    connection: ControlConnection,
}

#[derive(Debug)]
enum ControlConnection {
    Usb {
        _device: nusb::Device,
        interface: nusb::Interface,
    },
    #[cfg(all(test, not(target_arch = "wasm32")))]
    Test(std::sync::Arc<std::sync::Mutex<TestIo>>),
}

#[derive(Debug)]
pub(crate) struct NusbBulkIn {
    endpoint: InEndpoint,
}

#[derive(Debug)]
enum InEndpoint {
    Usb(Endpoint<Bulk, In>),
    #[cfg(all(test, not(target_arch = "wasm32")))]
    Test {
        io: std::sync::Arc<std::sync::Mutex<TestIo>>,
        buffers: std::collections::VecDeque<NusbBuffer>,
    },
}

impl InEndpoint {
    fn allocate(&self, len: usize) -> NusbBuffer {
        match self {
            Self::Usb(endpoint) => endpoint.allocate(len),
            #[cfg(all(test, not(target_arch = "wasm32")))]
            Self::Test { .. } => NusbBuffer::new(len),
        }
    }

    fn submit(&mut self, buffer: NusbBuffer) {
        match self {
            Self::Usb(endpoint) => endpoint.submit(buffer),
            #[cfg(all(test, not(target_arch = "wasm32")))]
            Self::Test { buffers, .. } => buffers.push_back(buffer),
        }
    }

    fn pending(&self) -> usize {
        match self {
            Self::Usb(endpoint) => endpoint.pending(),
            #[cfg(all(test, not(target_arch = "wasm32")))]
            Self::Test { buffers, .. } => buffers.len(),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn cancel_all(&mut self) {
        match self {
            Self::Usb(endpoint) => endpoint.cancel_all(),
            #[cfg(all(test, not(target_arch = "wasm32")))]
            Self::Test { io, buffers } => {
                io.lock().unwrap().events.push("rx:retire".into());
                buffers.clear();
            }
        }
    }

    #[cfg(all(test, not(target_arch = "wasm32")))]
    fn test_complete(&mut self) -> BulkInCompletion<NusbBuffer> {
        let Self::Test { io, buffers } = self else {
            unreachable!()
        };
        buffers.pop_front().unwrap();
        let failed = io.lock().unwrap().fail_rx;
        BulkInCompletion {
            buffer: vec![1; 512].into(),
            actual_len: 512,
            status: if failed { Err(Error::Busy) } else { Ok(()) },
        }
    }
}

#[derive(Debug)]
pub(crate) struct NusbBulkOut {
    endpoint: OutEndpoint,
}

#[derive(Debug)]
enum OutEndpoint {
    Usb(Endpoint<Bulk, Out>),
    #[cfg(all(test, not(target_arch = "wasm32")))]
    Test(std::sync::Arc<std::sync::Mutex<TestIo>>),
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[derive(Debug, Default)]
pub(crate) struct TestIo {
    pub events: Vec<String>,
    pub pending: usize,
    pub fail_rx: bool,
    pub fail_off: bool,
    pub pause_off: bool,
    pub fail_drain: bool,
    pub pause_drain: bool,
}

#[cfg(all(test, not(target_arch = "wasm32")))]
impl TestIo {
    pub(crate) fn backends() -> (
        NusbControl,
        NusbBulkOut,
        std::sync::Arc<std::sync::Mutex<Self>>,
    ) {
        let io = std::sync::Arc::new(std::sync::Mutex::new(Self::default()));
        (
            NusbControl {
                connection: ControlConnection::Test(io.clone()),
            },
            NusbBulkOut {
                endpoint: OutEndpoint::Test(io.clone()),
            },
            io,
        )
    }
}

impl NusbControl {
    pub(crate) fn new(device: nusb::Device, interface: nusb::Interface) -> Self {
        Self {
            connection: ControlConnection::Usb {
                _device: device,
                interface,
            },
        }
    }

    fn interface(&self) -> &nusb::Interface {
        match &self.connection {
            ControlConnection::Usb { interface, .. } => interface,
            #[cfg(all(test, not(target_arch = "wasm32")))]
            ControlConnection::Test(_) => panic!("test control has no USB interface"),
        }
    }

    pub(crate) fn bulk_out(&self, endpoint: u8) -> Result<NusbBulkOut> {
        Ok(NusbBulkOut {
            endpoint: OutEndpoint::Usb(
                self.interface()
                    .endpoint::<Bulk, Out>(endpoint)
                    .map_err(Error::from)?,
            ),
        })
    }
}

impl NusbBulkOut {
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn clear_halt_blocking(&mut self) -> Result<()> {
        match &mut self.endpoint {
            OutEndpoint::Usb(endpoint) => endpoint.clear_halt().wait().map_err(Error::from),
            #[cfg(all(test, not(target_arch = "wasm32")))]
            OutEndpoint::Test(_) => Ok(()),
        }
    }

    pub(crate) async fn clear_halt_async(&mut self) -> Result<()> {
        match &mut self.endpoint {
            OutEndpoint::Usb(endpoint) => endpoint.clear_halt().await.map_err(Error::from),
            #[cfg(all(test, not(target_arch = "wasm32")))]
            OutEndpoint::Test(_) => Ok(()),
        }
    }

    pub(crate) fn submit(&mut self, bytes: Vec<u8>) {
        match &mut self.endpoint {
            OutEndpoint::Usb(endpoint) => endpoint.submit(bytes.into()),
            #[cfg(all(test, not(target_arch = "wasm32")))]
            OutEndpoint::Test(io) => {
                let mut io = io.lock().unwrap();
                io.events.push(format!("submit:{}", bytes.len()));
                io.pending += 1;
            }
        }
    }

    pub(crate) fn pending(&self) -> usize {
        match &self.endpoint {
            OutEndpoint::Usb(endpoint) => endpoint.pending(),
            #[cfg(all(test, not(target_arch = "wasm32")))]
            OutEndpoint::Test(io) => io.lock().unwrap().pending,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn wait_next_complete(&mut self, timeout: Duration) -> Option<Result<()>> {
        match &mut self.endpoint {
            OutEndpoint::Usb(endpoint) => endpoint
                .wait_next_complete(timeout)
                .map(|completion| completion.status.map_err(Error::from)),
            #[cfg(all(test, not(target_arch = "wasm32")))]
            OutEndpoint::Test(io) => Some(test_complete(io)),
        }
    }

    pub(crate) async fn next_complete(&mut self) -> Result<()> {
        match &mut self.endpoint {
            OutEndpoint::Usb(endpoint) => {
                endpoint.next_complete().await.status.map_err(Error::from)
            }
            #[cfg(all(test, not(target_arch = "wasm32")))]
            OutEndpoint::Test(io) => {
                std::future::poll_fn(|_| {
                    if io.lock().unwrap().pause_drain {
                        Poll::Pending
                    } else {
                        Poll::Ready(test_complete(io))
                    }
                })
                .await
            }
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
fn test_complete(io: &std::sync::Mutex<TestIo>) -> Result<()> {
    let mut io = io.lock().unwrap();
    io.events.push("complete".into());
    if io.fail_drain {
        return Err(Error::Busy);
    }
    io.pending -= 1;
    Ok(())
}

#[cfg(all(test, not(target_arch = "wasm32")))]
struct TestControlOperation {
    io: std::sync::Arc<std::sync::Mutex<TestIo>>,
    value: u16,
    off: bool,
}

#[cfg(all(test, not(target_arch = "wasm32")))]
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

#[cfg(all(test, not(target_arch = "wasm32")))]
impl std::future::IntoFuture for TestControlOperation {
    type Output = Result<()>;
    type IntoFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send>>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(std::future::poll_fn(move |_| {
            if self.off && self.io.lock().unwrap().pause_off {
                Poll::Pending
            } else {
                Poll::Ready(self.execute())
            }
        }))
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
impl MaybeFuture for TestControlOperation {
    #[cfg(not(target_arch = "wasm32"))]
    fn wait(self) -> Result<()> {
        self.execute()
    }
}

impl ControlBackend for NusbControl {
    fn control_in(
        &self,
        request: VendorControlRequest,
    ) -> impl MaybeFuture<Output = Result<Vec<u8>>> + use<> {
        let interface = self.interface().clone();
        let timeout = request.timeout;
        ready(request.nusb_control_in())
            .and_then(move |control| interface.control_in(control, timeout).map_err(Error::from))
    }

    fn control_out(
        &self,
        request: VendorControlRequest,
    ) -> impl MaybeFuture<Output = Result<()>> + use<> {
        #[cfg(all(test, not(target_arch = "wasm32")))]
        if let ControlConnection::Test(io) = &self.connection {
            return Either::left(TestControlOperation {
                io: io.clone(),
                value: request.value,
                off: request.request == VendorRequest::SetTransceiverMode && request.value == 0,
            });
        }
        let interface = self.interface().clone();
        let timeout = request.timeout;
        let validation = if request.direction == ControlDirection::Out {
            Ok(request)
        } else {
            Err(Error::protocol(
                "encode control OUT request",
                "invalid direction",
            ))
        };
        let operation = ready(validation).and_then(move |request| {
            interface
                .control_out(
                    ControlOut {
                        control_type: ControlType::Vendor,
                        recipient: Recipient::Device,
                        request: request.request as u8,
                        value: request.value,
                        index: request.index,
                        data: &request.data,
                    },
                    timeout,
                )
                .map_err(Error::from)
        });
        #[cfg(all(test, not(target_arch = "wasm32")))]
        {
            Either::right(operation)
        }
        #[cfg(not(all(test, not(target_arch = "wasm32"))))]
        {
            operation
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl StreamingBackend for NusbControl {
    type BulkIn = NusbBulkIn;

    fn bulk_in(&self, endpoint: u8) -> Result<Self::BulkIn> {
        #[cfg(all(test, not(target_arch = "wasm32")))]
        if let ControlConnection::Test(io) = &self.connection {
            return Ok(NusbBulkIn {
                endpoint: InEndpoint::Test {
                    io: io.clone(),
                    buffers: Default::default(),
                },
            });
        }
        Ok(NusbBulkIn {
            endpoint: InEndpoint::Usb(
                self.interface()
                    .endpoint::<Bulk, In>(endpoint)
                    .map_err(Error::from)?,
            ),
        })
    }
}

impl AsyncStreamingBackend for NusbControl {
    type BulkIn = NusbBulkIn;

    fn bulk_in(&self, endpoint: u8) -> Result<Self::BulkIn> {
        #[cfg(all(test, not(target_arch = "wasm32")))]
        if let ControlConnection::Test(io) = &self.connection {
            return Ok(NusbBulkIn {
                endpoint: InEndpoint::Test {
                    io: io.clone(),
                    buffers: Default::default(),
                },
            });
        }
        Ok(NusbBulkIn {
            endpoint: InEndpoint::Usb(
                self.interface()
                    .endpoint::<Bulk, In>(endpoint)
                    .map_err(Error::from)?,
            ),
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl BulkInBackend for NusbBulkIn {
    type Buffer = NusbBuffer;

    fn clear_halt(&mut self) -> Result<()> {
        match &mut self.endpoint {
            InEndpoint::Usb(endpoint) => endpoint.clear_halt().wait().map_err(Error::from),
            #[cfg(all(test, not(target_arch = "wasm32")))]
            InEndpoint::Test { .. } => Ok(()),
        }
    }

    fn allocate(&self, len: usize) -> Self::Buffer {
        self.endpoint.allocate(len)
    }

    fn submit(&mut self, buffer: Self::Buffer) {
        self.endpoint.submit(buffer);
    }

    fn pending(&self) -> usize {
        self.endpoint.pending()
    }

    fn wait_next_complete(&mut self, timeout: Duration) -> Option<BulkInCompletion<Self::Buffer>> {
        match &mut self.endpoint {
            InEndpoint::Usb(endpoint) => {
                endpoint
                    .wait_next_complete(timeout)
                    .map(|completion| BulkInCompletion {
                        buffer: completion.buffer,
                        actual_len: completion.actual_len,
                        status: completion.status.map_err(Error::from),
                    })
            }
            #[cfg(all(test, not(target_arch = "wasm32")))]
            endpoint @ InEndpoint::Test { .. } => Some(endpoint.test_complete()),
        }
    }

    fn cancel_all(&mut self) {
        self.endpoint.cancel_all();
    }
}

impl AsyncBulkInBackend for NusbBulkIn {
    type Buffer = NusbBuffer;

    async fn clear_halt_async(&mut self) -> Result<()> {
        match &mut self.endpoint {
            InEndpoint::Usb(endpoint) => endpoint.clear_halt().await.map_err(Error::from),
            #[cfg(all(test, not(target_arch = "wasm32")))]
            InEndpoint::Test { .. } => Ok(()),
        }
    }

    fn allocate(&self, len: usize) -> Self::Buffer {
        self.endpoint.allocate(len)
    }

    fn submit(&mut self, buffer: Self::Buffer) {
        self.endpoint.submit(buffer);
    }

    fn pending(&self) -> usize {
        self.endpoint.pending()
    }

    fn poll_next_complete(&mut self, cx: &mut Context<'_>) -> Poll<BulkInCompletion<Self::Buffer>> {
        match &mut self.endpoint {
            InEndpoint::Usb(endpoint) => {
                endpoint
                    .poll_next_complete(cx)
                    .map(|completion| BulkInCompletion {
                        buffer: completion.buffer,
                        actual_len: completion.actual_len,
                        status: completion.status.map_err(Error::from),
                    })
            }
            #[cfg(all(test, not(target_arch = "wasm32")))]
            endpoint @ InEndpoint::Test { .. } => Poll::Ready(endpoint.test_complete()),
        }
    }

    fn cancel_all(&mut self) {
        #[cfg(not(target_arch = "wasm32"))]
        self.endpoint.cancel_all();
    }
}

pub(crate) fn decode_part_id_serial(bytes: &[u8]) -> Result<PartIdSerial> {
    let (chunks, remainder) = bytes.as_chunks::<4>();
    if !remainder.is_empty() || chunks.len() != 6 {
        return Err(Error::protocol(
            "decode part ID and serial number",
            "response must contain exactly six little-endian words",
        ));
    }
    let words = chunks
        .iter()
        .copied()
        .map(u32::from_le_bytes)
        .collect::<Vec<_>>();
    Ok(PartIdSerial {
        part_id: [words[0], words[1]],
        serial: [words[2], words[3], words[4], words[5]],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frequency_is_split_into_little_endian_mhz_and_remainder() {
        let request = VendorControlRequest::set_frequency(2_480_123_456);
        assert_eq!(request.request, VendorRequest::SetFreq);
        assert_eq!(&request.data[..4], &2480_u32.to_le_bytes());
        assert_eq!(&request.data[4..], &123_456_u32.to_le_bytes());
    }

    #[test]
    fn sample_rate_uses_integer_divider_one() {
        let request = VendorControlRequest::set_sample_rate(10_000_000);
        assert_eq!(&request.data[..4], &10_000_000_u32.to_le_bytes());
        assert_eq!(&request.data[4..], &1_u32.to_le_bytes());
    }

    #[test]
    fn full_bandwidth_is_packed_in_value_and_index() {
        let request = VendorControlRequest::set_baseband_bandwidth(20_000_000);
        assert_eq!(request.value, 20_000_000_u32 as u16);
        assert_eq!(request.index, (20_000_000_u32 >> 16) as u16);
    }

    #[test]
    fn tx_gain_and_flush_size_requests_match_libhackrf() {
        let gain = VendorControlRequest::set_tx_vga_gain(47);
        assert_eq!(gain.request, VendorRequest::SetTxVgaGain);
        assert_eq!(gain.index, 47);
        assert_eq!(gain.length, 1);

        let flush = VendorControlRequest::get_buffer_size();
        assert_eq!(flush.request, VendorRequest::GetBufferSize);
        assert_eq!(flush.length, 4);
    }

    #[test]
    fn part_serial_words_join_in_usb_descriptor_order() {
        let words = [
            0x1111_1111_u32,
            0x2222_2222,
            0x0011_2233,
            0x4455_6677,
            0x8899_aabb,
            0xccdd_eeff,
        ];
        let bytes = words
            .into_iter()
            .flat_map(u32::to_le_bytes)
            .collect::<Vec<_>>();
        let decoded = decode_part_id_serial(&bytes).unwrap();
        assert_eq!(decoded.part_id, [0x1111_1111, 0x2222_2222]);
        assert_eq!(
            decoded.serial_u128(),
            Some(0x0011_2233_4455_6677_8899_aabb_ccdd_eeff)
        );
    }
}
