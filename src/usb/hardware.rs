//! Direct nusb control and endpoint implementations.
use std::task::{Context, Poll};
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

use nusb::transfer::{
    Buffer as NusbBuffer, Bulk, ControlIn, ControlOut, ControlType, In, Out, Recipient,
};
use nusb::{Endpoint, MaybeFuture};

use super::{ControlBackend, ControlDirection, VendorControlRequest};
use crate::errors::{Error, Result};
use crate::maybe_future::{MaybeFutureExt, ready};
use crate::streaming::{AsyncBulkInBackend, AsyncStreamingBackend, BulkInCompletion};
#[cfg(not(target_arch = "wasm32"))]
use crate::streaming::{BulkInBackend, StreamingBackend};

impl VendorControlRequest {
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

#[derive(Debug)]
pub(crate) struct NusbControl {
    _device: nusb::Device,
    interface: nusb::Interface,
}

#[derive(Debug)]
pub(crate) struct NusbBulkIn {
    endpoint: Endpoint<Bulk, In>,
}

#[derive(Debug)]
pub(crate) struct NusbBulkOut {
    endpoint: Endpoint<Bulk, Out>,
}

impl NusbControl {
    pub(crate) fn new(device: nusb::Device, interface: nusb::Interface) -> Self {
        Self {
            _device: device,
            interface,
        }
    }

    pub(crate) fn bulk_out(&self, endpoint: u8) -> Result<NusbBulkOut> {
        Ok(NusbBulkOut {
            endpoint: self
                .interface
                .endpoint::<Bulk, Out>(endpoint)
                .map_err(Error::from)?,
        })
    }
}

impl NusbBulkOut {
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn clear_halt_blocking(&mut self) -> Result<()> {
        self.endpoint.clear_halt().wait().map_err(Error::from)
    }

    pub(crate) async fn clear_halt_async(&mut self) -> Result<()> {
        self.endpoint.clear_halt().await.map_err(Error::from)
    }

    pub(crate) fn submit(&mut self, bytes: Vec<u8>) {
        self.endpoint.submit(bytes.into());
    }

    pub(crate) fn pending(&self) -> usize {
        self.endpoint.pending()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn wait_next_complete(&mut self, timeout: Duration) -> Option<Result<()>> {
        self.endpoint
            .wait_next_complete(timeout)
            .map(|completion| completion.status.map_err(Error::from))
    }

    pub(crate) async fn next_complete(&mut self) -> Result<()> {
        self.endpoint
            .next_complete()
            .await
            .status
            .map_err(Error::from)
    }
}

impl ControlBackend for NusbControl {
    fn control_in(
        &self,
        request: VendorControlRequest,
    ) -> impl MaybeFuture<Output = Result<Vec<u8>>> + use<> {
        let interface = self.interface.clone();
        let timeout = request.timeout;
        ready(request.nusb_control_in())
            .and_then(move |control| interface.control_in(control, timeout).map_err(Error::from))
    }

    fn control_out(
        &self,
        request: VendorControlRequest,
    ) -> impl MaybeFuture<Output = Result<()>> + use<> {
        let interface = self.interface.clone();
        let timeout = request.timeout;
        let validation = if request.direction == ControlDirection::Out {
            Ok(request)
        } else {
            Err(Error::protocol(
                "encode control OUT request",
                "invalid direction",
            ))
        };
        ready(validation).and_then(move |request| {
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
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl StreamingBackend for NusbControl {
    type BulkIn = NusbBulkIn;

    fn bulk_in(&self, endpoint: u8) -> Result<Self::BulkIn> {
        Ok(NusbBulkIn {
            endpoint: self
                .interface
                .endpoint::<Bulk, In>(endpoint)
                .map_err(Error::from)?,
        })
    }
}

impl AsyncStreamingBackend for NusbControl {
    type BulkIn = NusbBulkIn;

    fn bulk_in(&self, endpoint: u8) -> Result<Self::BulkIn> {
        Ok(NusbBulkIn {
            endpoint: self
                .interface
                .endpoint::<Bulk, In>(endpoint)
                .map_err(Error::from)?,
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl BulkInBackend for NusbBulkIn {
    type Buffer = NusbBuffer;

    fn clear_halt(&mut self) -> Result<()> {
        self.endpoint.clear_halt().wait().map_err(Error::from)
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
        self.endpoint
            .wait_next_complete(timeout)
            .map(|completion| BulkInCompletion {
                buffer: completion.buffer,
                actual_len: completion.actual_len,
                status: completion.status.map_err(Error::from),
            })
    }

    fn cancel_all(&mut self) {
        self.endpoint.cancel_all();
    }
}

impl AsyncBulkInBackend for NusbBulkIn {
    type Buffer = NusbBuffer;

    async fn clear_halt_async(&mut self) -> Result<()> {
        self.endpoint.clear_halt().await.map_err(Error::from)
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
        self.endpoint
            .poll_next_complete(cx)
            .map(|completion| BulkInCompletion {
                buffer: completion.buffer,
                actual_len: completion.actual_len,
                status: completion.status.map_err(Error::from),
            })
    }

    fn cancel_all(&mut self) {
        #[cfg(not(target_arch = "wasm32"))]
        self.endpoint.cancel_all();
    }
}
