//! HackRF USB and configuration constants.

use std::time::Duration;

pub(crate) const CONTROL_TIMEOUT: Duration = Duration::from_millis(100);
pub(crate) const USB_CONFIGURATION: u8 = 1;
pub(crate) const USB_INTERFACE: u8 = 0;
pub(crate) const RX_ENDPOINT: u8 = 0x81;
pub(crate) const TRANSFER_COUNT: usize = 16;
pub(crate) const TRANSFER_SIZE: usize = 262_144;
pub(crate) const VERSION_STRING_SIZE: usize = 255;

/// Maximum number of complex IQ samples in one HackRF USB transfer.
pub const MAX_COMPLEX_SAMPLES_PER_TRANSFER: usize = TRANSFER_SIZE / 2;
