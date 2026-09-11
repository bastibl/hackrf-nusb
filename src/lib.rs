//! Rust-native half-duplex HackRF driver built on [`nusb`].
//!
//! One-shot and streaming operations implement [`MaybeFuture`]: await them in
//! asynchronous code, or call [`MaybeFuture::wait`] on native targets. Do not
//! mix execution styles across RX and TX streams. HackRF direction changes are
//! explicit: start one stream, use it, stop it, then start the other. The
//! receive stream owns its USB endpoint and transfer queue, so the steady-state
//! read path does not lock shared device state.
//!
//! # Synchronous example
//!
//! ```no_run
//! use hackrf_nusb::{Complex32, Device, MaybeFuture};
//! use std::time::Duration;
//!
//! fn main() -> hackrf_nusb::Result<()> {
//!     let mut device = Device::builder()
//!         .frequency_hz(100_000_000)
//!         .sample_rate_hz(10_000_000)
//!         .open()
//!         .wait()?;
//!     let mut rx = device.rx_stream()?;
//!     rx.start().wait()?;
//!     let mut samples = [Complex32::default(); 1024];
//!     let count = rx.read(&mut samples, Some(Duration::from_secs(1))).wait()?;
//!     println!("received {count} IQ samples");
//!     rx.close().wait()?;
//!     device.shutdown().wait()?;
//!     Ok(())
//! }
//! ```
//!
//! On `wasm32`, call `Device::request_permission` from a browser-window user
//! gesture before opening the device. WebUSB operations are asynchronous only.

#![deny(missing_docs)]

mod commands;
mod config;
mod constants;
mod device;
mod discovery;
mod errors;
mod high_level;
mod maybe_future;
mod radio;
mod streaming;
mod types;
mod usb;

pub use config::{Config, ConfigBuilder};
pub use constants::MAX_COMPLEX_SAMPLES_PER_TRANSFER;
pub use discovery::DeviceDescriptor;
pub use errors::{Error, ErrorKind, Result};
pub use high_level::{Device, DeviceBuilder, RxStream, TxStream};
pub use num_complex::Complex32;
pub use nusb::MaybeFuture;
pub use radio::TxStreamingStats;
pub use streaming::StreamingStats;
pub use types::{BoardId, DeviceInfo};
