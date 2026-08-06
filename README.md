# hackrf-nusb

`hackrf-nusb` is an experimental Rust-native HackRF driver built directly on
[`nusb`](https://crates.io/crates/nusb). It does not link to libusb or
libhackrf.

HackRF is half duplex. This driver makes the hardware boundary explicit:

```text
RX: start → read … → stop → Off
TX: start → write … → stop → Off
```

Starting RX while TX is active, or TX while RX is active, returns `Error::Busy`.
The driver never switches direction on a read or write. A higher-level adapter
may implement automatic TDD policy, but the core driver deliberately does not.

## Status

- discovery, opening, device information, and normal HackRF configuration;
- one owned RX stream and one owned TX stream;
- explicit reusable `start`/`stop` operations;
- synchronous and executor-agnostic asynchronous APIs;
- WebUSB support on `wasm32-unknown-unknown`;
- `Complex32` IQ conversion and TX terminal-flush handling.

There is no resampling or alternate raw/floating-point stream format. The
configured rate is the complex sample rate. Signed 8-bit IQ values are
normalized by `128.0` on receive.

## Synchronous API

```rust,no_run
use hackrf_nusb::{Complex32, Device, MaybeFuture};
use std::time::Duration;

fn main() -> hackrf_nusb::Result<()> {
    let mut device = Device::builder()
        .frequency_hz(100_000_000)
        .sample_rate_hz(10_000_000)
        .open()
        .wait()?;

    let mut rx = device.rx_stream()?;
    rx.start().wait()?;

    // This takes effect during RX, with no sample-accurate boundary.
    device.set_lna_gain_db(24).wait()?;

    let mut samples = [Complex32::default(); 4096];
    let count = rx.read(&mut samples, Some(Duration::from_secs(1))).wait()?;
    let stats = rx.stop().wait()?;
    println!("received {count} samples; {stats:?}");

    device.shutdown().wait()?;
    Ok(())
}
```

To transmit after RX, stop RX first:

```rust,no_run
# use hackrf_nusb::{Complex32, Device, MaybeFuture};
# use std::time::Duration;
# fn main() -> hackrf_nusb::Result<()> {
# let mut device = Device::open().wait()?;
# let mut rx = device.rx_stream()?;
# let mut tx = device.tx_stream()?;
# rx.start().wait()?;
rx.stop().wait()?;
tx.start().wait()?;
tx.write(&[Complex32::default(); 1024], Some(Duration::from_secs(1)), true)
    .wait()?;
tx.stop().wait()?;
# Ok(()) }
```

`start` and a successful `stop` are reusable. A read/write stream I/O failure
turns the radio off automatically but permanently invalidates that stream;
on native targets, drop it and create a fresh stream from the same device. On
WebUSB, drop and reopen the device before replacing a failed RX stream because
the browser cannot cancel its pending RX transfers. If that automatic stop
fails, retry `stop` before replacing the stream. Shutdown is terminal for a
device handle: if its hardware request fails or is cancelled, the handle
remains closed. Drop it and reopen the device to try again. Dropping a device
or stream attempts cleanup but cannot report its result and is not a lifecycle
guarantee.

## Execution and performance

Streaming and one-shot operations implement `nusb::MaybeFuture`. On native
targets, call `.wait()` for blocking operation or `.await` the same value in
async code. WebUSB supports only the async form. Do not mix execution styles
for one device session.

The RX stream owns endpoint `0x81` and its persistent queue of 16 × 256 KiB
transfers. A successful RX `stop` turns the radio off but retains that queue;
the next `start` resumes it without submitting a second queue. Drop the RX
stream for final best-effort queue cleanup. An active TX stream owns endpoint
`0x02` and its outstanding transfers. TX writes use a 16-transfer backpressure
window; `stop` may queue one additional terminal flush before draining, to
preserve the end-of-burst boundary. Steady-state `RxStream::read` and
`TxStream::write` do not take the direction controller mutex; only start,
stop, configuration, and shutdown do.

Configuration is allowed while the radio is off or streaming. A live control
request has no sample-accurate application boundary; callers that need a clean
configuration boundary should stop, configure, then start a stream.

## WebUSB

On `wasm32-unknown-unknown`, request permission from a browser user gesture:

```rust,ignore
Device::builder().serial(serial).request_permission().await?;
let device = Device::open_serial(serial).await?;
```

The final application crate must enable the unstable `web-sys` WebUSB bindings.
This repository's `.cargo/config.toml` contains the required setting. RX
`stop`/`start` retains and resumes the existing queue, which avoids trying to
cancel WebUSB bulk-IN transfers or create a second queue during a TX handoff.
Samples already buffered before `stop` may be returned after `start`.
Dropping an RX stream after it has started permanently reserves its browser
queue for the device session; reopen the device before creating a replacement
RX stream.

Manually validate a browser handoff on connected hardware by repeating RX
`start`/`read` → `stop`, TX `start`/`write`/`stop`, then RX `start`/`read` in
one device session.

## Hardware tests

Examples and hardware tests are gated and do not touch USB unless explicitly
requested:

```sh
cargo run --example rx_sync -- --run
cargo run --example rx_sync -- --run --rx
cargo run --features smol --example rx_async -- --run --rx
cargo test --test hardware --features smol -- --ignored --test-threads=1 --nocapture
```
