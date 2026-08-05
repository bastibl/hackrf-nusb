# hackrf-nusb

`hackrf-nusb` is an experimental, Rust-native HackRF driver built directly on
[`nusb`](https://crates.io/crates/nusb). It does not link to libusb or
libhackrf. Its control protocol follows the upstream `libhackrf` implementation,
while its owned device/stream API follows the shape of `hydrasdr-rs`.

The driver is currently RX-only. HackRF hardware is half duplex, and TX is not
exposed yet.

## Status

Implemented:

- discovery and opening of HackRF Jawbreaker (`1d50:604b`), HackRF One / HackRF
  Pro (`1d50:6089`), and rad1o (`1d50:cc15`);
- exact 128-bit serial discovery and selection;
- firmware, board, USB API version, and MCU serial queries;
- frequency, sample rate, LNA gain, VGA gain, RF amplifier, and antenna bias
  controls, including live setters while RX is active;
- one owned, pull-based RX stream with synchronous and executor-agnostic
  asynchronous APIs;
- WebUSB support on `wasm32-unknown-unknown`;
- direct conversion of HackRF interleaved signed 8-bit IQ into `Complex32`.

There is no resampling and no alternate raw/floating-point stream mode. The
configured rate is the complex sample rate produced by the radio. Each signed
8-bit component is normalized by `128.0` as it is copied to the caller's
`Complex32` slice.

## USB dependency and execution model

One-shot operations and receive operations implement `nusb::MaybeFuture`. On a
native target, call `.wait()` for blocking operation or `.await` the same value
in async code. WebUSB supports only the async form.

The receive stream owns endpoint `0x81` and a persistent queue of 16 transfers,
each 256 KiB (4 MiB total). Its steady-state `read` path does not acquire the
device lifecycle mutex and does not allocate per read. The stream is pull-based:
the application must keep reading while RX is enabled. At 20 Msamples/s the
queue represents about 105 ms of host-side buffering; at 10 Msamples/s it is
about 210 ms. HackRF transfers do not include sequence numbers, so
`StreamingStats` reports host completion failures and controlled discards but
cannot measure samples already lost inside the device.

Awaited native operations normally need one of `nusb`'s runtime integrations:

```sh
cargo check --features tokio
cargo check --features smol
```

Use the integration matching the application's executor. The default feature
set has no async-runtime dependency and supports the blocking native API.

## Configuration

`Config::builder()` and `Device::builder()` validate settings before USB is
opened or touched:

- frequency: 1 MHz through 6 GHz inclusive;
- complex sample rate: 2 MHz through 20 MHz inclusive;
- RX IF/LNA gain: 0 through 40 dB in 8 dB steps;
- baseband/VGA gain: 0 through 62 dB in 2 dB steps.

The defaults are 900 MHz, 10 Msamples/s, 8 dB LNA gain, 20 dB VGA gain, and both
the RF amplifier and antenna bias disabled. Whenever the sample rate is set,
the driver requests a baseband-filter bandwidth equal to the full sample rate;
firmware maps that request to a bandwidth supported by the installed radio.

`Device::config()` is the configuration last successfully sent by this driver,
not hardware readback. A complete validated `Config` can be applied with
`configure`, and individual settings can be changed while receiving.

## Synchronous API

```rust,no_run
use hackrf_nusb::{Complex32, Device, MaybeFuture};
use std::time::Duration;

fn main() -> hackrf_nusb::Result<()> {
    let mut device = Device::builder()
        .frequency_hz(100_000_000)
        .sample_rate_hz(10_000_000)
        .lna_gain_db(16)
        .vga_gain_db(20)
        .open()
        .wait()?;

    println!(
        "opened {} firmware={} serial={:?}",
        device.info().board_name(),
        device.info().firmware_version,
        device.info().serial.map(|serial| format!("{serial:032x}")),
    );

    let mut rx = device.rx_stream()?;
    rx.start().wait()?;
    device.set_frequency_hz(101_000_000).wait()?;

    let mut samples = [Complex32::default(); 4096];
    let count = rx
        .read(&mut samples, Some(Duration::from_secs(1)))
        .wait()?;

    let stats = rx.stop().wait()?;
    drop(rx);
    device.shutdown().wait()?;
    println!("read {count} samples; {stats:?}");
    Ok(())
}
```

Only one `RxStream` can be claimed per device. `stop` pauses RX while preserving
the async transfer queue for a later restart. A running stream makes explicit
device shutdown return `Busy`; a stopped stream may remain owned, but cannot be
restarted after shutdown.

## Asynchronous API

The same API can be awaited:

```rust,no_run
use futures_lite::future::block_on;
use hackrf_nusb::{Complex32, Device};

fn main() -> hackrf_nusb::Result<()> {
    block_on(async {
        let mut device = Device::builder()
            .frequency_hz(144_500_000)
            .sample_rate_hz(10_000_000)
            .open()
            .await?;

        let mut rx = device.rx_stream()?;
        rx.start().await?;
        let mut samples = [Complex32::default(); 4096];
        let count = rx.read(&mut samples, None).await?;
        let stats = rx.stop().await?;
        drop(rx);
        device.shutdown().await?;
        println!("read {count} samples; {stats:?}");
        Ok(())
    })
}
```

Waiting the first stream operation selects blocking USB; awaiting it selects
async USB. Do not mix the two styles on the same stream.

## Serials

HackRF firmware exposes a 32-hex-digit serial. Discovery parses all 128 bits into
`DeviceDescriptor::serial`, `DeviceInfo::serial` reports the firmware value, and
`Device::open_serial` / `DeviceBuilder::serial` select by the exact value. Format
it with `{serial:032x}` when leading zeroes must be shown.

## WebUSB

On `wasm32-unknown-unknown`, use the async API and request access during a browser
user gesture before opening:

```rust,ignore
Device::builder().serial(serial).request_permission().await?;
let device = Device::open_serial(serial).await?;
```

WebUSB requires a secure context, a supporting browser, and user permission.
The final application crate must enable the unstable `web-sys` WebUSB bindings.
This repository's `.cargo/config.toml` contains the required setting:

```toml
[target.wasm32-unknown-unknown]
rustflags = ["--cfg=web_sys_unstable_apis"]
```

Native endpoints can cancel retained transfers at stop. WebUSB cannot, so after
a restart the stream first drains and discards every transfer that was pending
at stop. `buffers_discarded_on_restart` reports that warm-up.

## Linux permissions and RF safety

Install the upstream HackRF udev rules, or equivalent rules for these IDs:

```udev
SUBSYSTEM=="usb", ATTR{idVendor}=="1d50", ATTR{idProduct}=="604b", MODE="0660", GROUP="plugdev", TAG+="uaccess"
SUBSYSTEM=="usb", ATTR{idVendor}=="1d50", ATTR{idProduct}=="6089", MODE="0660", GROUP="plugdev", TAG+="uaccess"
SUBSYSTEM=="usb", ATTR{idVendor}=="1d50", ATTR{idProduct}=="cc15", MODE="0660", GROUP="plugdev", TAG+="uaccess"
```

Reload rules and replug the device. Group names vary by distribution.

The antenna bias setting can place DC power on the RF port. It is disabled by
default. Prefer explicit `stop` and `shutdown` so cleanup errors are observable.
Native drops attempt receiver-off cleanup; WebUSB drops schedule best-effort
async cleanup, and neither drop path can report an error.

Examples and hardware tests are gated and do not touch USB unless explicitly
requested:

```sh
cargo run --example rx_sync -- --run
cargo run --example rx_sync -- --run --rx
cargo run --features smol --example rx_async -- --run
cargo run --features smol --example rx_async -- --run --rx
cargo test --test hardware --features smol -- --ignored --test-threads=1 --nocapture
```

## License

Licensed under either Apache-2.0 or MIT, at your option.
