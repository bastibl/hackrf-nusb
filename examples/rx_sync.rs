//! Synchronous HackRF receive example.
//!
//! The default invocation prints usage and exits without touching USB. Pass
//! `--run` only when a HackRF is connected and USB permissions are configured.

use std::env;
use std::time::Duration;

use hackrf_nusb::{Complex32, Device, MaybeFuture};

const EXAMPLE_FREQ_HZ: u64 = 100_000_000;
const EXAMPLE_SAMPLE_RATE_HZ: u32 = 10_000_000;

fn main() -> hackrf_nusb::Result<()> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if !args.iter().any(|arg| arg == "--run") {
        print_usage();
        return Ok(());
    }

    let run_rx = args.iter().any(|arg| arg == "--rx");
    let mut device = Device::builder()
        .frequency_hz(EXAMPLE_FREQ_HZ)
        .sample_rate_hz(EXAMPLE_SAMPLE_RATE_HZ)
        .lna_gain_db(16)
        .vga_gain_db(20)
        .bias_tee(false)
        .open()
        .wait()?;

    println!(
        "opened {} firmware={} serial={}",
        device.info().board_name(),
        device.info().firmware_version,
        device
            .info()
            .serial
            .map_or_else(|| "unknown".to_owned(), |serial| format!("{serial:032x}")),
    );

    if run_rx {
        let mut rx = device.rx_stream()?;
        rx.start().wait()?;
        let mut samples = [Complex32::default(); 4096];
        let count = rx.read(&mut samples, Some(Duration::from_secs(1))).wait()?;
        let stats = rx.stop().wait()?;
        drop(rx);
        println!(
            "read {count} samples; first={:?}; {stats:?}",
            samples.first()
        );
    } else {
        println!("RX not started; add --rx for a short receive smoke test.");
    }

    device.shutdown().wait()?;
    Ok(())
}

fn print_usage() {
    eprintln!(
        "This example opens and configures real HackRF hardware.\n\
         It is gated to avoid accidental USB access during checks/tests.\n\n\
         Run:\n\
           cargo run --example rx_sync -- --run       # open/query/configure\n\
           cargo run --example rx_sync -- --run --rx  # additionally read samples"
    );
}
