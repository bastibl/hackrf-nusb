//! Explicitly gated tests for a connected HackRF.

use std::sync::Mutex;
use std::time::Duration;

use hackrf_nusb::{Complex32, Device, MaybeFuture};

static HARDWARE: Mutex<()> = Mutex::new(());

#[test]
#[ignore = "requires a connected HackRF and USB permissions"]
fn discovery_finds_a_hackrf() -> hackrf_nusb::Result<()> {
    let _guard = HARDWARE.lock().unwrap();
    let devices = Device::list().wait()?;
    assert!(!devices.is_empty(), "no supported HackRF was discovered");
    println!("discovered: {devices:#?}");
    Ok(())
}

#[test]
#[ignore = "requires a connected HackRF and changes receiver configuration"]
fn sync_query_configure_and_receive() -> hackrf_nusb::Result<()> {
    let _guard = HARDWARE.lock().unwrap();
    let mut device = Device::builder()
        .frequency_hz(100_000_000)
        .sample_rate_hz(10_000_000)
        .lna_gain_db(16)
        .vga_gain_db(20)
        .bias_tee(false)
        .open()
        .wait()?;
    println!("device: {:#?}", device.info());

    let mut rx = device.rx_stream()?;
    rx.start().wait()?;
    let mut samples = [Complex32::default(); 4096];
    let count = rx.read(&mut samples, Some(Duration::from_secs(2))).wait()?;
    assert!(count > 0, "RX timed out without samples");
    println!("samples={count}, stats={:?}", rx.stop().wait()?);
    drop(rx);
    device.shutdown().wait()?;
    Ok(())
}

#[cfg(any(feature = "smol", feature = "tokio"))]
#[test]
#[ignore = "requires a connected HackRF and changes receiver configuration"]
#[allow(clippy::await_holding_lock)]
fn async_query_configure_and_receive() -> hackrf_nusb::Result<()> {
    let _guard = HARDWARE.lock().unwrap();
    futures_lite::future::block_on(async {
        let mut device = Device::builder()
            .frequency_hz(100_000_000)
            .sample_rate_hz(10_000_000)
            .lna_gain_db(16)
            .vga_gain_db(20)
            .bias_tee(false)
            .open()
            .await?;
        println!("device: {:#?}", device.info());

        let mut rx = device.rx_stream()?;
        rx.start().await?;
        let mut samples = [Complex32::default(); 4096];
        let count = rx.read(&mut samples, None).await?;
        assert!(count > 0, "async RX completed without samples");
        println!("samples={count}, stats={:?}", rx.stop().await?);
        drop(rx);
        device.shutdown().await?;
        Ok(())
    })
}
