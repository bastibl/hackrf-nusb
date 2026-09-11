//! Explicitly gated tests for a connected HackRF.

#![cfg(not(target_arch = "wasm32"))]

use std::sync::Mutex;
use std::time::Duration;

use hackrf_nusb::{Complex32, Config, Device, MaybeFuture};

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
    let mut samples = [Complex32::default(); 4096];
    rx.start().wait()?;
    let count = rx.read(&mut samples, Some(Duration::from_secs(2))).wait()?;
    assert!(count > 0, "RX timed out without samples");
    let live_config = Config::builder()
        .frequency_hz(101_000_000)
        .sample_rate_hz(8_000_000)
        .lna_gain_db(24)
        .vga_gain_db(28)
        .tx_vga_gain_db(0)
        .amp_enable(false)
        .bias_tee(false)
        .build()?;
    device.configure(&live_config).wait()?;
    assert_eq!(device.config(), &live_config);
    let count = rx.read(&mut samples, Some(Duration::from_secs(2))).wait()?;
    assert!(count > 0, "RX timed out after live reconfiguration");
    println!("samples={count}, stats={:?}", rx.stop().wait()?);
    rx.close().wait()?;
    device.shutdown().wait()?;
    Ok(())
}

#[test]
#[ignore = "requires a connected HackRF and briefly enables low-gain TX"]
fn sync_explicit_rx_tx_rx() -> hackrf_nusb::Result<()> {
    let _guard = HARDWARE.lock().unwrap();
    let mut device = Device::builder()
        .frequency_hz(2_450_000_000)
        .sample_rate_hz(10_000_000)
        .lna_gain_db(0)
        .vga_gain_db(0)
        .tx_vga_gain_db(0)
        .amp_enable(false)
        .bias_tee(false)
        .open()
        .wait()?;

    let mut rx = device.rx_stream()?;
    let mut tx = device.tx_stream()?;

    let mut received = [Complex32::default(); 4096];
    rx.start().wait()?;
    assert!(
        rx.read(&mut received, Some(Duration::from_secs(3)))
            .wait()?
            > 0,
        "initial RX timed out"
    );

    println!("rx={:?}", rx.stop().wait()?);
    tx.start().wait()?;
    let zeros = [Complex32::default(); 4096];
    assert_eq!(
        tx.write(&zeros, Some(Duration::from_secs(3)), false)
            .wait()?,
        zeros.len()
    );
    println!("tx={:?}", tx.stop().wait()?);
    rx.start().wait()?;
    assert!(
        rx.read(&mut received, Some(Duration::from_secs(5)))
            .wait()?
            > 0,
        "RX did not start after explicit TX stop"
    );

    println!("rx={:?}", rx.stop().wait()?);
    rx.close().wait()?;
    tx.close().wait()?;
    device.shutdown().wait()?;
    Ok(())
}

#[cfg(any(feature = "smol", feature = "tokio"))]
#[test]
#[ignore = "requires a connected HackRF and briefly enables low-gain TX"]
#[allow(clippy::await_holding_lock)]
fn async_explicit_rx_tx_rx() -> hackrf_nusb::Result<()> {
    let _guard = HARDWARE.lock().unwrap();
    futures_lite::future::block_on(async {
        let mut device = Device::builder()
            .frequency_hz(2_450_000_000)
            .sample_rate_hz(10_000_000)
            .lna_gain_db(0)
            .vga_gain_db(0)
            .tx_vga_gain_db(0)
            .amp_enable(false)
            .bias_tee(false)
            .open()
            .await?;
        let mut rx = device.rx_stream()?;
        let mut tx = device.tx_stream()?;
        let mut received = [Complex32::default(); 4096];

        rx.start().await?;
        assert!(
            rx.read(&mut received, None).await? > 0,
            "initial RX timed out"
        );
        println!("rx={:?}", rx.stop().await?);

        tx.start().await?;
        let zeros = [Complex32::default(); 4096];
        assert_eq!(tx.write(&zeros, None, true).await?, zeros.len());
        println!("tx={:?}", tx.stop().await?);

        rx.start().await?;
        assert!(
            rx.read(&mut received, None).await? > 0,
            "RX did not resume after TX stop"
        );

        println!("rx={:?}", rx.stop().await?);
        rx.close().await?;
        tx.close().await?;
        device.shutdown().await?;
        Ok(())
    })
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
        let mut samples = [Complex32::default(); 4096];
        rx.start().await?;
        let count = rx.read(&mut samples, None).await?;
        assert!(count > 0, "async RX completed without samples");
        let live_config = Config::builder()
            .frequency_hz(101_000_000)
            .sample_rate_hz(8_000_000)
            .lna_gain_db(24)
            .vga_gain_db(28)
            .tx_vga_gain_db(0)
            .amp_enable(false)
            .bias_tee(false)
            .build()?;
        device.configure(&live_config).await?;
        assert_eq!(device.config(), &live_config);
        let count = rx.read(&mut samples, None).await?;
        assert!(count > 0, "async RX completed without live reconfiguration");
        println!("samples={count}, stats={:?}", rx.stop().await?);
        rx.close().await?;
        device.shutdown().await?;
        Ok(())
    })
}
