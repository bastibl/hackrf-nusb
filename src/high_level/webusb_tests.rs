//! Browser tests use production streams, controller, and spawn_local cleanup.
//! Only USB control and endpoint completions are faked; no hardware is opened.
use super::*;
use crate::usb::TestIo;
use futures_lite::future::{poll_once, yield_now};
use std::sync::Arc;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

async fn cleanup_finished(device: &Device) {
    for _ in 0..100 {
        if Arc::strong_count(&device.radio) == 1 {
            return;
        }
        yield_now().await;
    }
    panic!("background cleanup did not release the controller");
}

#[wasm_bindgen_test]
async fn dormant_close_and_drop_allow_replacement() {
    let (mut device, _) = test_device();
    assert_eq!(
        device.rx_stream().unwrap().close().await.unwrap(),
        StreamingStats::default()
    );
    drop(device.rx_stream().unwrap());
    assert!(matches!(device.shutdown().await, Err(Error::Busy)));
    cleanup_finished(&device).await;
    device.rx_stream().unwrap().close().await.unwrap();
    device.shutdown().await.unwrap();
}

#[wasm_bindgen_test]
async fn stopped_rx_can_resume_but_closed_queue_cannot_be_replaced() {
    let (mut device, _) = test_device();
    let mut rx = device.rx_stream().unwrap();
    rx.start().await.unwrap();
    rx.stop().await.unwrap();
    let mut tx = device.tx_stream().unwrap();
    tx.start().await.unwrap();
    tx.stop().await.unwrap();
    rx.start().await.unwrap();
    // Closing inactive TX must not interfere with active RX.
    tx.close().await.unwrap();
    rx.read(&mut [Complex32::default(); 1], None).await.unwrap();
    rx.close().await.unwrap();
    assert!(matches!(
        device.rx_stream(),
        Err(Error::StreamClosed { .. })
    ));
    device.shutdown().await.unwrap();
}

#[wasm_bindgen_test]
async fn rx_drop_holds_claim_until_off_attempt_finishes() {
    for fail_off in [false, true] {
        let (mut device, io) = test_device();
        let mut rx = device.rx_stream().unwrap();
        rx.start().await.unwrap();
        {
            let mut io = io.lock().unwrap();
            io.pause_off = true;
            io.fail_off = fail_off;
        }
        drop(rx); // Exercise the actual wasm Drop implementation.
        yield_now().await;
        assert!(matches!(device.shutdown().await, Err(Error::Busy)));
        TestIo::resume(&io);
        cleanup_finished(&device).await;
        if fail_off {
            assert!(matches!(device.tx_stream(), Err(Error::Busy)));
        } else {
            assert!(matches!(
                device.rx_stream(),
                Err(Error::StreamClosed { .. })
            ));
        }
        io.lock().unwrap().fail_off = false;
        device.shutdown().await.unwrap();
    }
}

#[wasm_bindgen_test]
async fn tx_drop_retains_transport_and_claim_through_drain_and_off() {
    let (mut device, io) = test_device();
    let mut tx = device.tx_stream().unwrap();
    tx.start().await.unwrap();
    tx.write(&[Complex32::default(); 1], None, false)
        .await
        .unwrap();
    io.lock().unwrap().pause_drain = true;
    drop(tx);
    yield_now().await;
    assert!(matches!(device.shutdown().await, Err(Error::Busy)));
    assert!(matches!(device.tx_stream(), Err(Error::Busy)));
    TestIo::resume(&io);
    // resume wakes the drain; it will next suspend at receiver-off.
    io.lock().unwrap().pause_off = true;
    yield_now().await;
    assert!(matches!(device.shutdown().await, Err(Error::Busy)));
    TestIo::resume(&io);
    cleanup_finished(&device).await;
    let mut replacement = device.tx_stream().unwrap();
    replacement.start().await.unwrap();
    replacement.close().await.unwrap();
    device.shutdown().await.unwrap();
}

#[wasm_bindgen_test]
async fn cancelled_rx_close_uses_background_fallback() {
    let (mut device, io) = test_device();
    let mut rx = device.rx_stream().unwrap();
    rx.start().await.unwrap();
    io.lock().unwrap().pause_off = true;
    let mut close = Box::pin(rx.close().into_future());
    assert!(poll_once(close.as_mut()).await.is_none());
    drop(close);
    yield_now().await;
    assert!(matches!(device.shutdown().await, Err(Error::Busy)));
    TestIo::resume(&io);
    cleanup_finished(&device).await;
    assert!(matches!(
        device.rx_stream(),
        Err(Error::StreamClosed { .. })
    ));
    device.shutdown().await.unwrap();
}

#[wasm_bindgen_test]
async fn surviving_stream_and_final_owner_cleanup_work_in_browser() {
    let (device, io) = test_device();
    let mut rx = device.rx_stream().unwrap();
    drop(device);
    assert!(io.lock().unwrap().events.is_empty());
    rx.start().await.unwrap();
    io.lock().unwrap().events.clear();
    rx.close().await.unwrap();
    for _ in 0..100 {
        if io.lock().unwrap().events.len() == 2 {
            break;
        }
        yield_now().await;
    }
    assert_eq!(io.lock().unwrap().events, ["control:0", "control:0"]);
}
