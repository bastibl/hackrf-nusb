use hackrf_nusb::{BoardId, Config, ErrorKind, MAX_COMPLEX_SAMPLES_PER_TRANSFER};
#[cfg(not(target_arch = "wasm32"))]
use hackrf_nusb::{Device, DeviceBuilder, StreamingStats, TxStream, TxStreamingStats};

#[test]
fn documented_defaults_and_boundaries_are_public() {
    let config = Config::default();
    assert_eq!(config.frequency_hz(), 900_000_000);
    assert_eq!(config.sample_rate_hz(), 10_000_000);
    assert_eq!(config.lna_gain_db(), 8);
    assert_eq!(config.vga_gain_db(), 20);
    assert_eq!(config.tx_vga_gain_db(), 0);
    assert!(!config.amp_enabled());
    assert!(!config.bias_tee_enabled());

    assert!(
        Config::builder()
            .frequency_hz(1_000_000)
            .sample_rate_hz(2_000_000)
            .lna_gain_db(0)
            .vga_gain_db(0)
            .build()
            .is_ok()
    );
    assert!(
        Config::builder()
            .frequency_hz(6_000_000_000)
            .sample_rate_hz(20_000_000)
            .lna_gain_db(40)
            .vga_gain_db(62)
            .tx_vga_gain_db(47)
            .build()
            .is_ok()
    );
}

#[test]
fn invalid_config_has_a_stable_category() {
    let error = Config::builder().lna_gain_db(7).build().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidConfig);
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn main_public_types_remain_send_on_native() {
    fn assert_send<T: Send>() {}

    assert_send::<Config>();
    assert_send::<Device>();
    assert_send::<DeviceBuilder>();
    assert_send::<StreamingStats>();
    assert_send::<TxStream>();
    assert_send::<TxStreamingStats>();
}

#[test]
fn public_constants_and_board_names_are_usable() {
    assert_eq!(MAX_COMPLEX_SAMPLES_PER_TRANSFER, 131_072);
    assert_eq!(BoardId::HackRfOne.name(), "HackRF One");
    assert_eq!(BoardId::HackRfPro.name(), "HackRF Pro");
}
