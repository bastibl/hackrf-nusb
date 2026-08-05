//! Validated high-level HackRF receiver configuration.

use crate::errors::{Error, Result};

const DEFAULT_FREQUENCY_HZ: u64 = 900_000_000;
const DEFAULT_SAMPLE_RATE_HZ: u32 = 10_000_000;
const DEFAULT_LNA_GAIN_DB: u8 = 8;
const DEFAULT_VGA_GAIN_DB: u8 = 20;

/// Validated HackRF RX configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Config {
    frequency_hz: u64,
    sample_rate_hz: u32,
    lna_gain_db: u8,
    vga_gain_db: u8,
    amp_enabled: bool,
    bias_tee_enabled: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            frequency_hz: DEFAULT_FREQUENCY_HZ,
            sample_rate_hz: DEFAULT_SAMPLE_RATE_HZ,
            lna_gain_db: DEFAULT_LNA_GAIN_DB,
            vga_gain_db: DEFAULT_VGA_GAIN_DB,
            amp_enabled: false,
            bias_tee_enabled: false,
        }
    }
}

impl Config {
    /// Start building a validated receiver configuration.
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }

    /// Tuned center frequency in Hz.
    pub const fn frequency_hz(&self) -> u64 {
        self.frequency_hz
    }

    /// Complex IQ sample rate in Hz.
    pub const fn sample_rate_hz(&self) -> u32 {
        self.sample_rate_hz
    }

    /// MAX2837 RX IF/LNA gain in dB.
    pub const fn lna_gain_db(&self) -> u8 {
        self.lna_gain_db
    }

    /// MAX2837 baseband/VGA gain in dB.
    pub const fn vga_gain_db(&self) -> u8 {
        self.vga_gain_db
    }

    /// Whether the RF amplifier is enabled.
    pub const fn amp_enabled(&self) -> bool {
        self.amp_enabled
    }

    /// Whether antenna-port bias power is requested.
    pub const fn bias_tee_enabled(&self) -> bool {
        self.bias_tee_enabled
    }

    pub(crate) fn set_frequency_hz_internal(&mut self, value: u64) {
        self.frequency_hz = value;
    }

    pub(crate) fn set_sample_rate_hz_internal(&mut self, value: u32) {
        self.sample_rate_hz = value;
    }

    pub(crate) fn set_lna_gain_db_internal(&mut self, value: u8) {
        self.lna_gain_db = value;
    }

    pub(crate) fn set_vga_gain_db_internal(&mut self, value: u8) {
        self.vga_gain_db = value;
    }

    pub(crate) fn set_amp_enabled_internal(&mut self, value: bool) {
        self.amp_enabled = value;
    }

    pub(crate) fn set_bias_tee_enabled_internal(&mut self, value: bool) {
        self.bias_tee_enabled = value;
    }
}

/// Builder for [`Config`].
#[derive(Clone, Debug, Default)]
pub struct ConfigBuilder {
    config: Config,
}

impl ConfigBuilder {
    /// Set the center frequency in Hz.
    pub fn frequency_hz(mut self, value: u64) -> Self {
        self.config.frequency_hz = value;
        self
    }

    /// Set the complex IQ sample rate in Hz.
    pub fn sample_rate_hz(mut self, value: u32) -> Self {
        self.config.sample_rate_hz = value;
        self
    }

    /// Set RX IF/LNA gain in dB.
    pub fn lna_gain_db(mut self, value: u8) -> Self {
        self.config.lna_gain_db = value;
        self
    }

    /// Set baseband/VGA gain in dB.
    pub fn vga_gain_db(mut self, value: u8) -> Self {
        self.config.vga_gain_db = value;
        self
    }

    /// Enable or disable the RF amplifier.
    pub fn amp_enable(mut self, enabled: bool) -> Self {
        self.config.amp_enabled = enabled;
        self
    }

    /// Enable or disable antenna-port bias power.
    pub fn bias_tee(mut self, enabled: bool) -> Self {
        self.config.bias_tee_enabled = enabled;
        self
    }

    /// Validate and return the configuration.
    pub fn build(self) -> Result<Config> {
        validate_config(&self.config)?;
        Ok(self.config)
    }
}

pub(crate) fn validate_config(config: &Config) -> Result<()> {
    validate_frequency(config.frequency_hz)?;
    validate_sample_rate(config.sample_rate_hz)?;
    validate_lna_gain(config.lna_gain_db)?;
    validate_vga_gain(config.vga_gain_db)?;
    Ok(())
}

pub(crate) fn validate_frequency(value: u64) -> Result<()> {
    if !(1_000_000..=6_000_000_000).contains(&value) {
        return Err(Error::invalid_config(
            "frequency_hz",
            "must be between 1 MHz and 6 GHz inclusive",
        ));
    }
    Ok(())
}

pub(crate) fn validate_sample_rate(value: u32) -> Result<()> {
    if !(2_000_000..=20_000_000).contains(&value) {
        return Err(Error::invalid_config(
            "sample_rate_hz",
            "must be between 2 MHz and 20 MHz inclusive",
        ));
    }
    Ok(())
}

pub(crate) fn validate_lna_gain(value: u8) -> Result<()> {
    if value > 40 || !value.is_multiple_of(8) {
        return Err(Error::invalid_config(
            "lna_gain_db",
            "must be 0 through 40 dB in 8 dB steps",
        ));
    }
    Ok(())
}

pub(crate) fn validate_vga_gain(value: u8) -> Result<()> {
    if value > 62 || !value.is_multiple_of(2) {
        return Err(Error::invalid_config(
            "vga_gain_db",
            "must be 0 through 62 dB in 2 dB steps",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_hackrf_transfer() {
        let config = Config::default();
        assert_eq!(config.frequency_hz(), 900_000_000);
        assert_eq!(config.sample_rate_hz(), 10_000_000);
        assert_eq!(config.lna_gain_db(), 8);
        assert_eq!(config.vga_gain_db(), 20);
        assert!(!config.amp_enabled());
        assert!(!config.bias_tee_enabled());
    }

    #[test]
    fn validates_documented_ranges_and_steps() {
        assert!(Config::builder().frequency_hz(999_999).build().is_err());
        assert!(
            Config::builder()
                .frequency_hz(6_000_000_001)
                .build()
                .is_err()
        );
        assert!(Config::builder().sample_rate_hz(1_999_999).build().is_err());
        assert!(
            Config::builder()
                .sample_rate_hz(20_000_001)
                .build()
                .is_err()
        );
        assert!(Config::builder().lna_gain_db(7).build().is_err());
        assert!(Config::builder().vga_gain_db(3).build().is_err());
        assert!(
            Config::builder()
                .lna_gain_db(40)
                .vga_gain_db(62)
                .build()
                .is_ok()
        );
    }
}
