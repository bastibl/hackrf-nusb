//! HackRF USB vendor requests and transceiver modes.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum VendorRequest {
    SetTransceiverMode = 1,
    SampleRateSet = 6,
    BasebandFilterBandwidthSet = 7,
    BoardIdRead = 14,
    VersionStringRead = 15,
    SetFreq = 16,
    AmpEnable = 17,
    BoardPartIdSerialNoRead = 18,
    SetLnaGain = 19,
    SetVgaGain = 20,
    AntennaEnable = 23,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub(crate) enum TransceiverMode {
    Off = 0,
    Receive = 1,
}
