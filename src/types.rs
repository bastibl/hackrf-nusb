//! Public HackRF metadata types.

/// HackRF board identity reported by firmware.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum BoardId {
    /// Pre-production Jellybean board.
    Jellybean,
    /// HackRF Jawbreaker beta board.
    Jawbreaker,
    /// Original HackRF One board.
    HackRfOne,
    /// rad1o badge variant.
    Rad1o,
    /// HackRF One revision 9 or later.
    HackRfOneR9,
    /// HackRF Pro / Praline platform.
    HackRfPro,
    /// Firmware explicitly reported an unrecognized board.
    Unrecognized,
    /// Firmware has not detected a board.
    Undetected,
    /// A future firmware board ID not known by this crate.
    Unknown(u8),
}

impl BoardId {
    pub(crate) const fn from_raw(value: u8) -> Self {
        match value {
            0 => Self::Jellybean,
            1 => Self::Jawbreaker,
            2 => Self::HackRfOne,
            3 => Self::Rad1o,
            4 => Self::HackRfOneR9,
            5 => Self::HackRfPro,
            0xfe => Self::Unrecognized,
            0xff => Self::Undetected,
            other => Self::Unknown(other),
        }
    }

    /// Human-readable board name following libhackrf terminology.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Jellybean => "Jellybean",
            Self::Jawbreaker => "Jawbreaker",
            Self::HackRfOne | Self::HackRfOneR9 => "HackRF One",
            Self::Rad1o => "rad1o",
            Self::HackRfPro => "HackRF Pro",
            Self::Unrecognized => "unrecognized",
            Self::Undetected => "undetected",
            Self::Unknown(_) => "unknown",
        }
    }
}

/// Device metadata collected while opening a HackRF.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeviceInfo {
    /// Firmware-reported board identity.
    pub board_id: BoardId,
    /// Firmware version string.
    pub firmware_version: String,
    /// Raw HackRF USB API version from the USB `bcdDevice` field.
    ///
    /// The high byte is the major version and the low byte is the minor version,
    /// matching libhackrf's hexadecimal `MM.mm` convention.
    pub usb_api_version: u16,
    /// Full 128-bit MCU serial number, when it is nonzero.
    pub serial: Option<u128>,
}

impl DeviceInfo {
    /// Human-readable board name.
    pub const fn board_name(&self) -> &'static str {
        self.board_id.name()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PartIdSerial {
    pub(crate) part_id: [u32; 2],
    pub(crate) serial: [u32; 4],
}

impl PartIdSerial {
    pub(crate) const fn serial_u128(self) -> Option<u128> {
        let [a, b, c, d] = self.serial;
        let value = ((a as u128) << 96) | ((b as u128) << 64) | ((c as u128) << 32) | d as u128;
        if value == 0 { None } else { Some(value) }
    }
}
