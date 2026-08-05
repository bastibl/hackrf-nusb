//! USB discovery and exact serial-number selection.

use nusb::MaybeFuture;

use crate::errors::{Error, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct UsbDeviceId {
    pub(crate) vid: u16,
    pub(crate) pid: u16,
    pub(crate) description: &'static str,
}

pub(crate) const USB_DEVICE_IDS: &[UsbDeviceId] = &[
    UsbDeviceId {
        vid: 0x1d50,
        pid: 0x604b,
        description: "HackRF Jawbreaker",
    },
    UsbDeviceId {
        vid: 0x1d50,
        pid: 0x6089,
        description: "HackRF One / HackRF Pro",
    },
    UsbDeviceId {
        vid: 0x1d50,
        pid: 0xcc15,
        description: "rad1o",
    },
];

/// HackRF information available from USB discovery without claiming it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeviceDescriptor {
    /// USB vendor ID.
    pub vid: u16,
    /// USB product ID.
    pub pid: u16,
    /// Static description from the known HackRF USB ID table.
    pub description: &'static str,
    /// Parsed full 128-bit USB serial, if present and valid.
    pub serial: Option<u128>,
    /// USB product string, when available.
    pub product_string: Option<String>,
    /// Raw HackRF USB API version from `bcdDevice`.
    pub usb_api_version: u16,
}

impl DeviceDescriptor {
    pub(crate) fn from_nusb(info: &nusb::DeviceInfo) -> Option<Self> {
        let id = find_usb_device_id(info.vendor_id(), info.product_id())?;
        Some(Self {
            vid: id.vid,
            pid: id.pid,
            description: id.description,
            serial: info.serial_number().and_then(parse_serial),
            product_string: info.product_string().map(str::to_owned),
            usb_api_version: info.device_version(),
        })
    }
}

pub(crate) fn find_usb_device_id(vid: u16, pid: u16) -> Option<UsbDeviceId> {
    USB_DEVICE_IDS
        .iter()
        .copied()
        .find(|candidate| candidate.vid == vid && candidate.pid == pid)
}

pub(crate) fn list_devices() -> impl MaybeFuture<Output = Result<Vec<DeviceDescriptor>>> {
    nusb::list_devices().map(|devices| {
        Ok(devices
            .map_err(Error::from)?
            .filter_map(|info| DeviceDescriptor::from_nusb(&info))
            .collect())
    })
}

pub(crate) fn select_device(
    serial: Option<u128>,
) -> impl MaybeFuture<Output = Result<nusb::DeviceInfo>> {
    nusb::list_devices().map(move |devices| {
        devices
            .map_err(Error::from)?
            .find(|info| matches_device(info, serial))
            .ok_or(Error::DeviceNotFound)
    })
}

fn matches_device(info: &nusb::DeviceInfo, serial: Option<u128>) -> bool {
    find_usb_device_id(info.vendor_id(), info.product_id()).is_some()
        && serial.is_none_or(|wanted| info.serial_number().and_then(parse_serial) == Some(wanted))
}

/// Parse the 32 hexadecimal characters emitted by HackRF firmware.
pub(crate) fn parse_serial(value: &str) -> Option<u128> {
    let value = value.trim();
    if value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    u128::from_str_radix(value, 16).ok()
}

#[cfg(any(target_arch = "wasm32", test))]
pub(crate) fn format_serial(value: u128) -> String {
    format!("{value:032x}")
}

#[cfg(target_arch = "wasm32")]
async fn request_nusb_device(serial: Option<u128>) -> Result<Option<nusb::DeviceInfo>> {
    let selectors = USB_DEVICE_IDS
        .iter()
        .map(|id| {
            let selector = nusb::DeviceSelector::all().with_vid_pid(id.vid, id.pid);
            match serial {
                Some(serial) => selector.with_serial_number(format_serial(serial)),
                None => selector,
            }
        })
        .collect::<Vec<_>>();
    nusb::request_device(&selectors).await.map_err(Error::from)
}

#[cfg(target_arch = "wasm32")]
pub(crate) async fn request_device_permission(serial: Option<u128>) -> Result<()> {
    for info in nusb::list_devices().await.map_err(Error::from)? {
        if matches_device(&info, serial) {
            return Ok(());
        }
    }

    request_nusb_device(serial)
        .await?
        .filter(|info| matches_device(info, serial))
        .map(|_| ())
        .ok_or(Error::DeviceNotFound)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serial_round_trip_preserves_leading_zeroes() {
        let serial = 0x0011_2233_4455_6677_8899_aabb_ccdd_eeff;
        let text = format_serial(serial);
        assert_eq!(text, "00112233445566778899aabbccddeeff");
        assert_eq!(parse_serial(&text), Some(serial));
        assert_eq!(parse_serial(&text.to_uppercase()), Some(serial));
    }

    #[test]
    fn serial_parser_requires_exact_firmware_shape() {
        assert_eq!(parse_serial("1234"), None);
        assert_eq!(parse_serial("00112233445566778899aabbccddeefg"), None);
        assert_eq!(
            parse_serial(" 00112233445566778899aabbccddeeff "),
            Some(0x0011_2233_4455_6677_8899_aabb_ccdd_eeff)
        );
    }

    #[test]
    fn all_libhackrf_ids_are_known() {
        for pid in [0x604b, 0x6089, 0xcc15] {
            assert!(find_usb_device_id(0x1d50, pid).is_some());
        }
    }
}
