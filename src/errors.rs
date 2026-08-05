//! Errors returned by the HackRF driver.

use core::fmt;

/// Error returned by a HackRF operation.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// A receiver configuration value failed validation.
    InvalidConfig {
        /// Name of the invalid field.
        field: &'static str,
        /// Reason the value is invalid.
        reason: &'static str,
    },
    /// No matching HackRF device was found.
    DeviceNotFound,
    /// The logical device session has been shut down.
    DeviceClosed,
    /// The device or USB resource is already in use.
    Busy,
    /// The requested operation is unsupported.
    Unsupported,
    /// The receive stream is stopped or unusable.
    StreamClosed {
        /// Reason the stream is unavailable.
        reason: &'static str,
    },
    /// The USB backend returned an error outside an individual transfer.
    Usb(nusb::Error),
    /// An individual USB transfer failed.
    Transfer(nusb::transfer::TransferError),
    /// A driver operation failed with a more specific source error.
    Operation {
        /// Operation being performed.
        operation: &'static str,
        /// Underlying error.
        source: Box<Error>,
    },
    /// The device violated the expected HackRF USB protocol.
    Protocol {
        /// Operation that failed.
        operation: &'static str,
        /// Unexpected response or state.
        reason: &'static str,
    },
}

/// Stable high-level error category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Invalid configuration.
    InvalidConfig,
    /// No matching device.
    NotFound,
    /// The logical device is closed.
    DeviceClosed,
    /// The physical USB device disconnected.
    DeviceDisconnected,
    /// Device or resource busy.
    Busy,
    /// Unsupported operation.
    Unsupported,
    /// USB or transfer failure.
    Usb,
    /// Stream stopped or closed.
    StreamClosed,
    /// Protocol or other driver failure.
    Other,
}

impl Error {
    pub(crate) const fn invalid_config(field: &'static str, reason: &'static str) -> Self {
        Self::InvalidConfig { field, reason }
    }

    pub(crate) const fn stream_closed(reason: &'static str) -> Self {
        Self::StreamClosed { reason }
    }

    pub(crate) const fn protocol(operation: &'static str, reason: &'static str) -> Self {
        Self::Protocol { operation, reason }
    }

    pub(crate) fn at(self, operation: &'static str) -> Self {
        Self::Operation {
            operation,
            source: Box::new(self),
        }
    }

    /// Return a backend-independent error category.
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::InvalidConfig { .. } => ErrorKind::InvalidConfig,
            Self::DeviceNotFound => ErrorKind::NotFound,
            Self::DeviceClosed => ErrorKind::DeviceClosed,
            Self::Busy => ErrorKind::Busy,
            Self::Unsupported => ErrorKind::Unsupported,
            Self::StreamClosed { .. } => ErrorKind::StreamClosed,
            Self::Usb(error) => match error.kind() {
                nusb::ErrorKind::Disconnected => ErrorKind::DeviceDisconnected,
                nusb::ErrorKind::Busy => ErrorKind::Busy,
                nusb::ErrorKind::NotFound => ErrorKind::NotFound,
                nusb::ErrorKind::Unsupported => ErrorKind::Unsupported,
                _ => ErrorKind::Usb,
            },
            Self::Transfer(nusb::transfer::TransferError::Disconnected) => {
                ErrorKind::DeviceDisconnected
            }
            Self::Transfer(_) => ErrorKind::Usb,
            Self::Operation { source, .. } => source.kind(),
            Self::Protocol { .. } => ErrorKind::Other,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig { field, reason } => {
                write!(f, "invalid configuration for {field}: {reason}")
            }
            Self::DeviceNotFound => f.write_str("no matching HackRF device found"),
            Self::DeviceClosed => f.write_str("HackRF device is closed"),
            Self::Busy => f.write_str("HackRF device or USB resource is busy"),
            Self::Unsupported => f.write_str("operation is unsupported"),
            Self::StreamClosed { reason } => write!(f, "stream closed: {reason}"),
            Self::Usb(error) => write!(f, "USB error: {error}"),
            Self::Transfer(error) => write!(f, "USB transfer error: {error}"),
            Self::Operation { operation, source } => write!(f, "{operation}: {source}"),
            Self::Protocol { operation, reason } => write!(f, "{operation}: {reason}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Usb(error) => Some(error),
            Self::Transfer(error) => Some(error),
            Self::Operation { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<nusb::Error> for Error {
    fn from(value: nusb::Error) -> Self {
        Self::Usb(value)
    }
}

impl From<nusb::transfer::TransferError> for Error {
    fn from(value: nusb::transfer::TransferError) -> Self {
        Self::Transfer(value)
    }
}

/// Crate result alias using [`Error`].
pub type Result<T> = core::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_context_preserves_error_kind() {
        let error = Error::from(nusb::transfer::TransferError::Fault).at("reading samples");
        assert_eq!(error.kind(), ErrorKind::Usb);
        assert!(error.to_string().starts_with("reading samples:"));
    }
}
