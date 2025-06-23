use std::io::Error as IoError;
use std::num::ParseIntError;
use thiserror::Error;

#[derive(Error, Debug)]
pub(super) enum PciError {
    #[error("illegal pci address {0}")]
    IllegalPciAddress(String),
    #[error("parse int error {field:?} {source:?}")]
    ParseIntError {
        field: String,
        source: ParseIntError,
    },
    #[error("io error")]
    IoError(#[from] IoError),
}
