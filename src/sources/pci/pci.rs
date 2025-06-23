use super::errors::PciError;

#[derive(Debug)]
pub(super) struct PciDevice {
    pub(super) address: PciAddress,
    pub(super) vendor: u32,
    pub(super) device: u32,
    pub(super) class: u32,
    pub(super) current_link_speed: Option<String>,
    pub(super) current_link_width: Option<String>,
    pub(super) max_link_speed: Option<String>,
    pub(super) max_link_width: Option<String>,
}

impl PciDevice {
    pub(super) fn new(address: PciAddress, vendor: u32, device: u32, class: u32) -> Self {
        Self {
            address,
            vendor,
            device,
            class,
            current_link_speed: None,
            current_link_width: None,
            max_link_speed: None,
            max_link_width: None,
        }
    }

    pub(super) fn set_current_link_speed(mut self, speed: Option<String>) -> Self {
        self.current_link_speed = speed;
        self
    }

    pub(super) fn set_max_link_speed(mut self, speed: Option<String>) -> Self {
        self.max_link_speed = speed;
        self
    }

    pub(super) fn set_current_link_width(mut self, speed: Option<String>) -> Self {
        self.current_link_width = speed;
        self
    }

    pub(super) fn set_max_link_width(mut self, speed: Option<String>) -> Self {
        self.max_link_width = speed;
        self
    }
}

// eg. 0000:5d:0e.0
#[derive(Debug)]
pub(super) struct PciAddress {
    domain: u16,
    bus: u8,
    device: u8,
    function: u8,
}

impl PciAddress {
    pub(super) fn from_str(address: &str) -> crate::Result<PciAddress> {
        if address.split('.').collect::<Vec<&str>>().len() != 2 {
            return Err(Box::new(PciError::IllegalPciAddress(address.to_string())));
        }
        let fields: Vec<&str> = address.split(&[':', '.'][..]).collect();
        match fields.len() {
            3 => Ok(PciAddress {
                domain: 0,
                bus: u8::from_str_radix(fields[0], 16).map_err(|e| PciError::ParseIntError {
                    field: fields[0].to_string(),
                    source: e,
                })?,
                device: u8::from_str_radix(fields[1], 16).map_err(|e| PciError::ParseIntError {
                    field: fields[1].to_string(),
                    source: e,
                })?,
                function: u8::from_str_radix(fields[2], 16).map_err(|e| {
                    PciError::ParseIntError {
                        field: fields[2].to_string(),
                        source: e,
                    }
                })?,
            }),
            4 => Ok(PciAddress {
                domain: u16::from_str_radix(fields[0], 16).map_err(|e| {
                    PciError::ParseIntError {
                        field: fields[0].to_string(),
                        source: e,
                    }
                })?,
                bus: u8::from_str_radix(fields[1], 16).map_err(|e| PciError::ParseIntError {
                    field: fields[1].to_string(),
                    source: e,
                })?,
                device: u8::from_str_radix(fields[2], 16).map_err(|e| PciError::ParseIntError {
                    field: fields[2].to_string(),
                    source: e,
                })?,
                function: u8::from_str_radix(fields[3], 16).map_err(|e| {
                    PciError::ParseIntError {
                        field: fields[3].to_string(),
                        source: e,
                    }
                })?,
            }),
            _ => Err(Box::new(PciError::IllegalPciAddress(address.to_string()))),
        }
    }

    pub(super) fn to_string(&self) -> String {
        format!(
            "{:04x}:{:02x}:{:02x}.{:01x}",
            self.domain, self.bus, self.device, self.function
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_full_format() {
        let id_raw = "0000:5d:0e.0";
        let pci = PciAddress::from_str(id_raw).unwrap();
        assert_eq!(pci.domain, 0x0000);
        assert_eq!(pci.bus, 0x5d);
        assert_eq!(pci.device, 0x0e);
        assert_eq!(pci.function, 0x0);
        assert_eq!(pci.to_string(), id_raw);
    }

    #[test]
    fn test_parse_short_format() {
        let pci = PciAddress::from_str("5d:0e.0").unwrap();
        assert_eq!(pci.domain, 0x0000);
        assert_eq!(pci.bus, 0x5d);
        assert_eq!(pci.device, 0x0e);
        assert_eq!(pci.function, 0x0);
    }

    #[test]
    fn test_format_output() {
        let pci = PciAddress {
            domain: 0x0000,
            bus: 0x5d,
            device: 0x0e,
            function: 0x0,
        };
        assert_eq!(pci.to_string(), "0000:5d:0e.0");
    }

    #[test]
    fn test_invalid_input() {
        assert!(PciAddress::from_str("invalid").is_err());
        assert!(PciAddress::from_str("00:00:14").is_err());
    }
}
