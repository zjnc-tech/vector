mod errors;
#[allow(dead_code)]
mod pci;
use chrono::Utc;
use serde_with::serde_as;
use std::time::Duration;
use vector_lib::{config::DataType, schema};

use crate::config::LogNamespace;
use crate::{event::LogEvent, internal_events::StreamClosedError};
use vector_lib::config::LegacyKey;
use vector_lib::lookup::{owned_value_path, path};

use crate::config::{SourceConfig, SourceContext, SourceOutput};

use vrl::value::Kind;

use std::path::{Path, PathBuf};
use std::{fs, io};
use walkdir::WalkDir;

use vector_lib::configurable::configurable_component;

const SYS_PCI_PATH: &str = "/sys/bus/pci/devices";

const fn default_interval() -> Duration {
    Duration::from_secs(60 * 15)
}

/// Configuration for the `pci` source.
#[serde_as]
#[configurable_component(source("pci", "Collect pci data."))]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// collect interval of pci source.
    #[serde(alias = "batch_interval")]
    #[derivative(Default(value = "default_interval()"))]
    #[serde(default = "default_interval")]
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<f64>")]
    pub interval: Duration,
}

impl_generate_config_from_default!(Config);

impl Into<LogEvent> for pci::PciDevice {
    fn into(self) -> LogEvent {
        let namespace = LogNamespace::Legacy;
        let mut log = LogEvent::default();
        let now = Utc::now();
        namespace.insert_standard_vector_source_metadata(&mut log, Config::NAME, now);
        namespace.insert_source_metadata(
            Config::NAME,
            &mut log,
            Some(LegacyKey::InsertIfEmpty(path!("address"))),
            path!("address"),
            self.address.to_string(),
        );
        namespace.insert_source_metadata(
            Config::NAME,
            &mut log,
            Some(LegacyKey::InsertIfEmpty(path!("class"))),
            path!("class"),
            format!("{:#x}", self.class),
        );
        namespace.insert_source_metadata(
            Config::NAME,
            &mut log,
            Some(LegacyKey::InsertIfEmpty(path!("device"))),
            path!("device"),
            format!("{:#x}", self.device),
        );
        namespace.insert_source_metadata(
            Config::NAME,
            &mut log,
            Some(LegacyKey::InsertIfEmpty(path!("vendor"))),
            path!("vendor"),
            format!("{:#x}", self.vendor),
        );
        namespace.insert_source_metadata(
            Config::NAME,
            &mut log,
            Some(LegacyKey::InsertIfEmpty(path!("max_link_speed"))),
            path!("max_link_speed"),
            self.max_link_speed.unwrap_or("".to_string()),
        );
        namespace.insert_source_metadata(
            Config::NAME,
            &mut log,
            Some(LegacyKey::InsertIfEmpty(path!("max_link_width"))),
            path!("max_link_width"),
            self.max_link_width.unwrap_or("".to_string()),
        );
        namespace.insert_source_metadata(
            Config::NAME,
            &mut log,
            Some(LegacyKey::InsertIfEmpty(path!("current_link_speed"))),
            path!("current_link_speed"),
            self.current_link_speed.unwrap_or("".to_string()),
        );
        namespace.insert_source_metadata(
            Config::NAME,
            &mut log,
            Some(LegacyKey::InsertIfEmpty(path!("current_link_width"))),
            path!("current_link_width"),
            self.current_link_width.unwrap_or("".to_string()),
        );
        log
    }
}

impl Config {
    fn read_pci_file<P: AsRef<Path>>(&self, pci_addr: P, filename: &str) -> io::Result<String> {
        let fp = PathBuf::from(SYS_PCI_PATH).join(pci_addr).join(filename);
        let content = fs::read_to_string(&fp)?;
        Ok(content.trim().to_string())
    }

    fn read_pci_vendor<P: AsRef<Path>>(&self, pci_addr: P) -> io::Result<String> {
        self.read_pci_file(pci_addr, "vendor")
    }

    fn read_pci_device<P: AsRef<Path>>(&self, pci_addr: P) -> io::Result<String> {
        self.read_pci_file(pci_addr, "device")
    }

    fn read_pci_class<P: AsRef<Path>>(&self, pci_addr: P) -> io::Result<String> {
        self.read_pci_file(pci_addr, "class")
    }

    fn read_pci_max_link_speed<P: AsRef<Path>>(&self, pci_addr: P) -> io::Result<String> {
        self.read_pci_file(pci_addr, "max_link_speed")
    }

    fn read_pci_max_link_width<P: AsRef<Path>>(&self, pci_addr: P) -> io::Result<String> {
        self.read_pci_file(pci_addr, "max_link_width")
    }

    fn read_pci_current_link_speed<P: AsRef<Path>>(&self, pci_addr: P) -> io::Result<String> {
        self.read_pci_file(pci_addr, "current_link_speed")
    }

    fn read_pci_current_link_width<P: AsRef<Path>>(&self, pci_addr: P) -> io::Result<String> {
        self.read_pci_file(pci_addr, "current_link_width")
    }

    fn walk_pcie_addrs(&self) -> crate::Result<Vec<pci::PciDevice>> {
        let mut pcis = Vec::new();
        for entry in WalkDir::new(SYS_PCI_PATH)
            .min_depth(1)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let path = entry.path();
            let metadata = entry.metadata()?;

            // 如果是符号链接则解析到真实路径
            let actual_path = if metadata.file_type().is_symlink() {
                fs::canonicalize(path)?
            } else {
                path.to_path_buf()
            };

            let actual_metadata = fs::metadata(&actual_path)?;

            if actual_metadata.is_dir() {
                if let Some(dir_name) = actual_path.file_name().and_then(|n| n.to_str()) {
                    if dir_name.contains(":") {
                        let addr = pci::PciAddress::from_str(dir_name)?;
                        let class = u32::from_str_radix(
                            self.read_pci_class(&actual_path)?
                                .as_str()
                                .trim_start_matches("0x"),
                            16,
                        )?;
                        let device = u32::from_str_radix(
                            self.read_pci_device(&actual_path)?
                                .as_str()
                                .trim_start_matches("0x"),
                            16,
                        )?;
                        let vendor = u32::from_str_radix(
                            self.read_pci_vendor(&actual_path)?
                                .as_str()
                                .trim_start_matches("0x"),
                            16,
                        )?;
                        let pci_current_link_speed =
                            self.read_pci_current_link_speed(&actual_path).ok();
                        let pci_current_link_width =
                            self.read_pci_current_link_width(&actual_path).ok();
                        let pci_max_link_speed = self.read_pci_max_link_speed(&actual_path).ok();
                        let pci_max_link_width = self.read_pci_max_link_width(&actual_path).ok();
                        pcis.push(
                            pci::PciDevice::new(addr, vendor, device, class)
                                .set_current_link_speed(pci_current_link_speed)
                                .set_current_link_width(pci_current_link_width)
                                .set_max_link_speed(pci_max_link_speed)
                                .set_max_link_width(pci_max_link_width),
                        );
                    }
                }
            }
        }

        Ok(pcis)
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "pci")]
impl SourceConfig for Config {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        let mut out = cx.out.clone();
        let shutdown = cx.shutdown.clone();
        let this = self.clone(); // <--- clone self

        Ok(Box::pin(async move {
            let mut interface_interval = tokio::time::interval(this.interval);

            loop {
                tokio::select! {
                    _ = interface_interval.tick() => {
                        match this.walk_pcie_addrs(){
                            Ok(pcis) => {
                                let logs = pcis.into_iter().map(|pci| pci.into()).collect::<Vec<LogEvent>>();
                                let count = logs.len();
                                out.send_batch(logs).await.map_err(|_|{
                                    emit!(StreamClosedError {count});
                                })?;
                            }
                            Err(e) => warn!("walk pcie addrs error {}", e),
                        }
                    }
                    _ = shutdown.clone() => {
                        info!("Shutting down pci source");
                        break;
                    }
                }
            }

            Ok(())
        }))
    }

    fn outputs(&self, _: vector_lib::config::LogNamespace) -> Vec<SourceOutput> {
        let definition = schema::Definition::empty_legacy_namespace()
            .with_event_field(&owned_value_path!("address"), Kind::bytes(), Some(""))
            .with_event_field(&owned_value_path!("class"), Kind::bytes(), Some(""))
            .with_event_field(&owned_value_path!("device"), Kind::bytes(), Some(""))
            .with_event_field(&owned_value_path!("vendor"), Kind::bytes(), Some(""))
            .with_event_field(
                &owned_value_path!("max_link_speed"),
                Kind::bytes(),
                Some(""),
            )
            .with_event_field(
                &owned_value_path!("max_link_width"),
                Kind::bytes(),
                Some(""),
            )
            .with_event_field(
                &owned_value_path!("current_link_speed"),
                Kind::bytes(),
                Some(""),
            )
            .with_event_field(
                &owned_value_path!("current_link_width"),
                Kind::bytes(),
                Some(""),
            );

        vec![SourceOutput::new_maybe_logs(DataType::Log, definition)]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}
