//! Chromecast device discovery via mDNS.
//!
//! Listens for `_googlecast._tcp.local.` service records. `md` TXT field
//! carries the model name; audio-only devices ("Google Nest Mini", "Google

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use mdns_sd::{ServiceDaemon, ServiceEvent};

const SERVICE: &str = "_googlecast._tcp.local.";

#[derive(Debug, Clone)]
pub struct Device {
    pub friendly_name: String,
    pub model: Option<String>,
    pub host: String,
    /// True for Nest Mini / Google Home style speakers. Determined from the
    /// model name; the video mirroring app (0F5096E8) is rejected by these.
    pub is_audio_only: bool,
}

impl Device {
    fn from_txt(host: String, txt: &HashMap<String, String>) -> Option<Self> {
        let friendly = txt.get("fn")?.to_string();
        let model = txt.get("md").cloned();
        let is_audio_only = model
            .as_deref()
            .map(is_audio_only_model)
            .unwrap_or(false);
        Some(Self { friendly_name: friendly, model, host, is_audio_only })
    }
}

/// Rough heuristic based on Cast model names. Errs on the side of "video"
/// (which is fine for actual video-capable devices; audio-only devices are
/// well-known names). If mDNS gives us `ca` (capability bitmask) in the TXT
/// we could be more precise, but this matches how pychromecast does it.
fn is_audio_only_model(model: &str) -> bool {
    let m = model.to_ascii_lowercase();
    m.contains("google home")
        || m.contains("google nest mini")
        || m.contains("nest mini")
        || m.contains("google home mini")
        || m.contains("home mini")
        || m.contains("home max")
        || m.contains("nest audio")
}

/// Discover Chromecasts on the LAN. If `wanted_name` is given, returns as soon
/// as it's found; otherwise waits the full timeout.
pub fn discover(wanted_name: Option<&str>, timeout: Duration) -> Result<Vec<Device>> {
    let daemon = ServiceDaemon::new().context("start mdns daemon")?;
    let receiver = daemon.browse(SERVICE).context("start browse")?;

    let deadline = Instant::now() + timeout;
    let mut devices: HashMap<String, Device> = HashMap::new();

    while Instant::now() < deadline {
        let remaining = deadline - Instant::now();
        match receiver.recv_timeout(remaining.min(Duration::from_millis(200))) {
            Ok(ServiceEvent::ServiceResolved(info)) => {
                // TXT properties come as key=value; mdns-sd exposes them as `properties()`.
                let mut txt = HashMap::new();
                for prop in info.get_properties().iter() {
                    txt.insert(prop.key().to_string(), prop.val_str().to_string());
                }
                let host = info
                    .get_addresses_v4()
                    .iter()
                    .next()
                    .map(|ip| ip.to_string())
                    .unwrap_or_default();
                if host.is_empty() {
                    continue;
                }
                if let Some(dev) = Device::from_txt(host, &txt) {
                    let matched_wanted = wanted_name
                        .map(|n| n.eq_ignore_ascii_case(&dev.friendly_name))
                        .unwrap_or(false);
                    devices.insert(dev.friendly_name.clone(), dev);
                    if matched_wanted {
                        break;
                    }
                }
            }
            Ok(_) => {}
            Err(_) => {} // timeout tick
        }
    }

    let _ = daemon.shutdown();

    let mut out: Vec<Device> = devices.into_values().collect();
    out.sort_by(|a, b| a.friendly_name.cmp(&b.friendly_name));

    if let Some(name) = wanted_name {
        out.retain(|d| d.friendly_name.eq_ignore_ascii_case(name));
        if out.is_empty() {
            bail!("Device '{name}' not found within {timeout:?}");
        }
    }

    Ok(out)
}
