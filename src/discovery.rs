//! Chromecast device discovery via mDNS.
//!
//! Listens for `_googlecast._tcp.local.` service records. `md` TXT field
//! carries the model name; audio-only devices ("Google Nest Mini", "Google

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use mdns_sd::{ServiceDaemon, ServiceEvent};

const SERVICE: &str = "_googlecast._tcp.local.";

/// `ca` TXT bitmask: bit 0 = VIDEO_OUT. Verified on a real Nest Mini
/// (ca=198660, bit 0 clear); video Chromecasts carry e.g. ca=4101 (bit 0 set).
const CA_VIDEO_OUT: u32 = 0x01;

#[derive(Debug, Clone)]
pub struct Device {
    pub friendly_name: String,
    pub model: Option<String>,
    pub host: String,
    /// True for speakers (Nest Mini, Google Home, third-party Cast audio).
    /// These reject the video mirroring app (0F5096E8) outright, so getting
    pub is_audio_only: bool,
}

impl Device {
    fn from_txt(host: String, txt: &HashMap<String, String>) -> Option<Self> {
        let friendly = txt.get("fn")?.to_string();
        let model = txt.get("md").cloned();
        let ca = txt.get("ca").and_then(|v| v.parse::<u32>().ok());
        let is_audio_only = is_audio_only_device(ca, model.as_deref());
        Some(Self { friendly_name: friendly, model, host, is_audio_only })
    }
}

/// Primary signal is the `ca` capability bitmask (no VIDEO_OUT bit = audio
/// device); model names are only a fallback for responders that omit `ca`.
fn is_audio_only_device(ca: Option<u32>, model: Option<&str>) -> bool {
    match ca {
        Some(bits) => bits & CA_VIDEO_OUT == 0,
        None => model.is_some_and(is_audio_only_model),
    }
}

fn is_audio_only_model(model: &str) -> bool {
    let m = model.to_ascii_lowercase();
    m.contains("google home")
        || m.contains("nest mini")
        || m.contains("nest audio")
        || m.contains("home mini")
        || m.contains("home max")
}

/// Discover Chromecasts on the LAN. If `wanted_name` is given, returns as soon
/// as it's found; otherwise waits the full timeout.
pub fn discover(wanted_name: Option<&str>, timeout: Duration) -> Result<Vec<Device>> {
    let daemon = ServiceDaemon::new().context("start mdns daemon")?;
    let receiver = daemon.browse(SERVICE).context("start browse")?;

    let deadline = Instant::now() + timeout;
    let mut devices: HashMap<String, Device> = HashMap::new();

    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match receiver.recv_timeout(remaining.min(Duration::from_millis(200))) {
            Ok(ServiceEvent::ServiceResolved(info)) => {
                // TXT properties come as key=value; mdns-sd exposes them as `properties()`.
                let mut txt = HashMap::new();
                for prop in info.get_properties().iter() {
                    txt.insert(prop.key().to_string(), prop.val_str().to_string());
                }
                let Some(ip) = info.get_addresses_v4().iter().next().copied() else {
                    continue;
                };
                if let Some(dev) = Device::from_txt(ip.to_string(), &txt) {
                    let matched_wanted = wanted_name
                        .is_some_and(|n| n.eq_ignore_ascii_case(&dev.friendly_name));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ca_bitmask_decides_when_present() {
        assert!(is_audio_only_device(Some(198_660), None)); // real Nest Mini
        assert!(is_audio_only_device(Some(2052), Some("Chromecast Audio")));
        assert!(!is_audio_only_device(Some(4101), None)); // video Chromecast
        // ca wins over a misleading model name
        assert!(!is_audio_only_device(Some(4101), Some("Nest Mini")));
    }

    #[test]
    fn model_fallback_without_ca() {
        assert!(is_audio_only_device(None, Some("Google Nest Mini")));
        assert!(is_audio_only_device(None, Some("Google Home Max")));
        assert!(!is_audio_only_device(None, Some("Chromecast Ultra")));
        assert!(!is_audio_only_device(None, None));
    }
}
