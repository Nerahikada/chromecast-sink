//! Windows counterpart of the PipeWire sink. Windows cannot create an output device from user mode,
//! so this captures an existing render endpoint via WASAPI loopback: the default playback device unless
//! `--capture-device` names another one (for example a virtual cable such as VB-Audio "CABLE Input").

use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use windows::core::AgileReference;
use windows::Win32::Devices::FunctionDiscovery::PKEY_Device_FriendlyName;
use windows::Win32::Media::Audio::{
    eConsole, eRender, IAudioCaptureClient, IAudioClient, IAudioRenderClient, IMMDevice,
    IMMDeviceEnumerator, MMDeviceEnumerator, AUDCLNT_BUFFERFLAGS_SILENT, AUDCLNT_SHAREMODE_SHARED,
    AUDCLNT_STREAMFLAGS_AUTOCONVERTPCM, AUDCLNT_STREAMFLAGS_LOOPBACK,
    AUDCLNT_STREAMFLAGS_SRC_DEFAULT_QUALITY, DEVICE_STATE_ACTIVE, WAVEFORMATEX,
};
use windows::Win32::System::Com::{
    CoCreateInstance, CoInitializeEx, CoUninitialize, CLSCTX_ALL, COINIT_MULTITHREADED, STGM_READ,
};

use crate::audio_format::{CHANNELS, SAMPLE_RATE};
use crate::audio_ring::{self, RingConsumer, RingProducer};

/// ~170 ms at 48 kHz. Only has to absorb encoder-thread jitter.
const RING_CAPACITY_FRAMES: usize = 8192;

/// WASAPI buffer length in 100-ns units (100 ms). Loopback latency is set by the poll interval, not by this.
const BUFFER_HNS: i64 = 1_000_000;
const POLL: Duration = Duration::from_millis(3);

const WAVE_FORMAT_IEEE_FLOAT: u16 = 3;

pub struct VirtualSink {
    pub sink_name: String,
    consumer: Option<RingConsumer>,
    quit: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    // Held until join so the pump thread's COM objects stay valid until it enters its own MTA or drops them on failure.
    _mta: Mta,
}

impl VirtualSink {
    pub fn new(_device_name: &str, capture_device: Option<&str>) -> Result<Self> {
        let mta = Mta::enter()?;
        // Flattened while COM is still initialized: a windows::core::Error holds a COM error object.
        let (lb, sink_name) = start_capture(capture_device).map_err(|e| anyhow!("{e:#}"))?;
        let agile = AgileLoopback::new(&lb).map_err(|e| anyhow!("{e:#}"))?;
        let (producer, consumer) = audio_ring::channel(RING_CAPACITY_FRAMES, CHANNELS as usize);
        let quit = Arc::new(AtomicBool::new(false));

        let quit_c = Arc::clone(&quit);
        let thread = std::thread::Builder::new()
            .name("wasapi-loopback".into())
            .spawn(move || run_wasapi_thread(agile, producer, quit_c))
            .expect("spawn wasapi thread");

        log::info!("Capturing render endpoint: {sink_name}");
        Ok(Self {
            sink_name,
            consumer: Some(consumer),
            quit,
            thread: Some(thread),
            _mta: mta,
        })
    }

    /// Succeeds once. `VirtualSink` has a `Drop`, which prevents a field move.
    pub fn take_consumer(&mut self) -> Option<RingConsumer> {
        self.consumer.take()
    }
}

impl Drop for VirtualSink {
    fn drop(&mut self) {
        self.quit.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// This thread's membership in the multithreaded apartment. `!Send` because CoUninitialize must run on the thread that entered.
struct Mta(PhantomData<*const ()>);

impl Mta {
    #[allow(unsafe_code)]
    fn enter() -> Result<Self> {
        // SAFETY: plain COM init for this thread, balanced by CoUninitialize in Drop.
        unsafe { CoInitializeEx(None, COINIT_MULTITHREADED) }
            .ok()
            .map_err(|e| anyhow!("CoInitializeEx: {e}"))?;
        Ok(Self(PhantomData))
    }
}

impl Drop for Mta {
    #[allow(unsafe_code)]
    fn drop(&mut self) {
        // SAFETY: Mta is only created after CoInitializeEx succeeds, and `!Send` keeps the drop on the same thread.
        unsafe { CoUninitialize() };
    }
}

fn run_wasapi_thread(agile: AgileLoopback, mut producer: RingProducer, quit: Arc<AtomicBool>) {
    let closer = producer.closer();
    match Mta::enter() {
        Err(e) => log::error!("WASAPI capture failed: {e:#}"),
        Ok(_mta) => {
            if let Err(e) = agile
                .resolve()
                .and_then(|lb| pump_capture(lb, &mut producer, &quit))
            {
                log::error!("WASAPI capture failed: {e:#}");
            }
        }
    }
    closer.close();
    log::debug!("wasapi thread exiting");
}

struct Loopback {
    capture_client: IAudioClient,
    capture: IAudioCaptureClient,
    render_client: IAudioClient,
    render: IAudioRenderClient,
    render_frames: u32,
}

/// `Loopback` in a form that can cross to the pump thread. The interfaces themselves are `!Send`.
struct AgileLoopback {
    capture_client: AgileReference<IAudioClient>,
    capture: AgileReference<IAudioCaptureClient>,
    render_client: AgileReference<IAudioClient>,
    render: AgileReference<IAudioRenderClient>,
    render_frames: u32,
}

impl AgileLoopback {
    fn new(lb: &Loopback) -> Result<Self> {
        Ok(Self {
            capture_client: AgileReference::new(&lb.capture_client)?,
            capture: AgileReference::new(&lb.capture)?,
            render_client: AgileReference::new(&lb.render_client)?,
            render: AgileReference::new(&lb.render)?,
            render_frames: lb.render_frames,
        })
    }

    /// Consumes `self` so the references are released while the caller is still in the MTA.
    fn resolve(self) -> Result<Loopback> {
        Ok(Loopback {
            capture_client: self.capture_client.resolve()?,
            capture: self.capture.resolve()?,
            render_client: self.render_client.resolve()?,
            render: self.render.resolve()?,
            render_frames: self.render_frames,
        })
    }
}

#[allow(unsafe_code)]
fn start_capture(wanted: Option<&str>) -> Result<(Loopback, String)> {
    let (device, name) = match wanted {
        Some(w) => find_render_endpoint(w)?,
        None => default_render_endpoint()?,
    };
    let fmt = float_format();

    // SAFETY: COM calls on interfaces owned by this thread. `fmt` outlives Initialize.
    let capture_client: IAudioClient =
        unsafe { device.Activate(CLSCTX_ALL, None) }.context("activate capture client")?;
    unsafe {
        capture_client.Initialize(
            AUDCLNT_SHAREMODE_SHARED,
            AUDCLNT_STREAMFLAGS_LOOPBACK
                | AUDCLNT_STREAMFLAGS_AUTOCONVERTPCM
                | AUDCLNT_STREAMFLAGS_SRC_DEFAULT_QUALITY,
            BUFFER_HNS,
            0,
            &fmt,
            None,
        )
    }
    .context("initialize loopback capture (48 kHz / f32 / stereo)")?;
    let capture: IAudioCaptureClient =
        unsafe { capture_client.GetService() }.context("IAudioCaptureClient")?;

    // Loopback delivers no packets while nothing plays on the endpoint, which would stall the RTP stream
    // (PipeWire's `node.always-process` covers this on Linux). Rendering silence into the same endpoint keeps
    // the device clock, and therefore the packets, running.
    let render_client: IAudioClient =
        unsafe { device.Activate(CLSCTX_ALL, None) }.context("activate keep-alive client")?;
    unsafe {
        render_client.Initialize(
            AUDCLNT_SHAREMODE_SHARED,
            AUDCLNT_STREAMFLAGS_AUTOCONVERTPCM | AUDCLNT_STREAMFLAGS_SRC_DEFAULT_QUALITY,
            BUFFER_HNS,
            0,
            &fmt,
            None,
        )
    }
    .context("initialize keep-alive render")?;
    let render: IAudioRenderClient =
        unsafe { render_client.GetService() }.context("IAudioRenderClient")?;
    let render_frames = unsafe { render_client.GetBufferSize() }.context("render buffer size")?;

    unsafe { render_client.Start() }.context("start keep-alive render")?;
    unsafe { capture_client.Start() }.context("start loopback capture")?;
    Ok((
        Loopback {
            capture_client,
            capture,
            render_client,
            render,
            render_frames,
        },
        name,
    ))
}

#[allow(unsafe_code)]
fn pump_capture(lb: Loopback, producer: &mut RingProducer, quit: &AtomicBool) -> Result<()> {
    let Loopback {
        capture_client,
        capture,
        render_client,
        render,
        render_frames,
    } = lb;
    let frame_bytes = CHANNELS as usize * 4;
    let mut s16: Vec<u8> = Vec::new();
    while !quit.load(Ordering::Relaxed) {
        let padding = unsafe { render_client.GetCurrentPadding() }.context("keep-alive padding")?;
        let free = render_frames.saturating_sub(padding);
        if free > 0 {
            unsafe { render.GetBuffer(free) }.context("keep-alive buffer")?;
            unsafe { render.ReleaseBuffer(free, AUDCLNT_BUFFERFLAGS_SILENT.0 as u32) }
                .context("keep-alive release")?;
        }

        loop {
            let next = unsafe { capture.GetNextPacketSize() }.context("loopback packet size")?;
            if next == 0 {
                break;
            }
            let mut data: *mut u8 = std::ptr::null_mut();
            let mut frames = 0u32;
            let mut flags = 0u32;
            unsafe { capture.GetBuffer(&mut data, &mut frames, &mut flags, None, None) }
                .context("loopback buffer")?;
            let len = frames as usize * frame_bytes;
            if flags & AUDCLNT_BUFFERFLAGS_SILENT.0 as u32 != 0 || data.is_null() {
                s16.clear();
                s16.resize(len / 2, 0);
            } else {
                f32le_to_s16le(unsafe { std::slice::from_raw_parts(data, len) }, &mut s16);
            }
            producer.write_s16le(&s16);
            unsafe { capture.ReleaseBuffer(frames) }.context("loopback release")?;
        }
        std::thread::sleep(POLL);
    }

    let _ = unsafe { capture_client.Stop() };
    let _ = unsafe { render_client.Stop() };
    Ok(())
}

fn float_format() -> WAVEFORMATEX {
    let block_align = CHANNELS as u16 * 4;
    WAVEFORMATEX {
        wFormatTag: WAVE_FORMAT_IEEE_FLOAT,
        nChannels: CHANNELS as u16,
        nSamplesPerSec: SAMPLE_RATE,
        nAvgBytesPerSec: SAMPLE_RATE * block_align as u32,
        nBlockAlign: block_align,
        wBitsPerSample: 32,
        cbSize: 0,
    }
}

fn f32le_to_s16le(src: &[u8], dst: &mut Vec<u8>) {
    dst.clear();
    for b in src.chunks_exact(4) {
        let x = f32::from_le_bytes([b[0], b[1], b[2], b[3]]);
        let s = (x * 32768.0).round() as i16;
        dst.extend_from_slice(&s.to_le_bytes());
    }
}

#[allow(unsafe_code)]
fn friendly_name(dev: &IMMDevice) -> Result<String> {
    // SAFETY: COM calls on this thread's apartment.
    unsafe {
        Ok(dev
            .OpenPropertyStore(STGM_READ)?
            .GetValue(&PKEY_Device_FriendlyName)?
            .to_string())
    }
}

/// The endpoint Windows plays to by default (the eConsole role, which is what Sound settings call "Output").
#[allow(unsafe_code)]
fn default_render_endpoint() -> Result<(IMMDevice, String)> {
    // SAFETY: COM calls on this thread's apartment.
    let enumerator: IMMDeviceEnumerator =
        unsafe { CoCreateInstance(&MMDeviceEnumerator, None, CLSCTX_ALL) }
            .context("MMDeviceEnumerator")?;
    let dev = unsafe { enumerator.GetDefaultAudioEndpoint(eRender, eConsole) }
        .context("no default playback device")?;
    let name = friendly_name(&dev)?;
    Ok((dev, name))
}

/// Active render endpoints as (device, friendly name).
#[allow(unsafe_code)]
fn render_endpoints() -> Result<Vec<(IMMDevice, String)>> {
    // SAFETY: COM calls on this thread's apartment.
    let enumerator: IMMDeviceEnumerator =
        unsafe { CoCreateInstance(&MMDeviceEnumerator, None, CLSCTX_ALL) }
            .context("MMDeviceEnumerator")?;
    let coll = unsafe { enumerator.EnumAudioEndpoints(eRender, DEVICE_STATE_ACTIVE) }
        .context("EnumAudioEndpoints")?;
    let mut out = Vec::new();
    for i in 0..unsafe { coll.GetCount() }? {
        let dev = unsafe { coll.Item(i) }?;
        let name = friendly_name(&dev)?;
        out.push((dev, name));
    }
    Ok(out)
}

fn find_render_endpoint(wanted: &str) -> Result<(IMMDevice, String)> {
    let all = render_endpoints()?;
    let needle = wanted.to_lowercase();
    let mut hits: Vec<_> = all
        .iter()
        .filter(|(_, n)| n.to_lowercase().contains(&needle))
        .collect();
    match hits.len() {
        1 => {
            let (d, n) = hits.pop().unwrap();
            Ok((d.clone(), n.clone()))
        }
        0 => {
            let names: Vec<_> = all.iter().map(|(_, n)| format!("  - {n}")).collect();
            bail!(
                "no active playback device matches \"{wanted}\". Available:\n{}",
                names.join("\n")
            )
        }
        _ => {
            let names: Vec<_> = hits.iter().map(|(_, n)| format!("  - {n}")).collect();
            bail!(
                "\"{wanted}\" matches several playback devices, be more specific:\n{}",
                names.join("\n")
            )
        }
    }
}

/// For `--list-devices`.
pub fn list_capture_devices() -> Result<Vec<String>> {
    let _mta = Mta::enter()?;
    render_endpoints()
        .map(|v| v.into_iter().map(|(_, n)| n).collect())
        .map_err(|e| anyhow!("{e:#}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float_format_is_48k_f32_stereo() {
        let f = float_format();
        assert_eq!({ f.wFormatTag }, 3);
        assert_eq!({ f.nSamplesPerSec }, 48_000);
        assert_eq!({ f.wBitsPerSample }, 32);
        assert_eq!({ f.nChannels }, 2);
        assert_eq!({ f.nBlockAlign }, 8);
        assert_eq!({ f.nAvgBytesPerSec }, 384_000);
    }

    #[test]
    fn f32_to_s16_rounds_and_saturates() {
        let input: Vec<u8> = [
            0.0f32,
            -0.0,
            0.5,
            -1.0,
            1.0,
            2.0,
            -2.0,
            f32::NAN,
            1.4 / 32768.0,
        ]
        .iter()
        .flat_map(|x| x.to_le_bytes())
        .collect();
        let mut out = Vec::new();
        f32le_to_s16le(&input, &mut out);
        let got: Vec<i16> = out
            .chunks_exact(2)
            .map(|b| i16::from_le_bytes([b[0], b[1]]))
            .collect();
        assert_eq!(got, [0, 0, 16384, -32768, 32767, 32767, -32768, 0, 1]);
    }
}
