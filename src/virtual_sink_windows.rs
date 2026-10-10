//! Windows counterpart of the PipeWire sink. Windows cannot create an output device from user mode,
//! so this captures an existing render endpoint via WASAPI loopback — normally a virtual cable
//! (VB-Audio "CABLE Input"), which then acts as the selectable "Chromecast" output.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use windows::Win32::Devices::FunctionDiscovery::PKEY_Device_FriendlyName;
use windows::Win32::Media::Audio::{
    eRender, IAudioCaptureClient, IAudioClient, IAudioRenderClient, IMMDevice, IMMDeviceEnumerator, MMDeviceEnumerator,
    AUDCLNT_BUFFERFLAGS_SILENT, AUDCLNT_SHAREMODE_SHARED, AUDCLNT_STREAMFLAGS_AUTOCONVERTPCM, AUDCLNT_STREAMFLAGS_LOOPBACK,
    AUDCLNT_STREAMFLAGS_SRC_DEFAULT_QUALITY, DEVICE_STATE_ACTIVE, WAVEFORMATEX,
};
use windows::Win32::System::Com::{CoCreateInstance, CoInitializeEx, CoUninitialize, CLSCTX_ALL, COINIT_MULTITHREADED, STGM_READ};

use crate::audio_ring::{self, RingConsumer, RingProducer};
use crate::capture::{CHANNELS, SAMPLE_RATE};

/// ~170 ms at 48 kHz; only has to absorb encoder-thread jitter.
const RING_CAPACITY_FRAMES: usize = 8192;

const READY_TIMEOUT: Duration = Duration::from_secs(10);

/// Render endpoint captured when `--capture-device` is not given.
pub const DEFAULT_CAPTURE_DEVICE: &str = "CABLE Input";

/// WASAPI buffer length in 100-ns units (100 ms). Loopback latency is set by the poll interval, not by this.
const BUFFER_HNS: i64 = 1_000_000;
const POLL: Duration = Duration::from_millis(3);

const WAVE_FORMAT_PCM: u16 = 1;

static CAPTURE_DEVICE: OnceLock<String> = OnceLock::new();

/// Selects the render endpoint to capture (substring of its friendly name, case-insensitive). Call before `VirtualSink::new`.
pub fn set_capture_device(name: &str) {
    let _ = CAPTURE_DEVICE.set(name.to_string());
}

pub struct VirtualSink {
    pub sink_name: String,
    consumer: Option<RingConsumer>,
    quit: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl VirtualSink {
    pub fn new(_device_name: &str) -> Result<Self> {
        let wanted = CAPTURE_DEVICE.get().map(String::as_str).unwrap_or(DEFAULT_CAPTURE_DEVICE).to_string();
        let (producer, consumer) = audio_ring::channel(RING_CAPACITY_FRAMES, CHANNELS as usize);
        let quit = Arc::new(AtomicBool::new(false));
        let (ready_tx, ready_rx) = mpsc::channel::<Result<String>>();

        let quit_c = Arc::clone(&quit);
        let thread = std::thread::Builder::new()
            .name("wasapi-loopback".into())
            .spawn(move || run_wasapi_thread(wanted, producer, quit_c, ready_tx))
            .expect("spawn wasapi thread");

        let mut sink = Self { sink_name: String::new(), consumer: Some(consumer), quit, thread: Some(thread) };

        match ready_rx.recv_timeout(READY_TIMEOUT) {
            Ok(r) => sink.sink_name = r?,
            Err(mpsc::RecvTimeoutError::Timeout) => bail!("WASAPI capture was not started within {}s", READY_TIMEOUT.as_secs()),
            Err(mpsc::RecvTimeoutError::Disconnected) => bail!("WASAPI thread died before capture was ready"),
        }

        log::info!("Capturing render endpoint: {}", sink.sink_name);
        Ok(sink)
    }

    /// Succeeds once; `VirtualSink` has a `Drop` so this cannot be a field move.
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

fn run_wasapi_thread(wanted: String, mut producer: RingProducer, quit: Arc<AtomicBool>, ready_tx: mpsc::Sender<Result<String>>) {
    let closer = producer.closer();
    // SAFETY: plain COM init for this thread, balanced by CoUninitialize below.
    if let Err(e) = unsafe { CoInitializeEx(None, COINIT_MULTITHREADED) }.ok() {
        let _ = ready_tx.send(Err(anyhow!("CoInitializeEx: {e}")));
        closer.close();
        return;
    }
    let result = capture_loop(&wanted, &mut producer, &quit, &ready_tx);
    if let Err(e) = result {
        // Fails the caller if it is still waiting, otherwise only logs; either way the encoder sees a closed ring.
        if ready_tx.send(Err(anyhow!("{e:#}"))).is_err() || !quit.load(Ordering::Relaxed) {
            log::error!("WASAPI capture failed: {e:#}");
        }
    }
    closer.close();
    unsafe { CoUninitialize() };
    log::debug!("wasapi thread exiting");
}

fn capture_loop(
    wanted: &str,
    producer: &mut RingProducer,
    quit: &AtomicBool,
    ready_tx: &mpsc::Sender<Result<String>>,
) -> Result<()> {
    let (device, name) = find_render_endpoint(wanted)?;
    let fmt = pcm_format();

    // SAFETY: COM calls on interfaces owned by this thread; `fmt` outlives Initialize.
    unsafe {
        let capture_client: IAudioClient = device.Activate(CLSCTX_ALL, None).context("activate capture client")?;
        capture_client
            .Initialize(
                AUDCLNT_SHAREMODE_SHARED,
                AUDCLNT_STREAMFLAGS_LOOPBACK | AUDCLNT_STREAMFLAGS_AUTOCONVERTPCM | AUDCLNT_STREAMFLAGS_SRC_DEFAULT_QUALITY,
                BUFFER_HNS,
                0,
                &fmt,
                None,
            )
            .context("initialize loopback capture (48 kHz / 16 bit / stereo)")?;
        let capture: IAudioCaptureClient = capture_client.GetService().context("IAudioCaptureClient")?;

        // Loopback delivers no packets while nothing plays on the endpoint, which would stall the RTP stream
        // (PipeWire's `node.always-process` covers this on Linux). Rendering silence into the same endpoint keeps
        // the device clock — and therefore the packets — running.
        let render_client: IAudioClient = device.Activate(CLSCTX_ALL, None).context("activate keep-alive client")?;
        render_client
            .Initialize(
                AUDCLNT_SHAREMODE_SHARED,
                AUDCLNT_STREAMFLAGS_AUTOCONVERTPCM | AUDCLNT_STREAMFLAGS_SRC_DEFAULT_QUALITY,
                BUFFER_HNS,
                0,
                &fmt,
                None,
            )
            .context("initialize keep-alive render")?;
        let render: IAudioRenderClient = render_client.GetService().context("IAudioRenderClient")?;
        let render_frames = render_client.GetBufferSize().context("render buffer size")?;

        render_client.Start().context("start keep-alive render")?;
        capture_client.Start().context("start loopback capture")?;
        let _ = ready_tx.send(Ok(name));

        let frame_bytes = CHANNELS as usize * 2;
        let mut silence: Vec<u8> = Vec::new();
        while !quit.load(Ordering::Relaxed) {
            let padding = render_client.GetCurrentPadding().context("keep-alive padding")?;
            let free = render_frames.saturating_sub(padding);
            if free > 0 {
                render.GetBuffer(free).context("keep-alive buffer")?;
                render.ReleaseBuffer(free, AUDCLNT_BUFFERFLAGS_SILENT.0 as u32).context("keep-alive release")?;
            }

            loop {
                let next = capture.GetNextPacketSize().context("loopback packet size")?;
                if next == 0 {
                    break;
                }
                let mut data: *mut u8 = std::ptr::null_mut();
                let mut frames = 0u32;
                let mut flags = 0u32;
                capture.GetBuffer(&mut data, &mut frames, &mut flags, None, None).context("loopback buffer")?;
                let len = frames as usize * frame_bytes;
                if flags & AUDCLNT_BUFFERFLAGS_SILENT.0 as u32 != 0 || data.is_null() {
                    silence.resize(len, 0);
                    producer.write_s16le(&silence);
                } else {
                    producer.write_s16le(std::slice::from_raw_parts(data, len));
                }
                capture.ReleaseBuffer(frames).context("loopback release")?;
            }
            std::thread::sleep(POLL);
        }

        let _ = capture_client.Stop();
        let _ = render_client.Stop();
    }
    Ok(())
}

fn pcm_format() -> WAVEFORMATEX {
    let block_align = CHANNELS as u16 * 2;
    WAVEFORMATEX {
        wFormatTag: WAVE_FORMAT_PCM,
        nChannels: CHANNELS as u16,
        nSamplesPerSec: SAMPLE_RATE,
        nAvgBytesPerSec: SAMPLE_RATE * block_align as u32,
        nBlockAlign: block_align,
        wBitsPerSample: 16,
        cbSize: 0,
    }
}

/// Active render endpoints as (device, friendly name).
fn render_endpoints() -> Result<Vec<(IMMDevice, String)>> {
    // SAFETY: COM calls on this thread's apartment.
    unsafe {
        let enumerator: IMMDeviceEnumerator = CoCreateInstance(&MMDeviceEnumerator, None, CLSCTX_ALL).context("MMDeviceEnumerator")?;
        let coll = enumerator.EnumAudioEndpoints(eRender, DEVICE_STATE_ACTIVE).context("EnumAudioEndpoints")?;
        let mut out = Vec::new();
        for i in 0..coll.GetCount()? {
            let dev = coll.Item(i)?;
            let store = dev.OpenPropertyStore(STGM_READ)?;
            let name = store.GetValue(&PKEY_Device_FriendlyName)?.to_string();
            out.push((dev, name));
        }
        Ok(out)
    }
}

fn find_render_endpoint(wanted: &str) -> Result<(IMMDevice, String)> {
    let all = render_endpoints()?;
    let needle = wanted.to_lowercase();
    let mut hits: Vec<_> = all.iter().filter(|(_, n)| n.to_lowercase().contains(&needle)).collect();
    match hits.len() {
        1 => {
            let (d, n) = hits.pop().unwrap();
            Ok((d.clone(), n.clone()))
        }
        0 => {
            let names: Vec<_> = all.iter().map(|(_, n)| format!("  - {n}")).collect();
            bail!(
                "no active playback device matches \"{wanted}\". Install VB-Audio Virtual Cable or pass --capture-device.\nAvailable:\n{}",
                names.join("\n")
            )
        }
        _ => {
            let names: Vec<_> = hits.iter().map(|(_, n)| format!("  - {n}")).collect();
            bail!("\"{wanted}\" matches several playback devices, be more specific:\n{}", names.join("\n"))
        }
    }
}

/// Named event a launcher can signal to stop the cast cleanly (a hidden process has no console to send Ctrl+C to;
/// killing it would skip the receiver teardown). Signalling raises Ctrl+Break in our own console, which the
/// pipeline's ctrlc handler already turns into a graceful shutdown.
pub const STOP_EVENT_NAME: &str = "Local\\chromecast-sink-stop";

pub fn spawn_stop_event_listener() {
    use windows::core::HSTRING;
    use windows::Win32::System::Console::{GenerateConsoleCtrlEvent, CTRL_BREAK_EVENT};
    use windows::Win32::System::Threading::{CreateEventW, WaitForSingleObject, INFINITE};

    // SAFETY: plain Win32 event; the handle lives for the rest of the process.
    let event = match unsafe { CreateEventW(None, true, false, &HSTRING::from(STOP_EVENT_NAME)) } {
        Ok(h) => h,
        Err(e) => {
            log::warn!("stop event unavailable: {e}");
            return;
        }
    };
    let raw = event.0 as usize;
    let _ = std::thread::Builder::new().name("stop-event".into()).spawn(move || unsafe {
        let h = windows::Win32::Foundation::HANDLE(raw as *mut _);
        WaitForSingleObject(h, INFINITE);
        log::info!("stop event signalled");
        let _ = GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, 0);
    });
}

/// For `--list-devices`.
pub fn list_capture_devices() -> Result<Vec<String>> {
    unsafe { CoInitializeEx(None, COINIT_MULTITHREADED) }.ok().context("CoInitializeEx")?;
    let r = render_endpoints().map(|v| v.into_iter().map(|(_, n)| n).collect());
    unsafe { CoUninitialize() };
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pcm_format_is_48k_s16_stereo() {
        let f = pcm_format();
        assert_eq!({ f.nSamplesPerSec }, 48_000);
        assert_eq!({ f.wBitsPerSample }, 16);
        assert_eq!({ f.nChannels }, 2);
        assert_eq!({ f.nBlockAlign }, 4);
        assert_eq!({ f.nAvgBytesPerSec }, 192_000);
    }
}
