use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use pipewire::{
    self as pw,
    main_loop::MainLoop,
    properties::properties,
    spa,
    stream::{Stream, StreamFlags, StreamState},
};
use spa::pod::serialize::PodSerializer;

use crate::audio_ring::{self, RingConsumer, RingProducer};
use crate::capture::{CHANNELS, SAMPLE_RATE};

/// ~170 ms at 48 kHz; only has to absorb encoder-thread jitter.
const RING_CAPACITY_FRAMES: usize = 8192;

const READY_TIMEOUT: Duration = Duration::from_secs(10);

pub struct VirtualSink {
    pub sink_name: String,
    consumer: Option<RingConsumer>,
    quit_tx: Option<pw::channel::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl VirtualSink {
    pub fn new(device_name: &str) -> Result<Self> {
        let sink_name = sanitize_sink_name(device_name);
        let description = format!("Chromecast - {device_name}");
        let (producer, consumer) = audio_ring::channel(RING_CAPACITY_FRAMES, CHANNELS as usize);

        let (quit_tx, quit_rx) = pw::channel::channel::<()>();
        let (ready_tx, ready_rx) = mpsc::channel::<Result<()>>();

        let sink_name_c = sink_name.clone();
        let thread = std::thread::Builder::new()
            .name("pw-sink".into())
            .spawn(move || run_pw_thread(sink_name_c, description, producer, quit_rx, ready_tx))
            .expect("spawn pw-sink thread");

        // `channel::Sender` has no `Drop`, so a return before this leaves an orphan sink registered in PipeWire.
        let sink = Self {
            sink_name,
            consumer: Some(consumer),
            quit_tx: Some(quit_tx),
            thread: Some(thread),
        };

        // Bounded so a hung pipewire server can't wedge the CLI at startup.
        match ready_rx.recv_timeout(READY_TIMEOUT) {
            Ok(r) => r?,
            Err(mpsc::RecvTimeoutError::Timeout) => bail!("virtual sink was not started within {}s — is a session manager (wireplumber) running?", READY_TIMEOUT.as_secs()),
            Err(mpsc::RecvTimeoutError::Disconnected) => bail!("pipewire thread died before sink was ready"),
        }

        log::info!("Virtual sink ready: {}", sink.sink_name);
        Ok(sink)
    }

    /// Succeeds once; `VirtualSink` has a `Drop` so this cannot be a field move.
    pub fn take_consumer(&mut self) -> Option<RingConsumer> {
        self.consumer.take()
    }
}

impl Drop for VirtualSink {
    fn drop(&mut self) {
        if let Some(tx) = self.quit_tx.take() {
            let _ = tx.send(());
        }
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn sanitize_sink_name(device_name: &str) -> String {
    let mut out = String::from("chromecast_sink_");
    let mut prev_us = true;
    for c in device_name.chars() {
        if c.is_ascii_alphanumeric() {
            out.extend(c.to_lowercase());
            prev_us = false;
        } else if !prev_us {
            out.push('_');
            prev_us = true;
        }
    }
    while out.ends_with('_') {
        out.pop();
    }
    out
}

fn enum_format_pod() -> Result<Vec<u8>> {
    let mut info = spa::param::audio::AudioInfoRaw::new();
    info.set_format(spa::param::audio::AudioFormat::S16LE);
    info.set_rate(SAMPLE_RATE);
    info.set_channels(CHANNELS as u32);
    // The `audio.position` property is a no-op on a stream-backed node; the channel map only takes effect here, in the format.
    let mut position = [0u32; 64];
    position[0] = spa::sys::SPA_AUDIO_CHANNEL_FL;
    position[1] = spa::sys::SPA_AUDIO_CHANNEL_FR;
    info.set_position(position);

    let bytes = PodSerializer::serialize(
        std::io::Cursor::new(Vec::new()),
        &spa::pod::Value::Object(spa::pod::Object {
            type_: spa::sys::SPA_TYPE_OBJECT_Format,
            id: spa::sys::SPA_PARAM_EnumFormat,
            properties: info.into(),
        }),
    )
    .context("serialize EnumFormat")?
    .0
    .into_inner();
    Ok(bytes)
}

fn run_pw_thread(
    sink_name: String,
    description: String,
    mut producer: RingProducer,
    quit_rx: pw::channel::Receiver<()>,
    ready_tx: mpsc::Sender<Result<()>>,
) {
    pw::init();
    let closer = producer.closer();

    // Without `node.always-process` the node stops being processed whenever no client is playing, which stalls the RTP stream to the receiver.
    let props = properties! {
        "media.class" => "Audio/Sink",
        "node.name" => sink_name.as_str(),
        "node.description" => description.as_str(),
        "device.class" => "sound",
        "device.icon-name" => "audio-speakers",
        "node.virtual" => "true",
        "node.force-quantum" => "256",
        "node.always-process" => "true",
        "node.suspend-on-idle" => "false",
    };

    let init: Result<_> = (|| {
        let mainloop = MainLoop::new(None).context("MainLoop")?;
        let context = pw::context::Context::new(&mainloop).context("Context")?;
        let core = context.connect(None).context("Connect")?;
        let stream = Stream::new(&core, "chromecast-sink", props).context("Stream")?;
        let pod = enum_format_pod()?;
        Ok((mainloop, context, core, stream, pod))
    })();
    let (mainloop, _context, _core, stream, pod) = match init {
        Ok(v) => v,
        Err(e) => {
            let _ = ready_tx.send(Err(e));
            closer.close();
            return;
        }
    };

    let signalled = Arc::new(AtomicBool::new(false));
    let shutdown = Arc::new(AtomicBool::new(false));
    let signalled_c = Arc::clone(&signalled);
    let shutdown_c = Arc::clone(&shutdown);
    let ready_tx_c = ready_tx.clone();
    let closer_state = closer.clone();

    // Must be declared *below* `stream`: locals drop in reverse, and unlinking this hook after `pw_stream_destroy` would touch freed memory.
    let listener = stream
        .add_local_listener::<()>()
        .state_changed(move |_, _, old, new| {
            log::debug!("sink stream {old:?} -> {new:?}");
            match new {
                StreamState::Streaming => {
                    if !signalled_c.swap(true, Ordering::Relaxed) {
                        let _ = ready_tx_c.send(Ok(()));
                    }
                }
                // Nothing else would ever notice the audio stopping.
                StreamState::Error(ref e) => {
                    if !signalled_c.swap(true, Ordering::Relaxed) {
                        let _ = ready_tx_c.send(Err(anyhow!("pipewire stream error: {e}")));
                    } else {
                        log::error!("sink stream failed: {e}");
                    }
                    closer_state.close();
                }
                StreamState::Unconnected
                    if signalled_c.load(Ordering::Relaxed)
                        && !shutdown_c.load(Ordering::Relaxed) =>
                {
                    log::error!("sink stream was disconnected");
                    closer_state.close();
                }
                _ => {}
            }
        })
        .param_changed(|_, _, id, param| {
            if id != spa::sys::SPA_PARAM_Format {
                return;
            }
            let Some(param) = param else { return };
            let mut info = spa::param::audio::AudioInfoRaw::new();
            if info.parse(param).is_err() {
                return;
            }
            if info.format() != spa::param::audio::AudioFormat::S16LE
                || info.rate() != SAMPLE_RATE
                || info.channels() != CHANNELS as u32
            {
                log::error!(
                    "sink negotiated {:?} / {} Hz / {} ch, expected S16LE / {SAMPLE_RATE} / {CHANNELS}",
                    info.format(),
                    info.rate(),
                    info.channels(),
                );
            }
        })
        .process(move |stream, _| {
            // A cycle that skipped `process` leaves one permanently backed up.
            while let Some(mut buf) = stream.dequeue_buffer() {
                let Some(data) = buf.datas_mut().first_mut() else { return };
                let chunk_offset = data.chunk().offset() as usize;
                let size = data.chunk().size() as usize;
                let Some(slice) = data.data() else { return };
                if slice.is_empty() {
                    return;
                }
                // `data()` spans maxsize and `offset` is defined modulo it.
                let offset = chunk_offset % slice.len();
                let end = offset.saturating_add(size).min(slice.len());
                if offset < end {
                    producer.write_s16le(&slice[offset..end]);
                }
            }
        })
        .register();
    let _listener = match listener {
        Ok(l) => l,
        Err(e) => {
            let _ = ready_tx.send(Err(anyhow!("register stream listener: {e}")));
            closer.close();
            return;
        }
    };

    let mut params = [spa::pod::Pod::from_bytes(&pod).expect("EnumFormat pod is well-formed")];
    if let Err(e) = stream.connect(
        spa::utils::Direction::Input,
        None,
        StreamFlags::MAP_BUFFERS | StreamFlags::RT_PROCESS,
        &mut params,
    ) {
        let _ = ready_tx.send(Err(anyhow!("connect sink stream: {e}")));
        closer.close();
        return;
    }

    let mainloop_c = mainloop.clone();
    let _quit_source = quit_rx.attach(mainloop.loop_(), move |_| {
        shutdown.store(true, Ordering::Relaxed);
        mainloop_c.quit();
    });

    mainloop.run();

    // The loop can also exit on its own (`pw_loop_iterate` error); unblock the caller rather than leaving it on the readiness timeout.
    if !signalled.swap(true, Ordering::Relaxed) {
        let _ = ready_tx.send(Err(anyhow!("pipewire loop exited before the sink started")));
    }
    closer.close();
    let _ = stream.disconnect();
    log::debug!("pw-sink thread exiting");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_name_sanitize() {
        assert_eq!(sanitize_sink_name("Living Room speaker"), "chromecast_sink_living_room_speaker");
        assert_eq!(sanitize_sink_name("寝室"), "chromecast_sink");
        assert_eq!(sanitize_sink_name("Nest-Mini!"), "chromecast_sink_nest_mini");
    }

    #[test]
    fn enum_format_pod_is_parseable() {
        let bytes = enum_format_pod().expect("build pod");
        let pod = spa::pod::Pod::from_bytes(&bytes).expect("parse pod");
        let mut info = spa::param::audio::AudioInfoRaw::new();
        info.parse(pod).expect("parse audio info");
        assert_eq!(info.format(), spa::param::audio::AudioFormat::S16LE);
        assert_eq!(info.rate(), SAMPLE_RATE);
        assert_eq!(info.channels(), CHANNELS as u32);
        assert_eq!(info.position()[0], spa::sys::SPA_AUDIO_CHANNEL_FL);
        assert_eq!(info.position()[1], spa::sys::SPA_AUDIO_CHANNEL_FR);
    }
}
