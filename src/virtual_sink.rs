use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use std::{cell::RefCell, rc::Rc};

use anyhow::{anyhow, bail, Context, Result};
use pipewire::{
    self as pw,
    context::ContextRc,
    main_loop::MainLoopRc,
    properties::properties,
    spa,
    stream::{StreamFlags, StreamRc, StreamState},
};
use spa::pod::serialize::PodSerializer;
use spa::pod::{deserialize::PodDeserializer, Object, Property, Value, ValueArray};

use crate::audio_format::{CHANNELS, SAMPLE_RATE};
use crate::audio_ring::{self, RingConsumer, RingProducer};
use crate::volume::Volume;

/// ~170 ms at 48 kHz. Only has to absorb encoder-thread jitter.
const RING_CAPACITY_FRAMES: usize = 8192;

const READY_TIMEOUT: Duration = Duration::from_secs(10);

/// Control-thread endpoint. No Cast communication or locks enter the audio callback.
#[derive(Clone)]
pub struct VolumeControl {
    local: Arc<Mutex<LocalVolumeMailbox>>,
    remote: pw::channel::Sender<RemoteVolumeUpdate>,
}

impl VolumeControl {
    pub fn take_local_change(&self) -> Option<Volume> {
        self.local
            .lock()
            .expect("volume mailbox poisoned")
            .pending
            .take()
    }

    pub fn set_remote(&self, volume: Volume) -> Result<()> {
        let volume = Volume::new(volume.level, volume.muted).context("invalid receiver volume")?;
        let local_revision = self.local.lock().expect("volume mailbox poisoned").revision;
        // Release the mailbox lock before sending: the PipeWire receiver holds
        // its channel lock while invoking the callback, which takes this lock.
        self.remote
            .send(RemoteVolumeUpdate {
                volume,
                local_revision,
            })
            .map_err(|_| anyhow!("PipeWire volume channel closed"))
    }
}

#[derive(Default)]
struct LocalVolumeMailbox {
    pending: Option<Volume>,
    revision: u64,
}

impl LocalVolumeMailbox {
    fn publish(&mut self, volume: Volume) {
        self.pending = Some(volume);
        self.revision = self.revision.wrapping_add(1);
    }

    fn accept_remote(&mut self, local_revision: u64) -> bool {
        if self.revision != local_revision {
            return false;
        }
        self.pending = None;
        true
    }
}

struct RemoteVolumeUpdate {
    volume: Volume,
    local_revision: u64,
}

struct VolumeState {
    current: Volume,
    initialized: bool,
}

struct VolumeChannels {
    enabled: bool,
    remote: pw::channel::Receiver<RemoteVolumeUpdate>,
    local: Arc<Mutex<LocalVolumeMailbox>>,
}

pub struct VirtualSink {
    pub sink_name: String,
    consumer: Option<RingConsumer>,
    quit_tx: Option<pw::channel::Sender<()>>,
    thread: Option<JoinHandle<()>>,
    volume: VolumeControl,
}

impl VirtualSink {
    pub fn new(device_name: &str) -> Result<Self> {
        Self::new_with_mode(device_name, true)
    }

    /// Preserve upstream software attenuation for receivers without volume status.
    pub fn new_local(device_name: &str) -> Result<Self> {
        Self::new_with_mode(device_name, false)
    }

    fn new_with_mode(device_name: &str, sync_volume: bool) -> Result<Self> {
        let sink_name = sanitize_sink_name(device_name);
        let description = format!("Chromecast - {device_name}");
        let (producer, consumer) = audio_ring::channel(RING_CAPACITY_FRAMES, CHANNELS as usize);

        let (quit_tx, quit_rx) = pw::channel::channel::<()>();
        let (ready_tx, ready_rx) = mpsc::channel::<Result<()>>();
        let (remote, remote_rx) = pw::channel::channel::<RemoteVolumeUpdate>();
        let local = Arc::new(Mutex::new(LocalVolumeMailbox::default()));
        let local_c = Arc::clone(&local);

        let sink_name_c = sink_name.clone();
        let thread = std::thread::Builder::new()
            .name("pw-sink".into())
            .spawn(move || {
                run_pw_thread(
                    sink_name_c,
                    description,
                    producer,
                    quit_rx,
                    ready_tx,
                    VolumeChannels {
                        enabled: sync_volume,
                        remote: remote_rx,
                        local: local_c,
                    },
                )
            })
            .expect("spawn pw-sink thread");

        // `channel::Sender` has no `Drop`. A return before this leaves an orphan sink registered in PipeWire.
        let sink = Self {
            sink_name,
            consumer: Some(consumer),
            quit_tx: Some(quit_tx),
            thread: Some(thread),
            volume: VolumeControl { local, remote },
        };

        // Bounded so a hung pipewire server can't wedge the CLI at startup.
        match ready_rx.recv_timeout(READY_TIMEOUT) {
            Ok(r) => r?,
            Err(mpsc::RecvTimeoutError::Timeout) => bail!(
                "virtual sink was not started within {}s — is a session manager (wireplumber) running?",
                READY_TIMEOUT.as_secs()
            ),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                bail!("pipewire thread died before sink was ready")
            }
        }

        log::info!("Virtual sink ready: {}", sink.sink_name);
        Ok(sink)
    }

    /// Succeeds once. `VirtualSink` has a `Drop`, which prevents a field move.
    pub fn take_consumer(&mut self) -> Option<RingConsumer> {
        self.consumer.take()
    }

    pub fn volume_control(&self) -> VolumeControl {
        self.volume.clone()
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
    // The `audio.position` property is a no-op on a stream-backed node. The channel map only takes effect here, in the format.
    let mut position = [0u32; 64];
    position[0] = spa::sys::SPA_AUDIO_CHANNEL_FL;
    position[1] = spa::sys::SPA_AUDIO_CHANNEL_FR;
    info.set_position(position);

    let object = spa::pod::Object {
        type_: spa::sys::SPA_TYPE_OBJECT_Format,
        id: spa::sys::SPA_PARAM_EnumFormat,
        properties: info.into(),
    };
    let bytes = PodSerializer::serialize(
        std::io::Cursor::new(Vec::new()),
        &spa::pod::Value::Object(object),
    )
    .context("serialize EnumFormat")?
    .0
    .into_inner();
    Ok(bytes)
}

fn props_object(param: &spa::pod::Pod) -> Option<Object> {
    let (_, Value::Object(object)) =
        PodDeserializer::deserialize_from::<Value>(param.as_bytes()).ok()?
    else {
        return None;
    };
    (object.type_ == spa::sys::SPA_TYPE_OBJECT_Props).then_some(object)
}

/// PipeWire uses linear PCM gains; KDE/PulseAudio's percentage is their cube root.
/// Cast exposes a single volume, so an unbalanced desktop control uses its maximum.
fn volume_from_props(object: &Object, current: Volume) -> Volume {
    let mut scalar_gain = None;
    let mut channel_gain = None;
    let mut muted = current.muted;
    for property in &object.properties {
        match (&property.key, &property.value) {
            (&spa::sys::SPA_PROP_channelVolumes, Value::ValueArray(ValueArray::Float(values))) => {
                if let Some(gain) = values
                    .iter()
                    .copied()
                    .filter(|v| v.is_finite() && *v >= 0.0)
                    .reduce(f32::max)
                {
                    channel_gain = Some(gain);
                }
            }
            (&spa::sys::SPA_PROP_volume, Value::Float(gain))
                if gain.is_finite() && *gain >= 0.0 =>
            {
                scalar_gain = Some(*gain);
            }
            (&spa::sys::SPA_PROP_mute, Value::Bool(value)) => muted = *value,
            _ => {}
        }
    }
    let level = channel_gain
        .or(scalar_gain)
        .map_or(current.level, |gain| gain.cbrt().clamp(0.0, 1.0));
    Volume { level, muted }
}

fn unity_props(mut object: Object, volume: Volume) -> Result<Vec<u8>> {
    let controls = [
        (spa::sys::SPA_PROP_volume, Value::Float(1.0)),
        (spa::sys::SPA_PROP_mute, Value::Bool(volume.muted)),
        (
            spa::sys::SPA_PROP_channelVolumes,
            Value::ValueArray(ValueArray::Float(vec![
                volume.level.powi(3);
                CHANNELS as usize
            ])),
        ),
        (spa::sys::SPA_PROP_softMute, Value::Bool(false)),
        (
            spa::sys::SPA_PROP_softVolumes,
            Value::ValueArray(ValueArray::Float(vec![1.0; CHANNELS as usize])),
        ),
    ];
    for (key, value) in controls {
        object.properties.retain(|property| property.key != key);
        object.properties.push(Property::new(key, value));
    }
    object.id = spa::sys::SPA_PARAM_Props;
    Ok(
        PodSerializer::serialize(std::io::Cursor::new(Vec::new()), &Value::Object(object))
            .context("serialize sink controls")?
            .0
            .into_inner(),
    )
}

fn empty_props() -> Object {
    Object {
        type_: spa::sys::SPA_TYPE_OBJECT_Props,
        id: spa::sys::SPA_PARAM_Props,
        properties: Vec::new(),
    }
}

/// pipewire-rs 0.10 has no safe wrapper for this API (introduced in PipeWire 0.3.70).
/// The native API explicitly supports replacing adapter Props inside param_changed.
#[allow(unsafe_code)]
fn set_stream_props(stream: &pw::stream::Stream, bytes: &[u8]) -> Result<()> {
    let pod = spa::pod::Pod::from_bytes(bytes).context("invalid sink control pod")?;
    // SAFETY: stream is alive and accessed only from its owning main loop. The
    // validated Pod remains alive throughout this synchronous native call.
    let result = unsafe {
        pw::sys::pw_stream_set_param(
            stream.as_raw_ptr(),
            spa::sys::SPA_PARAM_Props,
            pod.as_raw_ptr(),
        )
    };
    if result < 0 {
        bail!(
            "set PipeWire sink controls: {}",
            std::io::Error::from_raw_os_error(-result)
        );
    }
    Ok(())
}

fn run_pw_thread(
    sink_name: String,
    description: String,
    mut producer: RingProducer,
    quit_rx: pw::channel::Receiver<()>,
    ready_tx: mpsc::Sender<Result<()>>,
    volume_channels: VolumeChannels,
) {
    let VolumeChannels {
        enabled: sync_volume,
        remote: remote_rx,
        local,
    } = volume_channels;
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
        let mainloop = MainLoopRc::new(None).context("MainLoop")?;
        let context = ContextRc::new(&mainloop, None).context("Context")?;
        let core = context.connect_rc(None).context("Connect")?;
        let stream = StreamRc::new(core.clone(), "chromecast-sink", props).context("Stream")?;
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
    let closer_controls = closer.clone();
    let volume_state = Rc::new(RefCell::new(VolumeState {
        current: Volume {
            level: 1.0,
            muted: false,
        },
        initialized: false,
    }));
    let volume_state_c = Rc::clone(&volume_state);
    let local_remote = Arc::clone(&local);

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
        .param_changed(move |stream, _, id, param| {
            if id == spa::sys::SPA_PARAM_Props && sync_volume {
                let Some(object) = param.and_then(props_object) else { return };
                let mut state = volume_state_c.borrow_mut();
                let volume = volume_from_props(&object, state.current);
                // Replace the complete adapter update synchronously, including
                // soft controls every time. An initial soft-volume pod alone
                // is insufficient: later desktop writes reset that mode.
                let result = unity_props(object, volume).and_then(|bytes| set_stream_props(stream, &bytes));
                match result {
                    Ok(()) => {
                        if state.initialized && !volume.approx_eq(state.current) {
                            local.lock().expect("volume mailbox poisoned").publish(volume);
                        }
                        state.current = volume;
                    }
                    Err(error) => {
                        log::error!("Cannot keep sink gain at unity: {error:#}");
                        closer_controls.close();
                    }
                }
                return;
            }
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
                    info.channels()
                );
            }
        })
        .process(move |stream, _| {
            // A cycle that skipped `process` leaves one permanently backed up.
            while let Some(mut buf) = stream.dequeue_buffer() {
                let Some(data) = buf.datas_mut().first_mut() else {
                    return;
                };
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

    if sync_volume {
        let result = unity_props(empty_props(), volume_state.borrow().current)
            .and_then(|bytes| set_stream_props(&stream, &bytes));
        if let Err(error) = result {
            let _ = ready_tx.send(Err(error));
            closer.close();
            return;
        }
    }

    let stream_c = stream.clone();
    let _volume_source = remote_rx.attach(mainloop.loop_(), move |update| {
        if !sync_volume {
            return;
        }
        let mut mailbox = local_remote.lock().expect("volume mailbox poisoned");
        // A desktop edit processed since this remote update was queued wins.
        // Otherwise discard older pending edits so they cannot echo back later.
        if !mailbox.accept_remote(update.local_revision) {
            return;
        }
        let volume = update.volume;
        match unity_props(empty_props(), volume)
            .and_then(|bytes| set_stream_props(&stream_c, &bytes))
        {
            Ok(()) => {
                let mut state = volume_state.borrow_mut();
                state.current = volume;
                state.initialized = true;
            }
            Err(error) => log::warn!("Cannot reflect Cast volume in PipeWire: {error:#}"),
        }
    });

    let mainloop_c = mainloop.clone();
    let _quit_source = quit_rx.attach(mainloop.loop_(), move |_| {
        shutdown.store(true, Ordering::Relaxed);
        mainloop_c.quit();
    });

    mainloop.run();

    // The loop can also exit on its own (`pw_loop_iterate` error). Unblock the caller rather than leaving it on the readiness timeout.
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
        assert_eq!(
            sanitize_sink_name("Living Room speaker"),
            "chromecast_sink_living_room_speaker"
        );
        assert_eq!(sanitize_sink_name("寝室"), "chromecast_sink");
        assert_eq!(
            sanitize_sink_name("Nest-Mini!"),
            "chromecast_sink_nest_mini"
        );
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

    fn parsed_controls(volume: Volume) -> Object {
        let bytes = unity_props(empty_props(), volume).unwrap();
        props_object(spa::pod::Pod::from_bytes(&bytes).unwrap()).unwrap()
    }

    #[test]
    fn desktop_percent_round_trips_through_cubic_pipewire_gains() {
        for level in [0.0, 0.1, 0.5, 1.0] {
            let volume = Volume { level, muted: true };
            let object = parsed_controls(volume);
            assert!(volume_from_props(
                &object,
                Volume {
                    level: 1.0,
                    muted: false
                }
            )
            .approx_eq(volume));
            let gains = object
                .properties
                .iter()
                .find(|p| p.key == spa::sys::SPA_PROP_channelVolumes)
                .unwrap();
            assert_eq!(
                gains.value,
                Value::ValueArray(ValueArray::Float(vec![level.powi(3); CHANNELS as usize]))
            );
        }
    }

    #[test]
    fn every_device_control_update_retains_unity_soft_gain_including_zero_and_mute() {
        for level in [0.0, 0.5, 1.0] {
            for muted in [false, true] {
                let object = parsed_controls(Volume { level, muted });
                for (key, expected) in [
                    (spa::sys::SPA_PROP_softMute, Value::Bool(false)),
                    (
                        spa::sys::SPA_PROP_softVolumes,
                        Value::ValueArray(ValueArray::Float(vec![1.0; CHANNELS as usize])),
                    ),
                    (spa::sys::SPA_PROP_volume, Value::Float(1.0)),
                ] {
                    assert_eq!(
                        object
                            .properties
                            .iter()
                            .find(|p| p.key == key)
                            .unwrap()
                            .value,
                        expected
                    );
                }
            }
        }
    }

    #[test]
    fn partial_changes_preserve_other_control_and_collapse_channels_to_maximum() {
        let mut object = empty_props();
        object
            .properties
            .push(Property::new(spa::sys::SPA_PROP_mute, Value::Bool(true)));
        let current = Volume {
            level: 0.5,
            muted: false,
        };
        assert_eq!(
            volume_from_props(&object, current),
            Volume {
                level: 0.5,
                muted: true
            }
        );
        object.properties.push(Property::new(
            spa::sys::SPA_PROP_channelVolumes,
            Value::ValueArray(ValueArray::Float(vec![0.125, 0.512])),
        ));
        assert!((volume_from_props(&object, current).level - 0.8).abs() < 0.0001);
        object.properties.push(Property::new(
            spa::sys::SPA_PROP_channelVolumes,
            Value::ValueArray(ValueArray::Float(vec![8.0, 8.0])),
        ));
        assert_eq!(volume_from_props(&object, current).level, 1.0);
    }

    #[test]
    fn replacing_gain_controls_preserves_unrelated_props_without_duplicates() {
        let mut object = empty_props();
        object
            .properties
            .push(Property::new(spa::sys::SPA_PROP_rate, Value::Double(1.0)));
        object.properties.push(Property::new(
            spa::sys::SPA_PROP_softMute,
            Value::Bool(true),
        ));
        let bytes = unity_props(
            object,
            Volume {
                level: 0.5,
                muted: true,
            },
        )
        .unwrap();
        let object = props_object(spa::pod::Pod::from_bytes(&bytes).unwrap()).unwrap();
        assert_eq!(
            object
                .properties
                .iter()
                .filter(|p| p.key == spa::sys::SPA_PROP_softMute)
                .count(),
            1
        );
        assert!(object
            .properties
            .iter()
            .any(|p| p.key == spa::sys::SPA_PROP_rate && p.value == Value::Double(1.0)));
    }

    #[test]
    fn scalar_volume_is_supported_but_channels_take_priority_in_either_order() {
        let mut object = empty_props();
        object.properties.push(Property::new(
            spa::sys::SPA_PROP_volume,
            Value::Float(0.125),
        ));
        let initial = Volume {
            level: 1.0,
            muted: false,
        };
        assert_eq!(volume_from_props(&object, initial).level, 0.5);
        object.properties.push(Property::new(
            spa::sys::SPA_PROP_channelVolumes,
            Value::ValueArray(ValueArray::Float(vec![0.512, 0.512])),
        ));
        assert!((volume_from_props(&object, initial).level - 0.8).abs() < 0.0001);
        object.properties.reverse();
        assert!((volume_from_props(&object, initial).level - 0.8).abs() < 0.0001);
    }

    #[test]
    fn authoritative_remote_update_discards_older_pending_desktop_change() {
        let mut mailbox = LocalVolumeMailbox::default();
        mailbox.publish(Volume {
            level: 0.2,
            muted: false,
        });
        assert!(mailbox.accept_remote(mailbox.revision));
        assert_eq!(mailbox.pending, None);
    }

    #[test]
    fn queued_remote_update_cannot_erase_a_newer_desktop_change() {
        let mut mailbox = LocalVolumeMailbox::default();
        mailbox.publish(Volume {
            level: 0.2,
            muted: false,
        });
        let queued_revision = mailbox.revision;
        let newer = Volume {
            level: 0.7,
            muted: true,
        };
        mailbox.publish(newer);
        assert!(!mailbox.accept_remote(queued_revision));
        assert_eq!(mailbox.pending, Some(newer));
    }

    #[test]
    fn public_receiver_update_rejects_nonfinite_volume() {
        let (remote, _receiver) = pw::channel::channel();
        let control = VolumeControl {
            local: Arc::new(Mutex::new(LocalVolumeMailbox::default())),
            remote,
        };
        for level in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            assert!(control
                .set_remote(Volume {
                    level,
                    muted: false
                })
                .is_err());
        }
    }
}
