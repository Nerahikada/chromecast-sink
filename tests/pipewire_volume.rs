//! Opt-in test against a private native PipeWire server, with no Cast hardware.
//! Start only through tests/run-pipewire-tests.py.

#![cfg(not(windows))]

use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use chromecast_sink::audio_ring::RingConsumer;
use chromecast_sink::virtual_sink::{VirtualSink, VolumeControl};
use chromecast_sink::volume::Volume;
use serde_json::Value;

const WAIT: Duration = Duration::from_secs(3);

struct Playback {
    child: Child,
    stop: Arc<AtomicBool>,
    writer: Option<JoinHandle<()>>,
}

impl Playback {
    fn new(target: &str) -> Self {
        let mut child = Command::new("pw-cat")
            .args([
                "--playback",
                "--raw",
                "--rate",
                "48000",
                "--channels",
                "2",
                "--format",
                "s16",
                "--target",
                target,
                "-",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .expect("start pw-cat");
        let mut input = child.stdin.take().expect("pw-cat stdin");
        let stop = Arc::new(AtomicBool::new(false));
        let stop_c = Arc::clone(&stop);
        let writer = thread::spawn(move || {
            let block: Vec<u8> = [12000_i16, -6000_i16]
                .iter()
                .flat_map(|sample| sample.to_le_bytes())
                .cycle()
                .take(480 * 4)
                .collect();
            while !stop_c.load(Ordering::Relaxed) {
                if input.write_all(&block).is_err() {
                    break;
                }
            }
        });
        Self {
            child,
            stop,
            writer: Some(writer),
        }
    }
}

impl Drop for Playback {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(writer) = self.writer.take() {
            let _ = writer.join();
        }
    }
}

fn volume(level: f32, muted: bool) -> Volume {
    Volume::new(level, muted).unwrap()
}

fn node_id(name: &str) -> u64 {
    let output = Command::new("pw-dump").output().expect("run pw-dump");
    assert!(output.status.success(), "pw-dump failed");
    let dump: Value = serde_json::from_slice(&output.stdout).expect("pw-dump JSON");
    dump.as_array()
        .unwrap()
        .iter()
        .find_map(|object| {
            (object
                .pointer("/info/props/node.name")
                .and_then(Value::as_str)
                == Some(name))
            .then(|| object["id"].as_u64().unwrap())
        })
        .expect("virtual sink node visible in private server")
}

fn set_props(node: u64, props: &str) {
    let output = Command::new("pw-cli")
        .args(["set-param", &node.to_string(), "Props", props])
        .output()
        .expect("run pw-cli set-param");
    assert!(
        output.status.success(),
        "pw-cli failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn wait_props(node: u64, expected: Volume) {
    let deadline = Instant::now() + WAIT;
    let mut last = Value::Null;
    while Instant::now() < deadline {
        let output = Command::new("pw-dump").output().expect("run pw-dump");
        assert!(output.status.success(), "pw-dump failed");
        let dump: Value = serde_json::from_slice(&output.stdout).unwrap();
        if let Some(object) = dump
            .as_array()
            .unwrap()
            .iter()
            .find(|object| object["id"] == node)
        {
            last = object
                .pointer("/info/params/Props")
                .cloned()
                .unwrap_or(Value::Null);
            if last.as_array().is_some_and(|parameters| {
                parameters.iter().any(|props| {
                    props["mute"].as_bool() == Some(expected.muted)
                        && props["channelVolumes"].as_array().is_some_and(|channels| {
                            channels.len() == 2
                                && channels.iter().all(|channel| {
                                    channel.as_f64().is_some_and(|gain| {
                                        (gain - f64::from(expected.level.powi(3))).abs() < 0.00001
                                    })
                                })
                        })
                })
            }) {
                return;
            }
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("native sink Props did not expose {expected:?}; last={last}");
}

fn wait_local(control: &VolumeControl, expected: Volume) {
    let deadline = Instant::now() + WAIT;
    let mut last = None;
    while Instant::now() < deadline {
        if let Some(change) = control.take_local_change() {
            last = Some(change);
            if change.approx_eq(expected) {
                return;
            }
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("no expected local change {expected:?}; last={last:?}");
}

fn assert_no_echo(control: &VolumeControl) {
    let deadline = Instant::now() + Duration::from_millis(150);
    while Instant::now() < deadline {
        assert_eq!(
            control.take_local_change(),
            None,
            "remote update echoed as local edit"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn assert_pcm(ring: &mut RingConsumer, expected: [i16; 2]) {
    // Discard audio buffered before the control change and allow graph startup.
    ring.skip_frames(ring.available_frames());
    thread::sleep(Duration::from_millis(80));
    ring.skip_frames(ring.available_frames());
    let deadline = Instant::now() + WAIT;
    let mut samples = [0_i16; 512];
    let mut last = [0_i16; 8];
    while Instant::now() < deadline {
        if ring.available_frames() > ring.capacity_frames() {
            ring.skip_frames(ring.available_frames());
        }
        if ring.read_frames(&mut samples) {
            last.copy_from_slice(&samples[..8]);
            if samples.chunks_exact(2).all(|frame| frame == expected) {
                return;
            }
        }
        assert!(!ring.is_closed(), "sink stopped during PCM check");
        thread::sleep(Duration::from_millis(2));
    }
    panic!("sink PCM did not match {expected:?}; last={last:?}");
}

fn assert_unity_pcm(ring: &mut RingConsumer) {
    assert_pcm(ring, [12000, -6000]);
}

#[test]
#[ignore = "requires private PipeWire; run python3 tests/run-pipewire-tests.py"]
fn receiver_controls_keep_native_pcm_at_unity() {
    assert_eq!(
        std::env::var("CHROMECAST_SINK_PRIVATE_PIPEWIRE_TEST").as_deref(),
        Ok("1")
    );
    let runtime = std::env::var("PIPEWIRE_RUNTIME_DIR").expect("private runtime");
    assert!(
        runtime.starts_with("/tmp/chromecast-sink-pw-"),
        "refusing non-test runtime"
    );
    assert_eq!(std::env::var("XDG_RUNTIME_DIR").unwrap(), runtime);

    let mut sink = VirtualSink::new("Volume Integration Test").expect("create virtual sink");
    let control = sink.volume_control();
    let mut ring = sink.take_consumer().unwrap();
    let _playback = Playback::new(&sink.sink_name);
    let node = node_id(&sink.sink_name);

    control
        .set_remote(volume(0.3, false))
        .expect("initialize receiver state");
    wait_props(node, volume(0.3, false));
    assert_no_echo(&control);
    assert_unity_pcm(&mut ring);

    for value in [
        volume(0.5, false),
        volume(0.0, false),
        volume(0.5, true),
        volume(1.0, false),
    ] {
        control.set_remote(value).expect("apply receiver state");
        wait_props(node, value);
        assert_no_echo(&control);
        assert_unity_pcm(&mut ring);
    }

    // Native PipeWire channel volumes are linear gain; desktop 50% is 0.5^3.
    set_props(node, "{ channelVolumes: [ 0.125, 0.125 ] }");
    wait_local(&control, volume(0.5, false));
    wait_props(node, volume(0.5, false));
    assert_unity_pcm(&mut ring);

    set_props(node, "{ mute: true }");
    wait_local(&control, volume(0.5, true));
    wait_props(node, volume(0.5, true));
    assert_unity_pcm(&mut ring);

    set_props(node, "{ channelVolumes: [ 0.0, 0.0 ], mute: false }");
    wait_local(&control, volume(0.0, false));
    wait_props(node, volume(0.0, false));
    assert_unity_pcm(&mut ring);

    set_props(node, "{ channelVolumes: [ 3.375, 3.375 ] }");
    wait_local(&control, volume(1.0, false));
    wait_props(node, volume(1.0, false));
    assert_unity_pcm(&mut ring);

    drop(_playback);
    drop(sink);
    let output = Command::new("pw-dump").output().unwrap();
    let dump: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(
        !dump.as_array().unwrap().iter().any(|object| {
            object
                .pointer("/info/props/node.name")
                .and_then(Value::as_str)
                == Some("chromecast_sink_volume_integration_test")
        }),
        "virtual sink remained registered after drop"
    );

    // The explicit fallback retains upstream software gain and mute, instead
    // of silently changing the default mode's semantics when Cast is offline.
    let mut local = VirtualSink::new_local("Local Volume Integration Test")
        .expect("create software-gain fallback sink");
    let mut local_ring = local.take_consumer().unwrap();
    let local_playback = Playback::new(&local.sink_name);
    let local_node = node_id(&local.sink_name);
    set_props(local_node, "{ channelVolumes: [ 1.0, 1.0 ], mute: false }");
    assert_unity_pcm(&mut local_ring);
    set_props(local_node, "{ channelVolumes: [ 0.125, 0.125 ] }");
    assert_pcm(&mut local_ring, [1500, -750]);
    set_props(local_node, "{ mute: true }");
    assert_pcm(&mut local_ring, [0, 0]);
    set_props(local_node, "{ channelVolumes: [ 1.0, 1.0 ], mute: false }");
    assert_unity_pcm(&mut local_ring);
    drop(local_playback);
    drop(local);
}
