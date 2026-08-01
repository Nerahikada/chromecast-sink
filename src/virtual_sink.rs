//! PipeWire virtual sink — a native in-process adapter node.
//!
//! This is the biggest simplification over the Python version. We hold a
//! `pipewire::node::Node` proxy for the lifetime of our process; when we drop
//! it (either on clean shutdown or on process death), pipewire destroys the
//! node. That means no `object.linger=true`, no stale-node cleanup on next
//! startup, no `pw-cli` subprocess, no `pw-dump`/`pactl` output parsing —
//! all of which the Python side needed.
//!
//!   node.virtual=false       — GNOME hides virtual=true
//!   device.class=sound       — WirePlumber treats it as a real device
//!   media.class=Audio/Sink
//!   node.force-quantum=256   — capture-latency ceiling (~5.3 ms @48k)
//!   monitor.channel-volumes=true

use std::cell::Cell;
use std::rc::Rc;
use std::sync::mpsc;
use std::thread::JoinHandle;

use anyhow::{Context, Result};
use pipewire::{self as pw, main_loop::MainLoop, node::Node, properties::properties};

pub struct VirtualSink {
    #[allow(dead_code)] // used by test scripts (paplay --device=...)
    pub sink_name: String,
    pub monitor_source: String,
    quit_tx: Option<pw::channel::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl VirtualSink {
    pub fn create(device_name: &str) -> Result<Self> {
        let sink_name = sanitize_sink_name(device_name);
        let description = format!("Chromecast - {device_name}");

        let (quit_tx, quit_rx) = pw::channel::channel::<()>();
        let (ready_tx, ready_rx) = mpsc::channel::<Result<()>>();

        let sink_name_c = sink_name.clone();
        let thread = std::thread::Builder::new()
            .name("pw-sink".into())
            .spawn(move || run_pw_thread(sink_name_c, description, quit_rx, ready_tx))
            .expect("spawn pw-sink thread");

        // Wait for the node to be bound on the server.
        ready_rx
            .recv()
            .context("pipewire thread died before sink was ready")??;

        log::info!("Virtual sink ready: {}", sink_name);
        Ok(Self {
            monitor_source: format!("{sink_name}.monitor"),
            sink_name,
            quit_tx: Some(quit_tx),
            thread: Some(thread),
        })
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

fn run_pw_thread(
    sink_name: String,
    description: String,
    quit_rx: pw::channel::Receiver<()>,
    ready_tx: mpsc::Sender<Result<()>>,
) {
    pw::init();

    let mainloop = match MainLoop::new(None) {
        Ok(m) => m,
        Err(e) => { let _ = ready_tx.send(Err(anyhow::anyhow!("MainLoop: {e}"))); return; }
    };
    let context = match pw::context::Context::new(&mainloop) {
        Ok(c) => c,
        Err(e) => { let _ = ready_tx.send(Err(anyhow::anyhow!("Context: {e}"))); return; }
    };
    let core = match context.connect(None) {
        Ok(c) => c,
        Err(e) => { let _ = ready_tx.send(Err(anyhow::anyhow!("Connect: {e}"))); return; }
    };

    let props = properties! {
        "factory.name" => "support.null-audio-sink",
        "node.name" => sink_name.as_str(),
        "node.description" => description.as_str(),
        "device.description" => description.as_str(),
        "device.class" => "sound",
        "node.virtual" => "false",
        "media.class" => "Audio/Sink",
        "audio.position" => "[ FL FR ]",
        "monitor.channel-volumes" => "true",
        "node.force-quantum" => "256",
    };

    // The proxy MUST outlive the main loop — dropping it destroys the node on
    // the server. Since Node isn't Send, we keep it on this thread.
    let node = match core.create_object::<Node>("adapter", &props) {
        Ok(n) => n,
        Err(e) => { let _ = ready_tx.send(Err(anyhow::anyhow!("create_object: {e}"))); return; }
    };

    // Roundtrip so the server has actually processed the create before we
    // signal readiness (otherwise pulse-simple may race and fail to open the
    // monitor). See examples/roundtrip.rs in pipewire-rs.
    let sync_seq = match core.sync(0) {
        Ok(s) => s,
        Err(e) => { let _ = ready_tx.send(Err(anyhow::anyhow!("sync: {e}"))); return; }
    };

    let signalled = Rc::new(Cell::new(false));
    let signalled_c = Rc::clone(&signalled);
    let ready_tx_c = ready_tx.clone();
    let _core_listener = core
        .add_listener_local()
        .done(move |id, seq| {
            if id == pw::core::PW_ID_CORE && seq == sync_seq && !signalled_c.get() {
                signalled_c.set(true);
                let _ = ready_tx_c.send(Ok(()));
            }
        })
        .register();

    let mainloop_c = mainloop.clone();
    let _quit_source = quit_rx.attach(mainloop.loop_(), move |_| mainloop_c.quit());

    mainloop.run();
    // node dropped here → node destroyed on server → sink disappears from GNOME
    drop(node);
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
}
