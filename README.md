# chromecast-sink

Use Google Chromecast / Nest devices as a Linux speaker output.

Creates a PipeWire virtual sink that appears in your system's sound settings. Select it as your audio output and all sound is streamed to the Chromecast in real time over low-latency Cast Streaming (Opus over encrypted Cast RTP).

## Requirements

**OS**: Linux with PipeWire (tested on Ubuntu 25.10, 26.04).

### Runtime

Shared libraries the compiled binary links against:

```bash
sudo apt install libpipewire-0.3-0t64 libpulse0 libopus0 libssl3t64
```

The PipeWire stack itself (`pipewire`, `pipewire-pulse`, `wireplumber`) is preinstalled on any modern Ubuntu desktop. On a headless machine, install those explicitly too.

### Build

Rust 1.85+ (install via `rustup`; the floor comes from `clap` 4.6's edition-2024 deps) plus:

```bash
sudo apt install libpipewire-0.3-dev libpulse-dev libopus-dev libssl-dev libclang-dev pkg-config
```

`libclang-dev` is used by `bindgen` (pulled in by `pipewire-sys`) and `pkg-config` locates `.pc` files; neither is linked into the final binary. The four `-dev` packages each depend on their runtime counterpart above, so on a build-and-run machine this line alone is sufficient.

## Installation

```bash
git clone https://github.com/Nerahikada/chromecast-sink.git
cd chromecast-sink
cargo install --path .
```

## Usage

```bash
# Stream to the only device found, or list them and ask for --device
chromecast-sink

# Specify device by name
chromecast-sink --device "Living Room speaker"
```

After starting, select **"Chromecast - \<device name\>"** as your audio output in Settings > Sound. Ctrl+C removes the virtual sink and disconnects.

## Scope

This tool does one thing: low-latency Cast Streaming to a single device.

If you want HTTP-based streaming instead — broader device compatibility and a virtual sink per discovered device, at the cost of several seconds of latency — use [p-cast](https://github.com/GenessyX/p-cast), which serves HLS segments over HTTP.

## Known Limitations

- **GNOME recording indicator**: a microphone icon appears in the top bar while casting (cosmetic only, audio works correctly)
- **No auto-reconnect**: if the Chromecast disconnects, the tool exits cleanly — restart to reconnect
