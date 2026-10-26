# chromecast-sink

Use Google Chromecast / Nest devices as speaker output.

## Usage

```bash
# Auto-discover and connect
chromecast-sink

# Specify device by name
chromecast-sink --device "Living Room speaker"

# Skip mDNS, connect by address (--port defaults to 8009)
chromecast-sink --host 192.168.1.20
```

On Linux, chromecast-sink creates a PipeWire virtual sink. After starting, select **"Chromecast - \<device name\>"** as your output in Settings > Sound.

### Windows

Windows support is experimental. Basic usage is the same, but chromecast-sink captures a playback device instead of creating a virtual sink, so a few extra flags are available:

```powershell
# List capturable playback devices
chromecast-sink.exe --list-devices

# Capture a specific playback device
chromecast-sink.exe --capture-device "Speakers"
```

To get a dedicated output you can switch to, like the Linux sink, install a virtual audio cable such as [VB-Audio Virtual Cable](https://vb-audio.com/Cable/), run with `--capture-device "CABLE Input"` and select **CABLE Input** as your output in Settings > Sound.

## Build

### Linux

Requires Rust 1.85+ and:

```bash
sudo apt install build-essential libpipewire-0.3-dev libopus-dev libclang-dev pkg-config
```

To build and install:

```bash
git clone https://github.com/Nerahikada/chromecast-sink.git
cd chromecast-sink
cargo build --release       # builds to target/release/chromecast-sink
cargo install --path .      # or, build and install to ~/.cargo/bin/
```

### Windows

Rust 1.85+, Visual Studio Build Tools (C++), and CMake.

## Scope

chromecast-sink uses Cast Streaming to send audio over UDP with low latency.

It has only been tested on a Google Nest Mini, because that is the only Cast hardware the maintainer owns. Other devices and speaker groups are in scope but untested, so reports and pull requests are welcome.

If you would rather have broader device compatibility and a virtual sink for every discovered device, and can live with several seconds of latency, use [p-cast](https://github.com/GenessyX/p-cast), which serves HLS segments over HTTP.
