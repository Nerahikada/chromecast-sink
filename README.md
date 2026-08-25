# chromecast-sink

Use Google Chromecast / Nest devices as a Linux speaker output.

## Requirements

**OS**: Linux with PipeWire (tested on Ubuntu 25.10, 26.04).

**Build**: Rust 1.85+ and:

```bash
sudo apt install build-essential libpipewire-0.3-dev libopus-dev libclang-dev pkg-config
```

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

After starting, select **"Chromecast - \<device name\>"** as your audio output in Settings > Sound.

## Scope

This tool does one thing: low-latency Cast Streaming to a single device.

If you want HTTP-based streaming instead — broader device compatibility and a virtual sink per discovered device, at the cost of several seconds of latency — use [p-cast](https://github.com/GenessyX/p-cast), which serves HLS segments over HTTP.

## Known Limitations

- **No auto-reconnect**: if the Chromecast disconnects, the tool exits cleanly — restart to reconnect
