# chromecast-sink

Use Google Chromecast / Nest devices as a Linux speaker output.

Creates a PipeWire virtual sink that appears in your system's sound settings. Select it as your audio output and all sound is streamed to the Chromecast in real time.

## Features

- **Cast Streaming** — Low-latency UDP streaming with Opus encoding and AES-128-CTR encrypted Cast RTP
- **No subprocesses** — Captures and encodes in-process via libpulse and libopus; nothing to spawn, nothing to pipe
- **PipeWire native** — Creates a proper audio sink visible in GNOME Sound Settings
- **Auto-discovery** — Finds Chromecast/Nest devices on your network via mDNS
- **Clean lifecycle** — Ctrl+C gracefully removes the virtual sink and disconnects

## Requirements

- **OS**: Linux with PipeWire (tested on Ubuntu 25.10)
- **Python**: 3.11+
- **System packages**:
  ```bash
  sudo apt install libopus0 pulseaudio-utils pipewire
  ```

## Installation

```bash
git clone https://github.com/Nerahikada/chromecast-sink.git
cd chromecast-sink
pip install -e .
```

## Usage

```bash
# Stream to the only device found, or pick from a menu
chromecast-sink

# Specify device by name
chromecast-sink --device "Living Room speaker"

# Debug logging
chromecast-sink --verbose
```

After starting, select **"Chromecast - \<device name\>"** as your audio output in Settings > Sound.

### Options

```
  -d, --device NAME        Select Chromecast by name
  --target-delay MS        Target playout delay in ms (default: 0)
  --opus-bitrate RATE      Opus bitrate, e.g. 128k (default: 128k)
  -t, --timeout SECS       Discovery timeout (default: 10)
  -v, --verbose            Enable debug logging
```

## Architecture

```
PipeWire Virtual Sink
  └─ monitor ──> libpulse capture ──> libopus encode
                                           │
                                 AES-128-CTR encrypt
                                 Cast RTP packetize
                                           │
                                     UDP ──> Chromecast
```

Audio is captured from the virtual sink's monitor source and encoded as 10ms Opus frames, then encrypted and packetized using the Cast Streaming protocol (a custom RTP profile, not WebRTC/SRTP) before being sent over UDP.

Capture and encoding both run in-process through `ctypes` bindings to the system's `libpulse` and `libopus`. A blocking read of one fragment per Opus frame paces the loop at real time, so no timer or buffering layer is involved.

## Scope

This tool does one thing: low-latency Cast Streaming to a single device.

If you want HTTP-based streaming instead — broader device compatibility, and a virtual sink per discovered device, at the cost of several seconds of latency — use [p-cast](https://github.com/GenessyX/p-cast), which serves HLS segments over HTTP.

## Known Limitations

- **GNOME recording indicator**: A microphone icon appears in the top bar while casting (cosmetic only, audio works correctly)
- **No auto-reconnect**: If the Chromecast disconnects, the tool exits cleanly — restart to reconnect
- **Single device**: Streams to one Chromecast at a time
