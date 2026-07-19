# chromecast-sink

Use Google Chromecast / Nest devices as a Linux speaker output.

Creates a PipeWire virtual sink that appears in your system's sound settings. Select it as your audio output and all sound is streamed to the Chromecast in real time over low-latency Cast Streaming (Opus over encrypted Cast RTP).

## Requirements

- **OS**: Linux with PipeWire (tested on Ubuntu 25.10, 26.04)
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
```

After starting, select **"Chromecast - \<device name\>"** as your audio output in Settings > Sound. Ctrl+C removes the virtual sink and disconnects.

## Scope

This tool does one thing: low-latency Cast Streaming to a single device.

If you want HTTP-based streaming instead — broader device compatibility and a virtual sink per discovered device, at the cost of several seconds of latency — use [p-cast](https://github.com/GenessyX/p-cast), which serves HLS segments over HTTP.

## Known Limitations

- **GNOME recording indicator**: a microphone icon appears in the top bar while casting (cosmetic only, audio works correctly)
- **No auto-reconnect**: if the Chromecast disconnects, the tool exits cleanly — restart to reconnect
