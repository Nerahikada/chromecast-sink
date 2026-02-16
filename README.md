# chromecast-sink

Use Google Chromecast / Nest devices as a Linux speaker output.

Creates a PipeWire virtual sink that appears in your system's sound settings. Select it as your audio output and all sound is streamed to the Chromecast in real time.

## Features

- **Cast Streaming mode** (default) — ~350ms latency via UDP, Opus encoding, AES-128-CTR encrypted Cast RTP
- **Legacy HTTP mode** — WAV/FLAC/MP3 streaming, ~2-10s latency, broader device compatibility
- **PipeWire native** — Creates a proper audio sink visible in GNOME Sound Settings
- **Auto-discovery** — Finds Chromecast/Nest devices on your network via mDNS
- **Clean lifecycle** — Ctrl+C gracefully removes the virtual sink and disconnects

## Requirements

- **OS**: Linux with PipeWire (tested on Ubuntu 25.10)
- **Python**: 3.10+
- **System packages**:
  ```bash
  sudo apt install ffmpeg pulseaudio-utils pipewire
  ```
  FFmpeg must be built with libopus support (included in Ubuntu's default ffmpeg package).

## Installation

```bash
git clone https://github.com/Nerahikada/chromecast-sink.git
cd chromecast-sink
pip install -e .
```

## Usage

```bash
# Default: Cast Streaming mode (~350ms latency)
chromecast-sink

# Specify device by name
chromecast-sink --device "Living Room speaker"

# Legacy HTTP mode (broader compatibility, higher latency)
chromecast-sink --mode http

# Debug logging
chromecast-sink --verbose
```

After starting, select **"Chromecast - \<device name\>"** as your audio output in Settings > Sound.

### Options

```
Mode:
  --mode {stream,http}     Cast Streaming (UDP) or legacy HTTP (default: stream)

Stream mode:
  --target-delay MS        Target playout delay in ms (default: 0)
  --opus-bitrate RATE      Opus bitrate, e.g. 128k (default: 128k)

HTTP mode:
  --format {wav,wav16,flac,mp3}  Audio format (default: wav)
  --bitrate RATE           MP3 bitrate (default: 320k)
  --port PORT              HTTP server port (default: auto)

General:
  -d, --device NAME        Select Chromecast by name
  -t, --timeout SECS       Discovery timeout (default: 10)
  -v, --verbose            Enable debug logging
```

## Architecture

### Cast Streaming mode (`--mode stream`)

```
PipeWire Virtual Sink
  └─ monitor ──> FFmpeg (Opus encode) ──RTP──> Python relay
                                                  │
                                        AES-128-CTR encrypt
                                        Cast RTP packetize
                                                  │
                                          UDP ──> Chromecast
```

Audio is captured from the virtual sink's monitor, encoded as Opus by FFmpeg, then encrypted and packetized using the Cast Streaming protocol (custom RTP, not WebRTC/SRTP) before being sent over UDP.

### Legacy HTTP mode (`--mode http`)

```
PipeWire Virtual Sink
  └─ monitor ──> FFmpeg (WAV/MP3/FLAC) ──pipe──> HTTP Server
                                                      │
                                        Chromecast pulls audio stream
```

Audio is encoded by FFmpeg and served via a local HTTP server. The Chromecast pulls the stream using its Default Media Receiver.

## Known Limitations

- **GNOME recording indicator**: A microphone icon appears in the top bar while casting (cosmetic only, audio works correctly)
- **No auto-reconnect**: If the Chromecast disconnects, the tool exits cleanly — restart to reconnect
- **Single device**: Streams to one Chromecast at a time
