# chromecast-sink

Use Google Chromecast / Nest devices as a Linux speaker output.

## Requirements

**OS**: Linux with PipeWire (tested on Ubuntu 24.04, 26.04).

22.04 works too, but you'll need to switch its audio stack from PulseAudio to PipeWire first.

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

## Windows

Windows can't create a virtual output device from a normal app, so on Windows chromecast-sink captures an existing playback device via WASAPI loopback instead. Install [VB-Audio Virtual Cable](https://vb-audio.com/Cable/) (free) and use its **CABLE Input** device, which is the default:

```powershell
chromecast-sink.exe --list-devices                       # playback devices it can capture
chromecast-sink.exe --device "Living Room speaker"       # casts whatever plays on "CABLE Input"
chromecast-sink.exe --device "Living Room speaker" --capture-device "Speakers"
```

Then select **CABLE Input** as your output in Settings > Sound. Nothing plays locally on that device, so it behaves like the Linux sink. Windows may ask for a firewall exception on the first run; mDNS discovery needs inbound UDP on private networks.

**Build**: Rust 1.85+, Visual Studio Build Tools (C++), and CMake.

## Scope

chromecast-sink uses Cast Streaming to send audio over UDP with low latency.

It has only been tested on a Google Nest Mini, because that is the only Cast hardware the maintainer owns. Other devices and speaker groups are in scope but untested, so reports and pull requests are welcome.

If you would rather have broader device compatibility and a virtual sink for every discovered device, and can live with several seconds of latency, use [p-cast](https://github.com/GenessyX/p-cast), which serves HLS segments over HTTP.

## Known Limitations

- **No auto-reconnect**: if the Chromecast disconnects, the tool exits cleanly — restart to reconnect
