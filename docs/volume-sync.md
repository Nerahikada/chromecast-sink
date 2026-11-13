# Linux receiver volume synchronization

The Linux sink exposes the selected Cast receiver's volume and mute state to
PipeWire. Changing the output-device slider in KDE sends receiver-level Cast
commands, allowing the speaker volume to rise above its previous setting.
Application-specific volume controls still adjust the individual application's
audio before it reaches the sink.

## Behavior and scope

This Linux build requires PipeWire 0.3.70 or newer at build time and runtime. The Rust bindings do not
wrap its synchronous `pw_stream_set_param` API, so this integration uses a narrow
FFI call to replace incoming sink gain properties before the audio converter
applies them. A failure to preserve unity gain stops capture rather than silently
continuing with double attenuation.

- The receiver's first valid volume status initializes the sink. Starting the
  program does not intentionally raise the receiver volume or unmute it.
- The sink's volume and mute controls represent receiver controls. Its audio
  samples remain at unity sink gain, including when the displayed receiver
  volume is zero or muted. This avoids attenuating both the PCM stream and the
  speaker. Source application gain and processing remain upstream of the sink.
- Cast volume is limited to 0–100%. KDE amplification above 100% cannot amplify
  the Cast receiver above its maximum. The receiver can quantize the requested
  value, so the displayed value can settle to its reported value.
- Native PipeWire gain values are converted to and from the desktop's cubic
  percentage scale: KDE's 50% corresponds to Cast level 0.5. Cast exposes a single
  level; if desktop channel volumes differ, the highest channel percentage is
  used and both sink channels are updated to that level.
- Receiver status notifications update PipeWire. Periodic `GET_STATUS` requests
  also check the current state. External changes in Google Home or on the
  speaker are reflected only when the selected Cast endpoint reports them in
  `RECEIVER_STATUS`. This is conditional bidirectional synchronization, not a
  guarantee that every external change is observable.
- Rapid slider changes are coalesced. Receiver echoes and sink updates caused
  by receiver status do not generate another volume command. Failed or
  unconfirmed commands are reconciled against receiver status rather than
  retried indefinitely.
- A receiver volume error does not by itself tear down the audio stream.
  Watch the warning logs when a receiver rejects commands, has fixed volume,
  or does not expose usable volume status. Synchronization requires both a valid
  level and mute state; separate partial status messages may provide them. Until
  both arrive, local volume/mute changes do not control the receiver. If usable
  status never arrives, streaming continues at unity sink gain and the existing
  device volume. The UI cannot promise device-volume or mute control in that
  situation.
- For receivers without usable volume status or controls, restart with
  `--local-volume` to select the original local software-gain behavior. This
  disables Cast volume synchronization and status polling. Local gain can lower
  the audio but cannot raise the actual device volume above its existing setting.
  There is no automatic fallback that changes gain semantics during playback.
- Windows loopback capture retains its existing behavior; this change concerns
  the Linux virtual sink.

Volume control runs outside the real-time PCM callback. The existing 48 kHz,
stereo S16LE capture, Opus encoder configuration, UDP Cast Streaming transport,
frame size, and target-delay offer are retained. This does not constitute a
measurement of latency or subjective audio quality on a physical receiver.

### Speaker groups and partial failures

Select the Google Home group's advertised name, as with the existing working
streaming setup. Commands go to that group's selected Cast endpoint and
advertised port. The code does not enumerate group members, alter group
membership, or send parallel commands to individual speakers.

A successful reply describes the endpoint's reported state. It cannot prove
that every member applied the change. An offline member, an independently
adjusted member, or a group implementation that omits aggregate volume status
can prevent complete synchronization. Member-specific Google Home changes may
not appear in the group's status. Use Google Home for individual member balance
and volume when the group endpoint does not expose those controls.

For direct addressing, a group commonly uses a port different from its leader's
individual-device port. Use the group's actual mDNS-advertised port:

```bash
./target/release/chromecast-sink --host 192.168.1.20 --port 32012
```

The address and port above are examples; discovering by group name is preferred.

## Bazzite / KDE with an existing Arch Distrobox

Run the commands in the same existing Arch Distrobox that successfully streamed
to the speakers. Keep Bazzite's desktop PipeWire and WirePlumber session running.
Use a normal user account for the program.

From a Bazzite terminal, find and enter the existing box:

```bash
distrobox list
distrobox enter --name YOUR_ARCH_BOX
```

Inside the Arch box, install build dependencies. Arch's `rust` package includes
Cargo; `libpipewire` contains the PipeWire and SPA development headers and
pkg-config files. The `pipewire` package supplies runtime client configuration,
including `/usr/share/pipewire/client.conf`; install it alongside `libpipewire`
and continue using Bazzite's desktop audio server. If an existing rustup toolchain is used instead of Arch's
`rust` package, omit `rust` from the pacman command, retain that toolchain, and
ensure it is Rust 1.85 or newer.

```bash
sudo pacman -Syu --needed base-devel rust pipewire libpipewire opus clang pkgconf
rustc --version
cargo --version
pkg-config --modversion libpipewire-0.3 libspa-0.2 opus
```

Build from a checkout of the branch containing receiver-volume synchronization.
Replace the path below with the checkout's location as seen inside the box:

```bash
cd /path/to/chromecast-sink
cargo test --locked
cargo build --release --locked
test -S "$XDG_RUNTIME_DIR/pipewire-0"
./target/release/chromecast-sink --device "YOUR GOOGLE HOME GROUP NAME"
```

Stop the old Chromecast Sink or P-Cast instance before starting this build so
that two programs do not compete for the Cast session. Keep this terminal open.
In KDE's Sound settings, select **Chromecast - YOUR GOOGLE HOME GROUP NAME** as
the output, then play audio. Change the output-device volume slider to control
the receiver. Press Ctrl+C in the terminal to stop and remove the virtual sink.

For diagnostics, run the same command with `--verbose`. Cast logs contain
receiver addresses, session identifiers, and stream negotiation data; inspect
them before sharing them publicly.

```bash
./target/release/chromecast-sink --device "YOUR GOOGLE HOME GROUP NAME" --verbose
```

If the receiver reports unsupported volume controls or never supplies complete
volume/mute status, stop the program and use the explicit local-gain fallback:

```bash
./target/release/chromecast-sink --device "YOUR GOOGLE HOME GROUP NAME" --local-volume
```

In that mode, set the speakers' actual volume in Google Home; KDE controls the
local audio gain and cannot raise that device setting.

If the socket check fails, check Distrobox's access to the desktop user's runtime
directory and its `XDG_RUNTIME_DIR`. Installing a second PipeWire daemon or
starting WirePlumber inside the box is not required for this setup. Discovery
uses mDNS UDP 5353; streaming and control retain the project's existing network
requirements. This change adds no HTTP/HLS server or new listening port.

Optional installation inside the box, after testing:

```bash
cargo install --path . --locked --force
chromecast-sink --device "YOUR GOOGLE HOME GROUP NAME"
```

The executable uses the box's shared libraries. Run it through the box rather
than copying an Arch-linked binary into Bazzite's host executable directories.
No AUR packaging or startup service is required to test the change.

## Automated native PipeWire check

The implementation was verified in the available Fedora 44 container with Rust
1.99.1, PipeWire 1.6.11, WirePlumber 0.5.19, and Opus 1.6. The regular suite passed
all 65 tests (63 library and two CLI tests). The separate native integration
test passed against private PipeWire, WirePlumber, and D-Bus processes. It checked
actual exposed volume/mute properties, unchanged stereo PCM at reduced volume,
zero and mute, echo suppression, above-100% clamping, sink teardown, and the
original local-volume fallback. Formatting, Clippy with warnings denied, and the
release build also passed. Cast hardware, Google Home, and physical group-member
failure behavior were not exercised in this environment.

The ordinary `cargo test --locked` suite runs deterministic protocol and control
tests. The native audio test is explicitly ignored in that suite because it
requires running temporary local daemons. To run it inside Arch, install its
additional tools and invoke the harness from the modified source tree:

```bash
sudo pacman -S --needed python wireplumber pipewire-audio dbus
python3 tests/run-pipewire-tests.py
```

The harness starts a private PipeWire server, WirePlumber policy instance, and
D-Bus session with temporary socket, configuration, and state directories. Its
WirePlumber profile disables hardware monitors. It does not stop or reconfigure
the desktop audio session. It feeds known stereo PCM into the virtual sink and
checks exposed volume/mute properties, local control changes, receiver-update
echo suppression, unity audio samples at zero/mute/partial/full device volume,
and the explicit local-gain fallback. No Google speaker connection is involved.
The harness cleans up its own child daemons on completion.

## Physical-device acceptance checks

Automated protocol and PipeWire tests cannot establish how a particular Google
speaker group implements receiver volume. Start at a comfortable low receiver
volume, then check the following on the two-speaker group:

1. Set volume and mute in Google Home before launch. Start this build and verify
   that launch preserves them and that KDE settles to the reported state.
2. Play familiar audio. Move KDE's output slider down and then modestly above
   the initial receiver setting. Check Google Home and both speakers to confirm
   that device volume changes, rather than only local audio attenuation.
3. Set KDE volume to zero, restore it, mute, and unmute. Confirm that sound
   returns and that the actual receiver mute state follows the UI.
4. Change the selected receiver's volume and mute in Google Home and, where
   available, using physical speaker controls. Check KDE after a status update
   or poll. Record changes the endpoint does not report.
5. Drag the slider quickly in both directions. Verify that it settles to the
   final reported level without oscillation or a repeated stream restart.
6. Adjust one group member in Google Home. Check whether the group endpoint
   reports that adjustment; do not assume it does. Temporarily disconnect one
   member and repeat a modest group change. Playback and status can be partial;
   the program should warn on protocol failures without claiming member success.
7. Compare playback responsiveness and sound with the unmodified build at
   matching device and source levels. The codec settings are unchanged, but a
   physical comparison is still needed to assess the actual setup.
8. Stop the program with Ctrl+C and verify that its sink disappears. Restart
   after any group leader/address change so discovery selects the current
   endpoint.

## Sources and reference implementation

The implementation starts from Chromecast Sink commit
`0d1fdaeb11ed35f88e14464083c777d4c0c113bb`.
[P-Cast's volume behavior](https://github.com/GenessyX/p-cast/blob/625eeaa97d5f98d9ff58bfb0c9f9c60967487b65/p_cast/device.py)
at commit `625eeaa97d5f98d9ff58bfb0c9f9c60967487b65` was inspected as a behavioral
reference: receiver commands for local changes and receiver status listeners
for external changes. The Rust implementation is independent; P-Cast's GPL
source and its sink gain workaround were not copied. Unlike P-Cast's startup
behavior at that commit, this implementation does not intentionally unmute or
impose a minimum audible volume on launch.

Build/setup references:
[Arch Rust file list](https://archlinux.org/packages/extra/x86_64/rust/files/),
[Arch PipeWire file list](https://archlinux.org/packages/extra/x86_64/pipewire/files/),
[Arch libpipewire file list](https://archlinux.org/packages/extra/x86_64/libpipewire/files/),
[Arch Opus package](https://archlinux.org/packages/extra/x86_64/opus/), and
[Distrobox enter documentation](https://distrobox.it/usage/distrobox-enter/).
The optional native-test tools are listed in the
[Arch pipewire-audio file list](https://archlinux.org/packages/extra/x86_64/pipewire-audio/files/)
and [Arch D-Bus file list](https://archlinux.org/packages/core/x86_64/dbus/files/).
