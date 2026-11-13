#!/usr/bin/env python3
"""Run opt-in audio tests against private PipeWire, WirePlumber, and D-Bus.

Usage: python3 tests/run-pipewire-tests.py [cargo test ...]
No host daemon is stopped, configured, or addressed. All state is temporary.
Requires pipewire, wireplumber, dbus-run-session, pw-cat, pw-cli and pw-dump.
"""

import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time


REPO = Path(__file__).resolve().parents[1]


def stop(process):
    if process.poll() is None:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)


def inside(command):
    root = Path(os.environ["PIPEWIRE_RUNTIME_DIR"]).parent
    assert os.environ.get("CHROMECAST_SINK_PRIVATE_PIPEWIRE_TEST") == "1"
    children = []
    logs = []
    result = 1
    try:
        for name, arguments in (
            ("pipewire", []),
            ("wireplumber", ["--profile=volume-test"]),
        ):
            log = open(root / f"{name}.log", "w+")
            logs.append((name, log))
            process = subprocess.Popen(
                [name, *arguments], stdout=log, stderr=log, start_new_session=True
            )
            children.append(process)
            deadline = time.monotonic() + 10
            last_dump = "not queried"
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError(f"private {name} exited with {process.returncode}")
                if name == "pipewire":
                    if (Path(os.environ["PIPEWIRE_RUNTIME_DIR"]) / "pipewire-0").is_socket():
                        break
                else:
                    try:
                        dump = subprocess.run(["pw-dump"], capture_output=True, timeout=2)
                        last_dump = (dump.stdout + dump.stderr).decode(errors="replace")
                        dump.check_returncode()
                        objects = json.loads(dump.stdout)
                        if any(
                            obj.get("props", obj.get("info", {}).get("props", {})).get("metadata.name")
                            in ("default", "sm-settings")
                            or obj.get("info", {}).get("props", {}).get("application.name")
                            == "WirePlumber"
                            for obj in objects
                        ):
                            break
                    except (subprocess.SubprocessError, json.JSONDecodeError) as error:
                        last_dump += "\n" + str(error)
                time.sleep(0.05)
            else:
                raise RuntimeError(f"private {name} did not become ready; last pw-dump:\n{last_dump[-12000:]}")
        child = subprocess.Popen(command, start_new_session=True)
        children.append(child)
        try:
            result = child.wait(timeout=240)
        except subprocess.TimeoutExpired:
            print("Private PipeWire test command timed out", file=sys.stderr)
            result = 124
    finally:
        for child in reversed(children):
            stop(child)
        for name, log in logs:
            if result:
                log.seek(0)
                print(f"\nPrivate {name} log:\n{log.read()[-12000:]}", file=sys.stderr)
            log.close()
    return result


def main():
    os.chdir(REPO)
    arguments = sys.argv[1:]
    if arguments[:1] == ["--inside"]:
        return inside(arguments[1:])
    command = arguments or [
        "cargo", "test", "--locked", "--test", "pipewire_volume", "--",
        "--ignored", "--nocapture",
    ]
    env = os.environ.copy()
    native_bin = REPO.parent / "native" / "usr" / "bin"
    env["PATH"] = str(native_bin) + os.pathsep + env.get("PATH", "")
    required = ["pipewire", "wireplumber", "dbus-run-session", "pw-cli", "pw-cat", "pw-dump"]
    missing = [name for name in required if not shutil.which(name, path=env["PATH"])]
    if missing:
        raise SystemExit("Missing integration-test tools: " + ", ".join(missing))
    with tempfile.TemporaryDirectory(prefix="chromecast-sink-pw-", dir="/tmp") as folder:
        root = Path(folder)
        for name in ("runtime", "config", "state", "cache"):
            (root / name).mkdir(mode=0o700)
        # Clear inherited server/config overrides, then explicitly select only
        # our private native socket and temporary configuration and state.
        for key in list(env):
            if key.startswith(("PIPEWIRE_", "WIREPLUMBER_")):
                del env[key]
        env.update({
            "XDG_RUNTIME_DIR": str(root / "runtime"),
            "PIPEWIRE_RUNTIME_DIR": str(root / "runtime"),
            "PIPEWIRE_REMOTE": "pipewire-0",
            "XDG_CONFIG_HOME": str(root / "config"),
            "XDG_STATE_HOME": str(root / "state"),
            "XDG_CACHE_HOME": str(root / "cache"),
            "DBUS_SYSTEM_BUS_ADDRESS": "unix:path=" + str(root / "no-system-bus"),
            "PULSE_SERVER": "unix:" + str(root / "no-pulse-server"),
            "CHROMECAST_SINK_PRIVATE_PIPEWIRE_TEST": "1",
        })
        fragment = root / "config" / "wireplumber" / "wireplumber.conf.d"
        fragment.mkdir(parents=True)
        (fragment / "99-volume-test.conf").write_text("""
wireplumber.profiles = {
  volume-test = {
    inherits = [ policy, mixin.stateless ]
    metadata.default = required
    hardware.audio = disabled
    hardware.bluetooth = disabled
    hardware.video-capture = disabled
    monitor.alsa = disabled
    monitor.alsa-midi = disabled
    monitor.bluez = disabled
    monitor.v4l2 = disabled
    monitor.libcamera = disabled
    support.logind = disabled
    support.reserve-device = disabled
    support.portal-permissionstore = disabled
    support.system-dbus = disabled
  }
}
""")
        process = subprocess.Popen(
            ["dbus-run-session", "--", sys.executable, str(Path(__file__).resolve()),
             "--inside", *command], env=env, start_new_session=True
        )
        try:
            return process.wait()
        finally:
            stop(process)


if __name__ == "__main__":
    sys.exit(main())
