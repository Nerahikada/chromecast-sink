"""Minimal ctypes binding for the PulseAudio simple API (recording only).

Reads a monitor source directly instead of shelling out to a capture process.
PipeWire's pulse-server implements this protocol, so this works on both
PulseAudio and PipeWire.

Setting fragsize explicitly is what keeps capture latency low: it is the
per-read buffer size the server honours, and specifying it removes any reliance
on the server's default fragment size.
"""

from __future__ import annotations

import ctypes
import ctypes.util

_PA_STREAM_RECORD = 2
_PA_SAMPLE_S16LE = 3

# (uint32_t) -1 tells PulseAudio to pick a sensible default for a field.
_DEFAULT = 0xFFFFFFFF


class _SampleSpec(ctypes.Structure):
    _fields_ = [
        ("format", ctypes.c_int),
        ("rate", ctypes.c_uint32),
        ("channels", ctypes.c_uint8),
    ]


class _BufferAttr(ctypes.Structure):
    _fields_ = [
        ("maxlength", ctypes.c_uint32),
        ("tlength", ctypes.c_uint32),
        ("prebuf", ctypes.c_uint32),
        ("minreq", ctypes.c_uint32),
        ("fragsize", ctypes.c_uint32),
    ]


_simple: ctypes.CDLL | None = None
_pulse: ctypes.CDLL | None = None


def _load() -> tuple[ctypes.CDLL, ctypes.CDLL]:
    """Load libpulse-simple/libpulse and declare the signatures we use."""
    global _simple, _pulse
    if _simple is not None and _pulse is not None:
        return _simple, _pulse

    simple_path = ctypes.util.find_library("pulse-simple") or "libpulse-simple.so.0"
    pulse_path = ctypes.util.find_library("pulse") or "libpulse.so.0"
    try:
        simple = ctypes.CDLL(simple_path)
        pulse = ctypes.CDLL(pulse_path)
    except OSError as e:
        raise RuntimeError(
            f"Could not load PulseAudio client libraries ({simple_path}, {pulse_path}): {e}\n"
            "Install with: sudo apt install libpulse0"
        ) from e

    simple.pa_simple_new.argtypes = [
        ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p,
        ctypes.c_char_p, ctypes.POINTER(_SampleSpec), ctypes.c_void_p,
        ctypes.POINTER(_BufferAttr), ctypes.POINTER(ctypes.c_int),
    ]
    simple.pa_simple_new.restype = ctypes.c_void_p
    simple.pa_simple_read.argtypes = [
        ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_int),
    ]
    simple.pa_simple_read.restype = ctypes.c_int
    simple.pa_simple_get_latency.argtypes = [
        ctypes.c_void_p, ctypes.POINTER(ctypes.c_int)
    ]
    simple.pa_simple_get_latency.restype = ctypes.c_uint64
    simple.pa_simple_free.argtypes = [ctypes.c_void_p]
    pulse.pa_strerror.argtypes = [ctypes.c_int]
    pulse.pa_strerror.restype = ctypes.c_char_p

    _simple, _pulse = simple, pulse
    return simple, pulse


def check_available() -> str | None:
    """Return an error message if libpulse cannot be loaded, else None."""
    try:
        _load()
    except RuntimeError as e:
        return str(e)
    return None


class MonitorRecorder:
    """Blocking reader for a PulseAudio/PipeWire monitor source.

    Each read() returns exactly one fragment, so the read cadence paces the
    caller at real time without any explicit sleeping.
    """

    def __init__(
        self,
        source: str,
        sample_rate: int = 48000,
        channels: int = 2,
        fragment_bytes: int = 1920,
        app_name: str = "chromecast-sink",
    ):
        simple, pulse = _load()
        self._simple = simple
        self._pulse = pulse
        self._nbytes = fragment_bytes
        self._buf = ctypes.create_string_buffer(fragment_bytes)

        spec = _SampleSpec(_PA_SAMPLE_S16LE, sample_rate, channels)
        attr = _BufferAttr(
            maxlength=_DEFAULT,
            tlength=_DEFAULT,
            prebuf=_DEFAULT,
            minreq=_DEFAULT,
            fragsize=fragment_bytes,
        )
        err = ctypes.c_int(0)
        self._handle = simple.pa_simple_new(
            None,                      # default server
            app_name.encode(),
            _PA_STREAM_RECORD,
            source.encode(),
            b"capture",
            ctypes.byref(spec),
            None,                      # default channel map
            ctypes.byref(attr),
            ctypes.byref(err),
        )
        if not self._handle:
            raise RuntimeError(
                f"Could not record from {source!r}: {self._strerror(err.value)}"
            )

    def _strerror(self, code: int) -> str:
        msg = self._pulse.pa_strerror(code)
        return msg.decode() if msg else f"error {code}"

    def read(self) -> bytes:
        """Block until one fragment is available and return it."""
        err = ctypes.c_int(0)
        if self._simple.pa_simple_read(
            self._handle, self._buf, self._nbytes, ctypes.byref(err)
        ) < 0:
            raise RuntimeError(f"Capture read failed: {self._strerror(err.value)}")
        return self._buf.raw[: self._nbytes]

    def latency_ms(self) -> float:
        """How stale the next sample to be read is, in milliseconds."""
        err = ctypes.c_int(0)
        usec = self._simple.pa_simple_get_latency(self._handle, ctypes.byref(err))
        if usec == 0xFFFFFFFFFFFFFFFF:
            raise RuntimeError(f"Latency query failed: {self._strerror(err.value)}")
        return usec / 1000.0

    def close(self) -> None:
        if self._handle:
            self._simple.pa_simple_free(self._handle)
            self._handle = None
