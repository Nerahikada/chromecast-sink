"""Minimal ctypes binding for libopus (encoder only).

Covers exactly what the Cast Streaming pipeline needs: a 48kHz stereo encoder
in restricted-lowdelay mode producing 10ms frames. libopus itself is a stable
system library, so binding it directly avoids depending on a Python wrapper.
"""

from __future__ import annotations

import ctypes
import ctypes.util

# opus_defines.h
APPLICATION_RESTRICTED_LOWDELAY = 2051
_SET_BITRATE_REQUEST = 4002
_GET_LOOKAHEAD_REQUEST = 4027

# Opus never emits more than ~1275 bytes per frame; 4000 is the size the
# reference encoder documentation recommends for the output buffer.
_MAX_PACKET_BYTES = 4000

_lib: ctypes.CDLL | None = None


def _load() -> ctypes.CDLL:
    """Load libopus and declare the signatures we use."""
    global _lib
    if _lib is not None:
        return _lib

    path = ctypes.util.find_library("opus") or "libopus.so.0"
    try:
        lib = ctypes.CDLL(path)
    except OSError as e:
        raise RuntimeError(
            f"Could not load libopus ({path}): {e}\n"
            "Install with: sudo apt install libopus0"
        ) from e

    lib.opus_encoder_get_size.argtypes = [ctypes.c_int]
    lib.opus_encoder_get_size.restype = ctypes.c_int
    lib.opus_encoder_init.argtypes = [
        ctypes.c_void_p, ctypes.c_int32, ctypes.c_int, ctypes.c_int
    ]
    lib.opus_encoder_init.restype = ctypes.c_int
    lib.opus_encode.argtypes = [
        ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int,
        ctypes.c_char_p, ctypes.c_int32,
    ]
    lib.opus_encode.restype = ctypes.c_int32
    # opus_encoder_ctl is variadic; argtypes is left unset so that callers pass
    # explicitly-typed ctypes values for the request-specific arguments.
    lib.opus_encoder_ctl.restype = ctypes.c_int
    lib.opus_strerror.argtypes = [ctypes.c_int]
    lib.opus_strerror.restype = ctypes.c_char_p

    _lib = lib
    return lib


def check_available() -> str | None:
    """Return an error message if libopus cannot be loaded, else None."""
    try:
        _load()
    except RuntimeError as e:
        return str(e)
    return None


class OpusEncoder:
    """A libopus encoder configured for Cast Streaming audio."""

    def __init__(
        self,
        sample_rate: int = 48000,
        channels: int = 2,
        bit_rate: int = 128000,
        application: int = APPLICATION_RESTRICTED_LOWDELAY,
    ):
        lib = _load()
        self._lib = lib

        # Allocate the encoder in a Python buffer rather than via
        # opus_encoder_create, so its lifetime follows this object.
        size = lib.opus_encoder_get_size(channels)
        self._state = ctypes.create_string_buffer(size)
        self._handle = ctypes.cast(self._state, ctypes.c_void_p)
        self._check(lib.opus_encoder_init(self._handle, sample_rate, channels, application))
        self._check(
            lib.opus_encoder_ctl(
                self._handle, _SET_BITRATE_REQUEST, ctypes.c_int32(bit_rate)
            )
        )
        self._out = ctypes.create_string_buffer(_MAX_PACKET_BYTES)

    def _check(self, code: int) -> None:
        if code < 0:
            raise RuntimeError(f"libopus error: {self._lib.opus_strerror(code).decode()}")

    @property
    def lookahead_samples(self) -> int:
        """Encoder lookahead, i.e. its algorithmic delay, in samples."""
        value = ctypes.c_int32(0)
        self._check(
            self._lib.opus_encoder_ctl(
                self._handle, _GET_LOOKAHEAD_REQUEST, ctypes.byref(value)
            )
        )
        return value.value

    def encode(self, pcm: bytes, frame_samples: int) -> bytes:
        """Encode one frame of interleaved 16-bit PCM.

        Args:
            pcm: Interleaved signed 16-bit native-endian samples.
            frame_samples: Samples per channel (480 for a 10ms frame at 48kHz).

        Returns:
            The encoded Opus packet.
        """
        n = self._lib.opus_encode(
            self._handle, pcm, frame_samples, self._out, _MAX_PACKET_BYTES
        )
        self._check(n)
        return self._out.raw[:n]
