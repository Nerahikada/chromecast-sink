//! Opus encoder wrapper for the Cast Streaming pipeline.
//!
//! 48 kHz stereo, `RESTRICTED_LOWDELAY` application, 10 ms frames.

use anyhow::Result;
use opus::{Application, Bitrate, Channels, Encoder};

pub struct OpusEncoder {
    enc: Encoder,
    out: Vec<u8>,
}

impl OpusEncoder {
    pub fn new(sample_rate: u32, channels: u8, bit_rate: i32) -> Result<Self> {
        let ch = match channels {
            1 => Channels::Mono,
            2 => Channels::Stereo,
            n => anyhow::bail!("unsupported channel count: {n}"),
        };
        let mut enc = Encoder::new(sample_rate, ch, Application::LowDelay)?;
        enc.set_bitrate(Bitrate::Bits(bit_rate))?;
        Ok(Self { enc, out: vec![0u8; 4000] })
    }

    /// Encoder algorithmic delay in samples.
    pub fn lookahead_samples(&mut self) -> i32 {
        self.enc.get_lookahead().unwrap_or(0)
    }

    /// Encode one frame of interleaved 16-bit signed PCM. Returns the Opus packet bytes.
    pub fn encode(&mut self, pcm_i16: &[i16]) -> Result<&[u8]> {
        let n = self.enc.encode(pcm_i16, &mut self.out)?;
        Ok(&self.out[..n])
    }
}
