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
        // Application::LowDelay = RESTRICTED_LOWDELAY: skips prediction/lookahead machinery Cast Streaming can't tolerate.
        // Paired with 10 ms frames by caller.
        let mut enc = Encoder::new(sample_rate, ch, Application::LowDelay)?;
        enc.set_bitrate(Bitrate::Bits(bit_rate))?;
        Ok(Self { enc, out: vec![0u8; 4000] })
    }

    /// Encoder algorithmic delay in samples (nonzero even in LowDelay mode).
    pub fn lookahead_samples(&mut self) -> i32 {
        self.enc.get_lookahead().unwrap_or(0)
    }

    pub fn encode(&mut self, pcm_i16: &[i16]) -> Result<&[u8]> {
        let n = self.enc.encode(pcm_i16, &mut self.out)?;
        Ok(&self.out[..n])
    }
}
