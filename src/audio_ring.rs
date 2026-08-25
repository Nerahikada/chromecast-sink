use std::sync::atomic::{fence, AtomicBool, AtomicI16, AtomicUsize, Ordering};
use std::sync::Arc;

/// The realtime producer laps the read index on overrun rather than blocking; slots are atomic only to make that a defined race rather than UB.
///
/// The indices advance by a load/store pair, not an atomic RMW, so a second endpoint on either side would drive its index backwards — and every field being atomic, no sanitizer would see it.
/// Hence the split endpoints.
struct AudioRing {
    buf: Box<[AtomicI16]>,
    capacity: usize,
    channels: usize,
    sample_mask: usize,
    /// Published *before* the producer touches any slot; `write` alone would hide a batch in flight over the region the consumer is copying.
    reserved: AtomicUsize,
    write: AtomicUsize,
    read: AtomicUsize,
    closed: AtomicBool,
}

pub fn channel(capacity_frames: usize, channels: usize) -> (RingProducer, RingConsumer) {
    assert!(capacity_frames.is_power_of_two());
    assert!(channels.is_power_of_two());
    let samples = capacity_frames * channels;
    let ring = Arc::new(AudioRing {
        buf: (0..samples).map(|_| AtomicI16::new(0)).collect(),
        capacity: capacity_frames,
        channels,
        sample_mask: samples - 1,
        reserved: AtomicUsize::new(0),
        write: AtomicUsize::new(0),
        read: AtomicUsize::new(0),
        closed: AtomicBool::new(false),
    });
    (RingProducer { ring: Arc::clone(&ring) }, RingConsumer { ring })
}

impl AudioRing {
    fn capacity_frames(&self) -> usize {
        self.capacity
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn available_frames(&self) -> usize {
        let w = self.write.load(Ordering::Acquire);
        let r = self.read.load(Ordering::Relaxed);
        w.wrapping_sub(r)
    }

    fn write_s16le(&self, bytes: &[u8]) {
        let frame_bytes = self.channels * 2;
        let mut frames = bytes.len() / frame_bytes;
        if frames == 0 {
            return;
        }
        let mut bytes = bytes;
        if frames > self.capacity {
            bytes = &bytes[(frames - self.capacity) * frame_bytes..];
            frames = self.capacity;
        }

        let w = self.write.load(Ordering::Relaxed);
        let end = w.wrapping_add(frames);
        self.reserved.store(end, Ordering::Relaxed);
        fence(Ordering::Release);

        let base = w.wrapping_mul(self.channels);
        for (i, s) in bytes[..frames * frame_bytes].chunks_exact(2).enumerate() {
            self.buf[base.wrapping_add(i) & self.sample_mask].store(i16::from_le_bytes([s[0], s[1]]), Ordering::Relaxed);
        }
        self.write.store(end, Ordering::Release);
    }

    fn read_frames(&self, out: &mut [i16]) -> bool {
        debug_assert_eq!(out.len() % self.channels, 0);
        let need = out.len() / self.channels;
        if need == 0 {
            return false;
        }
        let r = self.read.load(Ordering::Relaxed);
        if self.write.load(Ordering::Acquire).wrapping_sub(r) < need {
            return false;
        }

        let base = r.wrapping_mul(self.channels);
        for (i, o) in out.iter_mut().enumerate() {
            *o = self.buf[base.wrapping_add(i) & self.sample_mask].load(Ordering::Relaxed);
        }

        // Pins the copy above ahead of the check below; without it aarch64 may satisfy the slot loads after the `reserved` load.
        fence(Ordering::Acquire);
        if self.reserved.load(Ordering::Relaxed).wrapping_sub(r) > self.capacity {
            return false;
        }
        self.read.store(r.wrapping_add(need), Ordering::Release);
        true
    }

    fn skip_frames(&self, frames: usize) {
        let r = self.read.load(Ordering::Relaxed);
        let max = self.write.load(Ordering::Acquire).wrapping_sub(r);
        self.read.store(r.wrapping_add(frames.min(max)), Ordering::Release);
    }
}

pub struct RingProducer {
    ring: Arc<AudioRing>,
}

impl RingProducer {
    pub fn write_s16le(&mut self, bytes: &[u8]) {
        self.ring.write_s16le(bytes);
    }

    pub fn close(&self) {
        self.ring.close();
    }

    pub fn closer(&self) -> RingCloser {
        RingCloser { ring: Arc::clone(&self.ring) }
    }
}

/// Data-free, so the sink's give-up paths can hold one while the producer itself lives in the process callback.
#[derive(Clone)]
pub struct RingCloser {
    ring: Arc<AudioRing>,
}

impl RingCloser {
    pub fn close(&self) {
        self.ring.close();
    }
}

/// Index-advancing methods take `&mut self` so an `Arc` cannot fan this out.
pub struct RingConsumer {
    ring: Arc<AudioRing>,
}

impl RingConsumer {
    pub fn read_frames(&mut self, out: &mut [i16]) -> bool {
        self.ring.read_frames(out)
    }

    pub fn skip_frames(&mut self, frames: usize) {
        self.ring.skip_frames(frames);
    }

    pub fn available_frames(&self) -> usize {
        self.ring.available_frames()
    }

    pub fn capacity_frames(&self) -> usize {
        self.ring.capacity_frames()
    }

    pub fn is_closed(&self) -> bool {
        self.ring.is_closed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pcm(vals: &[i16]) -> Vec<u8> {
        vals.iter().flat_map(|v| v.to_le_bytes()).collect()
    }

    #[test]
    fn roundtrip_preserves_samples() {
        let (mut tx, mut rx) = channel(8, 2);
        tx.write_s16le(&pcm(&[1, 2, 3, 4, 5, 6]));
        assert_eq!(rx.available_frames(), 3);

        let mut out = [0i16; 4];
        assert!(rx.read_frames(&mut out));
        assert_eq!(out, [1, 2, 3, 4]);
        assert_eq!(rx.available_frames(), 1);
    }

    #[test]
    fn underrun_consumes_nothing() {
        let (mut tx, mut rx) = channel(8, 2);
        tx.write_s16le(&pcm(&[1, 2]));
        let mut out = [0i16; 4];
        assert!(!rx.read_frames(&mut out));
        assert_eq!(rx.available_frames(), 1);
    }

    #[test]
    fn wraps_around_the_end() {
        let (mut tx, mut rx) = channel(4, 2);
        tx.write_s16le(&pcm(&[1, 1, 2, 2, 3, 3]));
        let mut out = [0i16; 4];
        assert!(rx.read_frames(&mut out));
        tx.write_s16le(&pcm(&[4, 4, 5, 5]));
        assert!(rx.read_frames(&mut out));
        assert_eq!(out, [3, 3, 4, 4]);
    }

    #[test]
    fn skip_advances_reader() {
        let (mut tx, mut rx) = channel(8, 2);
        tx.write_s16le(&pcm(&[1, 1, 2, 2, 3, 3, 4, 4]));
        rx.skip_frames(3);
        let mut out = [0i16; 2];
        assert!(rx.read_frames(&mut out));
        assert_eq!(out, [4, 4]);
    }

    #[test]
    fn skip_cannot_overtake_the_writer() {
        let (mut tx, mut rx) = channel(8, 2);
        tx.write_s16le(&pcm(&[1, 1, 2, 2]));
        rx.skip_frames(99);
        assert_eq!(rx.available_frames(), 0);
        let mut out = [0i16; 2];
        assert!(!rx.read_frames(&mut out));
    }

    #[test]
    fn overrun_is_visible_to_consumer() {
        let (mut tx, mut rx) = channel(4, 2);
        for _ in 0..3 {
            tx.write_s16le(&pcm(&[9, 9, 9, 9, 9, 9, 9, 9]));
        }
        assert!(rx.available_frames() > rx.capacity_frames());
        let mut out = [0i16; 2];
        assert!(!rx.read_frames(&mut out));
    }

    #[test]
    fn oversized_chunk_keeps_newest() {
        let (mut tx, mut rx) = channel(2, 2);
        tx.write_s16le(&pcm(&[1, 1, 2, 2, 3, 3]));
        assert_eq!(rx.available_frames(), 2);
        let mut out = [0i16; 4];
        assert!(rx.read_frames(&mut out));
        assert_eq!(out, [2, 2, 3, 3]);
    }

    #[test]
    fn chunk_exactly_capacity_is_kept_whole() {
        let (mut tx, mut rx) = channel(4, 2);
        tx.write_s16le(&pcm(&[1, 1, 2, 2, 3, 3, 4, 4]));
        assert_eq!(rx.available_frames(), 4);
        let mut out = [0i16; 8];
        assert!(rx.read_frames(&mut out));
        assert_eq!(out, [1, 1, 2, 2, 3, 3, 4, 4]);
    }

    #[test]
    fn empty_request_is_rejected() {
        let (mut tx, mut rx) = channel(8, 2);
        tx.write_s16le(&pcm(&[1, 1]));
        assert!(!rx.read_frames(&mut []));
        assert_eq!(rx.available_frames(), 1);
    }

    #[test]
    fn close_is_observable() {
        let (tx, rx) = channel(8, 2);
        let closer = tx.closer();
        assert!(!rx.is_closed());
        drop(tx);
        closer.close();
        assert!(rx.is_closed());
    }

    #[test]
    fn concurrent_producer_and_consumer() {
        let (mut tx, mut rx) = channel(8, 2);
        let h = std::thread::spawn(move || {
            for i in 0..64i16 {
                tx.write_s16le(&pcm(&[i, i, i, i]));
            }
            tx.close();
        });

        let mut out = [0i16; 4];
        let mut reads = 0;
        while !rx.is_closed() || rx.available_frames() >= 2 {
            if rx.available_frames() > rx.capacity_frames() {
                rx.skip_frames(rx.available_frames() - 2);
                continue;
            }
            if rx.read_frames(&mut out) {
                assert_eq!(out[0], out[1]);
                reads += 1;
            }
        }
        h.join().unwrap();
        assert!(reads > 0);
    }
}
