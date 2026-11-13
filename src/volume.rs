//! Device volume control, independent of the PCM/encoding path.
//!
//! This uses the platform receiver namespace, not a media-session volume. A
//! group is controlled through its selected receiver endpoint; its reported
//! volume is not an inventory of, or acknowledgement from, individual members.

use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::cast_channel::{local_sender_id, CastMessage, NS_RECEIVER, PLATFORM_RECEIVER_ID};
use crate::mirroring::new_request_id;

const EPSILON: f32 = 0.001;
const COALESCE: Duration = Duration::from_millis(50);
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(2);
const POLL_INTERVAL: Duration = Duration::from_secs(5);
const INITIAL_WARNING_DELAY: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Volume {
    /// Receiver volume, in the normalized Cast range (not a PCM multiplier).
    pub level: f32,
    pub muted: bool,
}

impl Volume {
    /// Reject invalid numbers and cap desktop amplification at the device's 100%.
    pub fn new(level: f32, muted: bool) -> Option<Self> {
        level.is_finite().then(|| Self {
            level: level.clamp(0.0, 1.0),
            muted,
        })
    }

    pub fn approx_eq(self, other: Self) -> bool {
        (self.level - other.level).abs() <= EPSILON && self.muted == other.muted
    }
}

#[derive(Debug, PartialEq)]
pub enum Effect {
    /// Send to receiver-0 on NS_RECEIVER, on the control worker only.
    Send(Value),
    /// Update the virtual controls without changing outgoing PCM gain.
    SetLocal(Volume),
    Warning(&'static str),
}

#[derive(Default)]
struct ReportedVolume {
    level: Option<f32>,
    muted: Option<bool>,
}

impl ReportedVolume {
    fn merge(&mut self, value: &Value) -> (bool, bool) {
        let mut level_present = false;
        let mut mute_present = false;
        if let Some(level) = value.get("level").and_then(Value::as_f64) {
            // Reject rather than saturate malformed receiver data. In
            // particular, an enormous finite f64 must not become f32 infinity.
            if level.is_finite() && (0.0..=1.0).contains(&level) {
                self.level = Some(level as f32);
                level_present = true;
            }
        }
        if let Some(muted) = value.get("muted").and_then(Value::as_bool) {
            self.muted = Some(muted);
            mute_present = true;
        }
        (level_present, mute_present)
    }

    fn complete(&self) -> Option<Volume> {
        Some(Volume {
            level: self.level?,
            muted: self.muted?,
        })
    }
}

struct Pending {
    target: Volume,
    /// Separate commands allow a level-only or mute-only change and expose
    /// partial success without assuming the receiver atomically accepts both.
    requests: Vec<PendingRequest>,
    deadline: Instant,
}

struct PendingRequest {
    id: u64,
    is_level: bool,
}

/// A deterministic control state machine. Nothing here blocks, touches audio,
/// or retries an unconfirmed volume change. `tick` should run about every 50 ms.
pub struct VolumeSync {
    reported: ReportedVolume,
    displayed: Option<Volume>,
    queued: Option<(Volume, Instant)>,
    pending: Option<Pending>,
    /// Responses to an earlier GET_STATUS/SET_VOLUME cannot undo a newer edit.
    latest_set_id: u64,
    fixed_level: bool,
    next_poll: Instant,
    initial_warning_at: Instant,
    initial_warning_sent: bool,
}

impl VolumeSync {
    pub fn new(now: Instant) -> Self {
        Self {
            reported: ReportedVolume::default(),
            displayed: None,
            queued: None,
            pending: None,
            latest_set_id: 0,
            fixed_level: false,
            next_poll: now,
            initial_warning_at: now + INITIAL_WARNING_DELAY,
            initial_warning_sent: false,
        }
    }

    pub fn local_changed(&mut self, volume: Volume, now: Instant) -> Vec<Effect> {
        let mut effects = Vec::new();
        let original_level = volume.level;
        let Some(mut volume) = Volume::new(volume.level, volume.muted) else {
            return effects;
        };
        let Some(reported) = self.reported.complete() else {
            // PipeWire/session-manager defaults must never raise or unmute a
            // receiver at startup. Its first complete status wins.
            return effects;
        };
        if original_level != volume.level {
            effects.push(Effect::SetLocal(volume));
        }
        if self.displayed.is_some_and(|old| old.approx_eq(volume)) {
            return effects;
        }
        if self.fixed_level && (volume.level - reported.level).abs() > EPSILON {
            volume.level = reported.level;
            effects.push(Effect::Warning(
                "Receiver reports fixed volume; device level cannot be changed",
            ));
            effects.push(Effect::SetLocal(volume));
        }
        self.displayed = Some(volume);
        // Keep the first due time while replacing the target: a continuous
        // slider drag must not postpone every command until the drag ends.
        let due = self.queued.map_or(now + COALESCE, |(_, due)| due);
        self.queued = Some((volume, due));
        effects
    }

    pub fn receive(&mut self, msg: &CastMessage, now: Instant) -> Vec<Effect> {
        let mut effects = Vec::new();
        if msg.namespace != NS_RECEIVER
            || msg.source != PLATFORM_RECEIVER_ID
            || (msg.destination != local_sender_id() && msg.destination != "*")
        {
            return effects;
        }
        let Ok(value) = serde_json::from_str::<Value>(&msg.payload) else {
            return effects;
        };
        let request_id = value.get("requestId").and_then(Value::as_u64);
        // A positive ID is a solicited response. Broadcasts use 0 or omit it.
        if request_id.is_some_and(|id| id != 0 && id < self.latest_set_id) {
            // An earlier level response from the *current* batch still matters
            // when the following command changes mute.
            if !self
                .pending
                .as_ref()
                .is_some_and(|p| p.requests.iter().any(|r| Some(r.id) == request_id))
            {
                return effects;
            }
        }
        match value.get("type").and_then(Value::as_str) {
            Some("RECEIVER_STATUS") => {
                let Some(volume) = value.pointer("/status/volume").filter(|v| v.is_object()) else {
                    return effects;
                };
                let (level_present, mute_present) = self.reported.merge(volume);
                if let Some(control_type) = volume.get("controlType").and_then(Value::as_str) {
                    self.fixed_level = control_type == "fixed";
                }
                if let Some(pending) = &mut self.pending {
                    if let Some(id) = request_id {
                        pending.requests.retain(|request| {
                            request.id != id
                                || if request.is_level {
                                    !level_present
                                } else {
                                    !mute_present
                                }
                        });
                    }
                    let matched = self
                        .reported
                        .complete()
                        .is_some_and(|v| v.approx_eq(pending.target));
                    if matched || pending.requests.is_empty() {
                        self.pending = None;
                    }
                }
                // Hold the newest desktop target through intermediate echoes
                // and partial command acknowledgements. Once idle, receiver
                // status (including quantization/external edits) is authority.
                if self.pending.is_none() && self.queued.is_none() {
                    self.reflect(&mut effects);
                }
            }
            Some("INVALID_REQUEST" | "INVALID_PARAMS" | "SET_VOLUME_ERROR" | "ERROR") => {
                let is_pending = self.pending.as_ref().is_some_and(|p| {
                    request_id.is_none_or(|id| p.requests.iter().any(|r| r.id == id))
                });
                if is_pending {
                    self.abandon(&mut effects);
                    effects.push(Effect::Warning("Receiver rejected volume control; restoring reported state (audio continues)"));
                    self.poll(now, &mut effects);
                }
            }
            _ => {}
        }
        effects
    }

    pub fn tick(&mut self, now: Instant) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.pending.as_ref().is_some_and(|p| now >= p.deadline) {
            self.abandon(&mut effects);
            effects.push(Effect::Warning("Receiver did not confirm volume control; restoring reported state (audio continues)"));
            self.poll(now, &mut effects);
        }
        if self.reported.complete().is_none()
            && now >= self.initial_warning_at
            && !self.initial_warning_sent
        {
            self.initial_warning_sent = true;
            effects.push(Effect::Warning("Receiver has not reported device volume/mute; volume synchronization is unavailable until status arrives"));
        }
        if self.pending.is_none() && self.queued.is_some_and(|(_, due)| now >= due) {
            let (target, _) = self.queued.take().expect("queued target checked");
            if let Some(reported) = self.reported.complete() {
                let mut requests = Vec::new();
                if !self.fixed_level && (target.level - reported.level).abs() > EPSILON {
                    let id = new_request_id();
                    effects.push(Effect::Send(json!({"type":"SET_VOLUME", "requestId":id, "volume":{"level":target.level}})));
                    requests.push(PendingRequest { id, is_level: true });
                }
                if target.muted != reported.muted {
                    let id = new_request_id();
                    effects.push(Effect::Send(json!({"type":"SET_VOLUME", "requestId":id, "volume":{"muted":target.muted}})));
                    requests.push(PendingRequest {
                        id,
                        is_level: false,
                    });
                }
                if let Some(request) = requests.last() {
                    self.latest_set_id = request.id;
                    self.pending = Some(Pending {
                        target,
                        requests,
                        deadline: now + CONFIRM_TIMEOUT,
                    });
                } else {
                    self.reflect(&mut effects);
                }
            }
        }
        if self.pending.is_none() && now >= self.next_poll {
            self.poll(now, &mut effects);
        }
        effects
    }

    fn reflect(&mut self, effects: &mut Vec<Effect>) {
        if let Some(reported) = self.reported.complete() {
            if !self.displayed.is_some_and(|v| v.approx_eq(reported)) {
                self.displayed = Some(reported);
                effects.push(Effect::SetLocal(reported));
            }
        }
    }

    fn abandon(&mut self, effects: &mut Vec<Effect>) {
        self.pending = None;
        self.queued = None;
        self.reflect(effects);
    }

    fn poll(&mut self, now: Instant, effects: &mut Vec<Effect>) {
        effects.push(Effect::Send(
            json!({"type":"GET_STATUS", "requestId":new_request_id()}),
        ));
        self.next_poll = now + POLL_INTERVAL;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(level: f32, muted: bool) -> Volume {
        Volume::new(level, muted).unwrap()
    }
    fn message(value: Value) -> CastMessage {
        CastMessage {
            source: PLATFORM_RECEIVER_ID.into(),
            destination: local_sender_id().into(),
            namespace: NS_RECEIVER.into(),
            payload: value.to_string(),
        }
    }
    fn status(level: f32, muted: bool, request_id: Option<u64>) -> CastMessage {
        let mut value =
            json!({"type":"RECEIVER_STATUS","status":{"volume":{"level":level,"muted":muted}}});
        if let Some(id) = request_id {
            value["requestId"] = json!(id);
        }
        message(value)
    }
    fn initialized(now: Instant) -> VolumeSync {
        let mut sync = VolumeSync::new(now);
        sync.tick(now);
        assert_eq!(
            sync.receive(&status(0.2, false, None), now),
            vec![Effect::SetLocal(v(0.2, false))]
        );
        sync
    }
    fn sends(effects: &[Effect]) -> Vec<&Value> {
        effects
            .iter()
            .filter_map(|e| {
                if let Effect::Send(v) = e {
                    Some(v)
                } else {
                    None
                }
            })
            .collect()
    }
    fn request_id(value: &Value) -> u64 {
        value["requestId"].as_u64().unwrap()
    }

    #[test]
    fn startup_reads_receiver_without_raising_or_unmuting_it() {
        let now = Instant::now();
        let mut sync = VolumeSync::new(now);
        assert!(sync.local_changed(v(1.0, false), now).is_empty());
        assert_eq!(sends(&sync.tick(now))[0]["type"], "GET_STATUS");
        assert_eq!(
            sync.receive(&status(0.0, true, None), now),
            vec![Effect::SetLocal(v(0.0, true))]
        );
        assert!(sync.local_changed(v(0.0, true), now).is_empty());
        assert!(sync.tick(now + COALESCE).is_empty());
    }

    #[test]
    fn finite_levels_are_clamped_but_invalid_numbers_are_rejected() {
        assert_eq!(Volume::new(2.0, false), Some(v(1.0, false)));
        assert_eq!(Volume::new(-2.0, false), Some(v(0.0, false)));
        assert!(Volume::new(f32::NAN, false).is_none());
        assert!(Volume::new(f32::INFINITY, false).is_none());
    }

    #[test]
    fn desktop_amplification_is_corrected_even_when_device_is_already_full() {
        let now = Instant::now();
        let mut sync = initialized(now);
        sync.receive(&status(1.0, false, None), now);
        assert_eq!(
            sync.local_changed(
                Volume {
                    level: 2.0,
                    muted: false
                },
                now
            ),
            vec![Effect::SetLocal(v(1.0, false))]
        );
        assert!(sync.tick(now + COALESCE).is_empty());
    }

    #[test]
    fn slider_changes_are_coalesced_and_use_receiver_commands() {
        let now = Instant::now();
        let mut sync = initialized(now);
        sync.local_changed(v(0.4, false), now);
        sync.local_changed(v(0.7, false), now + Duration::from_millis(40));
        assert!(sync.tick(now + Duration::from_millis(49)).is_empty());
        let effects = sync.tick(now + COALESCE);
        let sent = sends(&effects);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0]["type"], "SET_VOLUME");
        assert_eq!(sent[0]["volume"], json!({"level":0.7_f32}));
        assert!(sync
            .receive(
                &status(0.7, false, Some(request_id(sent[0]))),
                now + COALESCE
            )
            .is_empty());
        assert!(sync.local_changed(v(0.7, false), now + COALESCE).is_empty());
    }

    #[test]
    fn external_updates_reflect_without_feedback_commands() {
        let now = Instant::now();
        let mut sync = initialized(now);
        assert_eq!(
            sync.receive(&status(0.6, true, None), now),
            vec![Effect::SetLocal(v(0.6, true))]
        );
        assert!(sync.local_changed(v(0.6, true), now).is_empty());
        assert!(sync.tick(now + COALESCE).is_empty());
    }

    #[test]
    fn quantized_receiver_acknowledgement_corrects_desktop_without_resending() {
        let now = Instant::now();
        let mut sync = initialized(now);
        sync.local_changed(v(0.65, false), now);
        let effects = sync.tick(now + COALESCE);
        let id = request_id(sends(&effects)[0]);
        assert_eq!(
            sync.receive(&status(0.64, false, Some(id)), now + COALESCE),
            vec![Effect::SetLocal(v(0.64, false))]
        );
        assert!(sync
            .local_changed(v(0.64, false), now + COALESCE)
            .is_empty());
        assert!(sync.tick(now + COALESCE + COALESCE).is_empty());
    }

    #[test]
    fn old_replies_do_not_overwrite_a_newer_slider_target() {
        let now = Instant::now();
        let mut sync = initialized(now);
        sync.local_changed(v(0.4, false), now);
        let effects = sync.tick(now + COALESCE);
        let id = request_id(sends(&effects)[0]);
        sync.local_changed(v(0.8, false), now + COALESCE);
        assert!(sync
            .receive(&status(0.4, false, Some(id)), now + COALESCE)
            .is_empty());
        let effects = sync.tick(now + COALESCE + COALESCE);
        let new_id = request_id(sends(&effects)[0]);
        assert!(sync
            .receive(&status(0.1, false, Some(id)), now + COALESCE + COALESCE)
            .is_empty());
        assert!(sync
            .receive(&status(0.8, false, Some(new_id)), now + COALESCE + COALESCE)
            .is_empty());
        assert_eq!(sync.reported.complete(), Some(v(0.8, false)));
    }

    #[test]
    fn mute_and_level_partial_success_is_reconciled_without_retry() {
        let now = Instant::now();
        let mut sync = initialized(now);
        sync.local_changed(v(0.6, true), now);
        let effects = sync.tick(now + COALESCE);
        let sent = sends(&effects);
        assert_eq!(sent.len(), 2);
        let level_id = request_id(sent[0]);
        let mute_id = request_id(sent[1]);
        assert!(sync
            .receive(&status(0.6, false, Some(level_id)), now + COALESCE)
            .is_empty());
        let effects = sync.receive(
            &message(json!({"type":"INVALID_REQUEST","requestId":mute_id})),
            now + COALESCE,
        );
        assert!(effects.contains(&Effect::SetLocal(v(0.6, false))));
        assert_eq!(sends(&effects)[0]["type"], "GET_STATUS");
        assert!(sync.tick(now + Duration::from_secs(3)).is_empty());
    }

    #[test]
    fn status_without_changed_field_does_not_confirm_the_command() {
        let now = Instant::now();
        let mut sync = initialized(now);
        sync.local_changed(v(0.8, false), now);
        let effects = sync.tick(now + COALESCE);
        let id = request_id(sends(&effects)[0]);
        assert!(sync.receive(&message(json!({"type":"RECEIVER_STATUS", "requestId":id, "status":{"volume":{"muted":false}}})),now + COALESCE).is_empty());
        assert!(sync.pending.is_some());
        let effects = sync.tick(now + COALESCE + CONFIRM_TIMEOUT);
        assert!(effects.contains(&Effect::SetLocal(v(0.2, false))));
    }

    #[test]
    fn timeout_rolls_back_and_polls_without_reissuing_volume() {
        let now = Instant::now();
        let mut sync = initialized(now);
        sync.local_changed(v(0.9, false), now);
        sync.tick(now + COALESCE);
        let effects = sync.tick(now + COALESCE + CONFIRM_TIMEOUT);
        assert!(effects.contains(&Effect::SetLocal(v(0.2, false))));
        assert_eq!(sends(&effects).len(), 1);
        assert_eq!(sends(&effects)[0]["type"], "GET_STATUS");
        assert!(sync.tick(now + Duration::from_secs(4)).is_empty());
    }

    #[test]
    fn incomplete_or_malformed_status_is_not_invented() {
        let now = Instant::now();
        let mut sync = VolumeSync::new(now);
        assert!(sync
            .receive(
                &message(json!({"type":"RECEIVER_STATUS","status":{"volume":{"level":0.3}}})),
                now
            )
            .is_empty());
        assert!(sync.receive(&message(json!({"type":"RECEIVER_STATUS","status":{"volume":{"level":10000000,"muted":"no"}}})),now).is_empty());
        assert_eq!(
            sync.receive(
                &message(json!({"type":"RECEIVER_STATUS","status":{"volume":{"muted":true}}})),
                now
            ),
            vec![Effect::SetLocal(v(0.3, true))]
        );
    }

    #[test]
    fn unrelated_messages_and_wrong_destinations_are_ignored() {
        let now = Instant::now();
        let mut sync = initialized(now);
        let mut msg = status(0.8, false, None);
        msg.namespace = "urn:x-cast:com.google.cast.media".into();
        assert!(sync.receive(&msg, now).is_empty());
        msg.namespace = NS_RECEIVER.into();
        msg.source = "app-1".into();
        assert!(sync.receive(&msg, now).is_empty());
        msg.source = PLATFORM_RECEIVER_ID.into();
        msg.destination = "other-sender".into();
        assert!(sync.receive(&msg, now).is_empty());
        assert_eq!(sync.reported.complete(), Some(v(0.2, false)));
    }

    #[test]
    fn fixed_level_is_not_written_but_mute_can_be_requested() {
        let now = Instant::now();
        let mut sync = initialized(now);
        sync.receive(
            &message(json!({"type":"RECEIVER_STATUS","status":{"volume":{"controlType":"fixed"}}})),
            now,
        );
        let effects = sync.local_changed(v(0.8, true), now);
        assert!(effects.contains(&Effect::SetLocal(v(0.2, true))));
        let effects = sync.tick(now + COALESCE);
        let sent = sends(&effects);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0]["volume"], json!({"muted":true}));
    }

    #[test]
    fn missing_initial_status_warns_once_and_polling_continues() {
        let now = Instant::now();
        let mut sync = VolumeSync::new(now);
        sync.tick(now);
        assert!(sync
            .tick(now + INITIAL_WARNING_DELAY)
            .iter()
            .any(|e| matches!(e, Effect::Warning(_))));
        let effects = sync.tick(now + POLL_INTERVAL);
        assert_eq!(sends(&effects)[0]["type"], "GET_STATUS");
        assert!(!effects.iter().any(|e| matches!(e, Effect::Warning(_))));
    }
}
