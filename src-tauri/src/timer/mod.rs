pub mod engine;
pub mod sequence;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use tauri::{AppHandle, Emitter, Manager};

use crate::audio::{AudioCue, AudioManager};
use crate::db::{queries, DbState};
use crate::settings::Settings;
use crate::tray::{self, TrayState};
use crate::websocket::{self, WsState};

use engine::{EngineHandle, TimerCommand, TimerEvent};
use sequence::{RoundType, SequenceState};

// ---------------------------------------------------------------------------
// Snapshot — serialized to JSON for the frontend
// ---------------------------------------------------------------------------

/// Full timer state snapshot. Sent as the payload of Tauri events and
/// returned by the `timer_get_state` IPC command.
#[derive(Debug, Clone, Serialize)]
pub struct TimerSnapshot {
    /// "work" | "short-break" | "long-break"
    pub round_type: String,
    /// Round type that was active before this one. Empty string on the first round of a session.
    pub previous_round_type: String,
    pub elapsed_secs: u32,
    pub total_secs: u32,
    pub is_running: bool,
    /// True if the timer has been started and then paused (elapsed > 0, not running).
    pub is_paused: bool,
    pub work_round_number: u32,
    pub work_rounds_total: u32,
    /// Monotonically-increasing focus round count since last reset. Used as a
    /// session counter when long breaks are disabled.
    pub session_work_count: u32,
}

// ---------------------------------------------------------------------------
// Shared mutable state between the controller and the event-listener thread
// ---------------------------------------------------------------------------

struct TimerShared {
    elapsed_secs: u32,
    is_running: bool,
}

// ---------------------------------------------------------------------------
// TimerController — public API registered as Tauri state
// ---------------------------------------------------------------------------

pub struct TimerController {
    engine: EngineHandle,
    sequence: Arc<Mutex<SequenceState>>,
    settings: Arc<Mutex<Settings>>,
    shared: Arc<Mutex<TimerShared>>,
    /// Kept alive so TrayState is not dropped if lib.rs forgets its copy.
    #[allow(dead_code)]
    tray: Arc<TrayState>,
}

impl TimerController {
    /// Construct and start the background threads.
    /// Call once from `lib.rs` during Tauri `setup`.
    pub fn new(
        app: AppHandle,
        settings: Settings,
        tray: Arc<TrayState>,
        db: DbState,
    ) -> Self {
        let seq = SequenceState::new(settings.long_break_interval);
        let duration = seq.current_duration_secs(&settings);

        let (engine, event_rx) = engine::spawn(duration, Duration::from_secs(1));

        let sequence = Arc::new(Mutex::new(seq));
        let settings_arc = Arc::new(Mutex::new(settings));
        let shared = Arc::new(Mutex::new(TimerShared {
            elapsed_secs: 0,
            is_running: false,
        }));

        // Clone handles for the event-listener thread.
        let seq_thread = Arc::clone(&sequence);
        let settings_thread = Arc::clone(&settings_arc);
        let shared_thread = Arc::clone(&shared);
        let engine_thread = engine.clone();
        let tray_thread = Arc::clone(&tray);

        std::thread::Builder::new()
            .name("timer-events".to_string())
            .spawn(move || {
                listen_events(
                    app,
                    event_rx,
                    ListenContext {
                        sequence: seq_thread,
                        settings: settings_thread,
                        shared: shared_thread,
                        engine: engine_thread,
                        tray: tray_thread,
                        db,
                    },
                );
            })
            .expect("failed to spawn timer event listener");

        Self {
            engine,
            sequence,
            settings: settings_arc,
            shared,
            tray,
        }
    }

    #[cfg(test)]
    fn new_for_test(
        settings: Settings,
        tick_interval: Duration,
    ) -> (Self, std::sync::mpsc::Receiver<TimerEvent>) {
        let sequence = SequenceState::new(settings.long_break_interval);
        let duration = sequence.current_duration_secs(&settings);
        let (engine, event_rx) = engine::spawn(duration, tick_interval);
        (
            Self {
                engine,
                sequence: Arc::new(Mutex::new(sequence)),
                settings: Arc::new(Mutex::new(settings)),
                shared: Arc::new(Mutex::new(TimerShared {
                    elapsed_secs: 0,
                    is_running: false,
                })),
                tray: TrayState::new(),
            },
            event_rx,
        )
    }

    // --- Commands ---

    /// Toggle: start a fresh timer if idle, resume if paused, pause if running.
    pub fn toggle(&self) {
        let s = self.shared.lock().unwrap();
        if s.is_running {
            log::info!("[timer] pause");
            self.engine.send(TimerCommand::Pause);
        } else if s.elapsed_secs > 0 {
            log::info!("[timer] resume");
            self.engine.send(TimerCommand::Resume);
        } else {
            log::info!("[timer] start");
            self.engine.send(TimerCommand::Start);
        }
    }

    pub fn reset(&self) {
        log::info!("[timer] reset");
        self.sequence.lock().unwrap().reset();
        // Send only Reset — the event listener's Reset handler will follow up
        // with Prime once the engine is confirmed Idle. Sending a duration
        // update here first would race the Reset and can leave the UI stale.
        self.engine.send(TimerCommand::Reset);
    }

    /// Restart only the current round's timer without touching the sequence.
    /// Round type, round number, and position in the work/break cycle are all
    /// preserved — only the elapsed time is zeroed.
    pub fn restart_round(&self) {
        log::info!("[timer] restart round");
        self.engine.send(TimerCommand::Reset);
    }

    pub fn skip(&self) {
        log::info!("[timer] skip");
        self.engine.send(TimerCommand::Skip);
    }

    pub fn suspend(&self) {
        self.engine.send(TimerCommand::Suspend);
    }

    pub fn wake_resume(&self) {
        self.engine.send(TimerCommand::WakeResume);
    }

    /// Update the duration for the current round when settings change.
    /// Only takes effect after the next Start/Resume (current countdown is not interrupted).
    pub fn reconfigure(&self) {
        let duration = {
            let seq = self.sequence.lock().unwrap();
            let settings = self.settings.lock().unwrap();
            seq.current_duration_secs(&settings)
        };
        self.engine.send(TimerCommand::Reconfigure { duration_secs: duration });
    }

    // --- Query ---

    pub fn get_snapshot(&self) -> TimerSnapshot {
        let seq = self.sequence.lock().unwrap();
        let settings = self.settings.lock().unwrap();
        let shared = self.shared.lock().unwrap();

        TimerSnapshot {
            round_type: seq.current_round.as_str().to_string(),
            previous_round_type: seq.previous_round.map(|r| r.as_str().to_string()).unwrap_or_default(),
            elapsed_secs: shared.elapsed_secs,
            total_secs: seq.current_duration_secs(&settings),
            is_running: shared.is_running,
            is_paused: !shared.is_running && shared.elapsed_secs > 0,
            work_round_number: seq.work_round_number,
            work_rounds_total: seq.work_rounds_total,
            session_work_count: seq.session_work_count,
        }
    }

    /// Apply new settings values. A paused round adopts a changed duration
    /// immediately; if elapsed time no longer fits, restart that round.
    /// Running countdowns are left untouched because Reconfigure would stop
    /// the engine.
    pub fn apply_settings(&self, new: Settings) {
        // Sync work_rounds_total so the round counter and advance() logic both
        // reflect the new long_break_interval immediately.
        let update_command = {
            let mut sequence = self.sequence.lock().unwrap();
            let mut settings = self.settings.lock().unwrap();
            let mut shared = self.shared.lock().unwrap();

            sequence.work_rounds_total = new.long_break_interval;
            let old_duration = sequence.current_duration_secs(&settings);
            *settings = new;
            let duration = sequence.current_duration_secs(&settings);
            let duration_changed = duration != old_duration;
            if shared.is_running {
                None
            } else if shared.elapsed_secs > 0 && duration_changed {
                if shared.elapsed_secs >= duration {
                    shared.elapsed_secs = 0;
                    Some(TimerCommand::UpdatePausedDuration {
                        duration_secs: duration,
                        restart: true,
                    })
                } else {
                    Some(TimerCommand::UpdatePausedDuration {
                        duration_secs: duration,
                        restart: false,
                    })
                }
            } else if shared.elapsed_secs == 0 {
                Some(TimerCommand::Reconfigure {
                    duration_secs: duration,
                })
            } else {
                None
            }
        };
        if let Some(command) = update_command {
            self.engine.send(command);
        }
    }
}

// ---------------------------------------------------------------------------
// Background event listener thread
// ---------------------------------------------------------------------------

struct ListenContext {
    sequence: Arc<Mutex<SequenceState>>,
    settings: Arc<Mutex<Settings>>,
    shared: Arc<Mutex<TimerShared>>,
    engine: EngineHandle,
    tray: Arc<TrayState>,
    db: DbState,
}

fn listen_events(
    app: AppHandle,
    event_rx: std::sync::mpsc::Receiver<TimerEvent>,
    ctx: ListenContext,
) {
    let ListenContext { sequence, settings, shared, engine, tray, db } = ctx;
    // Track last tray progress to throttle redraws to ≥ 1% delta.
    let mut last_tray_progress: f32 = -1.0;
    // Active session row ID for recording (None = not started yet).
    let mut current_session_id: Option<i64> = None;

    while let Ok(event) = event_rx.recv() {
        match event {
            TimerEvent::Started { total_secs } => {
                log::info!("[timer] started total={total_secs}s");
                // A restarted round may have been edited again while idle.
                // Record the duration the engine actually starts with.
                if let Some(session_id) = current_session_id {
                    match db.lock() {
                        Ok(conn) => {
                            if let Err(e) =
                                queries::update_session_duration(&conn, session_id, total_secs)
                            {
                                log::error!("[timer] failed to update session duration: {e}");
                            }
                        }
                        Err(e) => log::error!("[timer] failed to access session database: {e}"),
                    }
                }
                shared.lock().unwrap().is_running = true;
                let _ = app.emit("timer:started", serde_json::json!({ "total_secs": total_secs }));
                if let Some(ws) = app.try_state::<Arc<WsState>>() {
                    websocket::broadcast_started(&ws, total_secs);
                }
                tray::update_menu_items(&tray, true, false);
            }

            TimerEvent::Tick { elapsed_secs, total_secs } => {
                {
                    let mut s = shared.lock().unwrap();
                    s.elapsed_secs = elapsed_secs;
                    s.is_running = true;
                }
                let _ = app.emit(
                    "timer:tick",
                    serde_json::json!({ "elapsed_secs": elapsed_secs, "total_secs": total_secs }),
                );

                // --- Session recording: start on first tick of a new round ---
                if elapsed_secs == 1 && current_session_id.is_none() {
                    let rt = sequence.lock().unwrap().current_round.as_str().to_string();
                    let total = {
                        let seq = sequence.lock().unwrap();
                        let s = settings.lock().unwrap();
                        seq.current_duration_secs(&s)
                    };
                    if let Ok(conn) = db.lock() {
                        match queries::insert_session(&conn, &rt, total) {
                            Ok(id) => current_session_id = Some(id),
                            Err(e) => log::error!("[timer] failed to record session: {e}"),
                        }
                    }
                }

                // --- Tick sound ---
                let rt = sequence.lock().unwrap().current_round.as_str().to_string();
                if let Some(audio) = app.try_state::<Arc<AudioManager>>() {
                    if audio.tick_enabled_for(&rt) {
                        audio.play_cue(AudioCue::Tick);
                    }
                }

                // Update tray arc — throttle to 1% visual change.
                let progress = if total_secs > 0 {
                    elapsed_secs as f32 / total_secs as f32
                } else {
                    0.0
                };
                if (progress - last_tray_progress).abs() >= 0.01 {
                    tray::update_icon(&tray, &rt, false, progress);
                    last_tray_progress = progress;
                }
            }

            TimerEvent::Complete { skipped: was_skipped } => {
                let completed_round = sequence.lock().unwrap().current_round.as_str().to_string();
                log::info!(
                    "[timer] round complete type={completed_round} skipped={was_skipped}"
                );

                // --- Session recording: mark the completed round ---
                if let Some(session_id) = current_session_id.take() {
                    if let Ok(conn) = db.lock() {
                        let _ = queries::complete_session(&conn, session_id, !was_skipped);
                    }
                }

                // Advance sequence.
                let (next_round, next_duration, auto_start_work, auto_start_break) = {
                    let mut seq = sequence.lock().unwrap();
                    let s = settings.lock().unwrap();
                    let (rt, dur) = seq.advance(&s);
                    (rt, dur, s.auto_start_work, s.auto_start_break)
                };

                // Reset shared state for the new round.
                {
                    let mut s = shared.lock().unwrap();
                    s.elapsed_secs = 0;
                    s.is_running = false;
                }

                // Arm the next round's duration without risking a late
                // reconfigure that kicks a freshly-started timer back to Idle.
                engine.send(TimerCommand::Prime {
                    duration_secs: next_duration,
                });

                // Emit round-change with the new snapshot.
                let snapshot = build_snapshot(&sequence, &settings, &shared);
                let _ = app.emit("timer:round-change", snapshot);

                // Desktop notifications are dispatched by the frontend via the
                // notification_show command after receiving the timer:round-change
                // event, so translated strings can be used.

                // Audio alert for the new round.
                if let Some(audio) = app.try_state::<Arc<AudioManager>>() {
                    let cue = match next_round {
                        RoundType::Work => AudioCue::WorkAlert,
                        RoundType::ShortBreak => AudioCue::ShortBreakAlert,
                        RoundType::LongBreak => AudioCue::LongBreakAlert,
                    };
                    audio.play_cue(cue);
                }

                // Lower-priority-during-breaks: when always_on_top is on and
                // break_always_on_top is enabled, disable always-on-top for
                // breaks and restore it when work resumes.
                let (always_on_top, break_always_on_top) = {
                    let s = settings.lock().unwrap();
                    (s.always_on_top, s.break_always_on_top)
                };
                if always_on_top {
                    if let Some(window) = app.get_webview_window("main") {
                        let is_break = next_round != RoundType::Work;
                        let _ = window.set_always_on_top(!(break_always_on_top && is_break));
                    }
                }

                // Update tray to reflect new round type and reset progress.
                // Use -1.0 (same as initialisation and Reset) so the very
                // first tick of the new round always passes the ≥1% threshold,
                // regardless of how long the round is.  Using 0.0 here caused
                // a ≥15-second blank period before the arc started animating.
                let rt = sequence.lock().unwrap().current_round.as_str().to_string();
                tray::update_icon(&tray, &rt, false, 0.0);
                last_tray_progress = -1.0;

                // Broadcast round-change to any connected WebSocket clients.
                if let Some(ws) = app.try_state::<Arc<WsState>>() {
                    let snap = build_snapshot(&sequence, &settings, &shared);
                    websocket::broadcast_round_change(&ws, snap);
                }

                // Auto-start if configured.
                let should_auto = match next_round {
                    RoundType::Work => auto_start_work,
                    _ => auto_start_break,
                };
                if should_auto {
                    log::debug!("[timer] auto-starting {}", next_round.as_str());
                    engine.send(TimerCommand::Start);
                } else {
                    // Timer is idle waiting for the user to start the new round.
                    // Reset the tray menu to "Start" so it doesn't keep showing
                    // "Pause" from the round that just completed.
                    tray::update_menu_items(&tray, false, false);
                }
            }

            TimerEvent::Paused { elapsed_secs } => {
                log::info!("[timer] paused elapsed={elapsed_secs}s");
                shared.lock().unwrap().is_running = false;
                let _ = app.emit("timer:paused", serde_json::json!({ "elapsed_secs": elapsed_secs }));
                if let Some(ws) = app.try_state::<Arc<WsState>>() {
                    websocket::broadcast_paused(&ws, elapsed_secs);
                }

                // Show pause bars in tray.
                let rt = sequence.lock().unwrap().current_round.as_str().to_string();
                let total = {
                    let seq = sequence.lock().unwrap();
                    let s = settings.lock().unwrap();
                    seq.current_duration_secs(&s)
                };
                let progress = if total > 0 { elapsed_secs as f32 / total as f32 } else { 0.0 };
                tray::update_icon(&tray, &rt, true, progress);
                tray::update_menu_items(&tray, false, true);
            }

            TimerEvent::Resumed { elapsed_secs } => {
                log::info!("[timer] resumed elapsed={elapsed_secs}s");
                shared.lock().unwrap().is_running = true;
                let _ = app.emit("timer:resumed", serde_json::json!({ "elapsed_secs": elapsed_secs }));
                if let Some(ws) = app.try_state::<Arc<WsState>>() {
                    websocket::broadcast_resumed(&ws, elapsed_secs);
                }

                // Restore arc in tray.
                let rt = sequence.lock().unwrap().current_round.as_str().to_string();
                let total = {
                    let seq = sequence.lock().unwrap();
                    let s = settings.lock().unwrap();
                    seq.current_duration_secs(&s)
                };
                let progress = if total > 0 { elapsed_secs as f32 / total as f32 } else { 0.0 };
                tray::update_icon(&tray, &rt, false, progress);
                last_tray_progress = progress;
                tray::update_menu_items(&tray, true, false);
            }

            TimerEvent::DurationChanged { duration_secs } => {
                if let Some(session_id) = current_session_id {
                    match db.lock() {
                        Ok(conn) => {
                            if let Err(e) =
                                queries::update_session_duration(&conn, session_id, duration_secs)
                            {
                                log::error!(
                                    "[timer] failed to update session duration: {e}"
                                );
                            }
                        }
                        Err(e) => {
                            log::error!("[timer] failed to access session database: {e}");
                        }
                    }
                }
            }

            TimerEvent::Reset => {
                log::debug!("[timer] idle");
                // Abandon the active session (leave DB row as-is).
                current_session_id = None;

                {
                    let mut s = shared.lock().unwrap();
                    s.elapsed_secs = 0;
                    s.is_running = false;
                }
                let snapshot = build_snapshot(&sequence, &settings, &shared);
                let _ = app.emit("timer:reset", snapshot);
                if let Some(ws) = app.try_state::<Arc<WsState>>() {
                    websocket::broadcast_reset(&ws);
                }

                // Prime the engine with the current round's duration so the
                // next Start uses the correct (possibly settings-updated)
                // total. Using the lighter-weight command here avoids a race
                // where a fast user click on Start is immediately clobbered by
                // a late follow-up duration update.
                let duration = {
                    let seq = sequence.lock().unwrap();
                    let s = settings.lock().unwrap();
                    seq.current_duration_secs(&s)
                };
                engine.send(TimerCommand::Prime { duration_secs: duration });

                // Reset tray to idle (empty arc).
                let rt = sequence.lock().unwrap().current_round.as_str().to_string();
                tray::update_icon(&tray, &rt, false, 0.0);
                last_tray_progress = -1.0;
                tray::update_menu_items(&tray, false, false);
            }

            TimerEvent::Suspended { elapsed_secs } => {
                log::info!("[timer] suspended by system elapsed={elapsed_secs}s");
                shared.lock().unwrap().is_running = false;
                let _ = app.emit(
                    "timer:suspended",
                    serde_json::json!({ "elapsed_secs": elapsed_secs }),
                );

                // Show pause bars while suspended.
                let rt = sequence.lock().unwrap().current_round.as_str().to_string();
                let total = {
                    let seq = sequence.lock().unwrap();
                    let s = settings.lock().unwrap();
                    seq.current_duration_secs(&s)
                };
                let progress = if total > 0 { elapsed_secs as f32 / total as f32 } else { 0.0 };
                tray::update_icon(&tray, &rt, true, progress);
            }
        }
    }
}

fn build_snapshot(
    sequence: &Arc<Mutex<SequenceState>>,
    settings: &Arc<Mutex<Settings>>,
    shared: &Arc<Mutex<TimerShared>>,
) -> TimerSnapshot {
    let seq = sequence.lock().unwrap();
    let s = settings.lock().unwrap();
    let sh = shared.lock().unwrap();

    TimerSnapshot {
        round_type: seq.current_round.as_str().to_string(),
        previous_round_type: seq.previous_round.map(|r| r.as_str().to_string()).unwrap_or_default(),
        elapsed_secs: sh.elapsed_secs,
        total_secs: seq.current_duration_secs(&s),
        is_running: sh.is_running,
        is_paused: !sh.is_running && sh.elapsed_secs > 0,
        work_round_number: seq.work_round_number,
        work_rounds_total: seq.work_rounds_total,
        session_work_count: seq.session_work_count,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TICK: Duration = Duration::from_millis(10);

    fn settings(work_secs: u32) -> Settings {
        Settings {
            time_work_secs: work_secs,
            ..Settings::default()
        }
    }

    fn pause_after(
        timer: &TimerController,
        events: &std::sync::mpsc::Receiver<TimerEvent>,
        ticks: u32,
    ) {
        timer.engine.send(TimerCommand::Start);
        assert!(matches!(
            events.recv_timeout(Duration::from_secs(1)),
            Ok(TimerEvent::Started { .. })
        ));
        timer.shared.lock().unwrap().is_running = true;

        let mut elapsed = 0;
        while elapsed < ticks {
            if let TimerEvent::Tick { elapsed_secs, .. } =
                events.recv_timeout(Duration::from_secs(1)).unwrap()
            {
                elapsed = elapsed_secs;
            }
        }

        timer.engine.send(TimerCommand::Pause);
        loop {
            if let TimerEvent::Paused { elapsed_secs } =
                events.recv_timeout(Duration::from_secs(1)).unwrap()
            {
                let mut shared = timer.shared.lock().unwrap();
                shared.elapsed_secs = elapsed_secs;
                shared.is_running = false;
                break;
            }
        }
    }

    #[test]
    fn shortening_paused_round_beyond_elapsed_restarts_with_new_duration() {
        let (timer, events) = TimerController::new_for_test(settings(10), TICK);
        pause_after(&timer, &events, 7);

        timer.apply_settings(settings(5));

        let snapshot = timer.get_snapshot();
        assert_eq!(snapshot.elapsed_secs, 0);
        assert_eq!(snapshot.total_secs, 5);
        assert!(!snapshot.is_paused);

        timer.toggle();
        assert!(matches!(
            events.recv_timeout(Duration::from_secs(1)),
            Ok(TimerEvent::DurationChanged { duration_secs: 5 })
        ));
        assert!(matches!(
            events.recv_timeout(Duration::from_secs(1)),
            Ok(TimerEvent::Started { total_secs: 5 })
        ));

        let mut ticks = 0;
        loop {
            match events.recv_timeout(Duration::from_secs(1)).unwrap() {
                TimerEvent::Tick { total_secs, .. } => {
                    assert_eq!(total_secs, 5);
                    ticks += 1;
                }
                TimerEvent::Complete { .. } => break,
                _ => {}
            }
        }
        assert_eq!(ticks, 5);
    }

    #[test]
    fn extending_paused_round_preserves_elapsed_and_resumes() {
        let (timer, events) = TimerController::new_for_test(settings(10), TICK);
        pause_after(&timer, &events, 3);

        timer.apply_settings(settings(15));

        let snapshot = timer.get_snapshot();
        let paused_elapsed = snapshot.elapsed_secs;
        assert!(paused_elapsed >= 3);
        assert_eq!(snapshot.total_secs, 15);
        assert!(snapshot.is_paused);

        timer.toggle();
        assert!(matches!(
            events.recv_timeout(Duration::from_secs(1)),
            Ok(TimerEvent::DurationChanged { duration_secs: 15 })
        ));
        assert!(matches!(
            events.recv_timeout(Duration::from_secs(1)),
            Ok(TimerEvent::Resumed { elapsed_secs }) if elapsed_secs == paused_elapsed
        ));
        assert!(matches!(
            events.recv_timeout(Duration::from_secs(1)),
            Ok(TimerEvent::Tick {
                elapsed_secs,
                total_secs: 15
            }) if elapsed_secs == paused_elapsed + 1
        ));
    }
}
