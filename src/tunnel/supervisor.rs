//! The supervisor: discovery refreshes, one slot per tunnel server, one
//! reconnect loop per slot.

use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::FutureExt;
use http::HeaderValue;
use rand::Rng;
use rustls::pki_types::ServerName;
use socket2::{SockRef, TcpKeepalive};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{sleep_until, timeout_at, Instant};
use tokio_rustls::TlsConnector;
use tokio_util::sync::CancellationToken;

use super::connection::{
    run_attempt, AttemptContext, AttemptEvent, AttemptOutcome, BoxIo, ConnectionSettings,
};
use super::draining::DrainState;
use super::options::{ReconnectPolicy, TunnelEngineOptions};
use super::targets::{Discovery, Target, TargetResolver};
use super::{Error, Event, StartError, Status, TunnelInfo, TunnelService};
use crate::IdentityVerifier;

const TCP_KEEPALIVE: Duration = Duration::from_secs(75);
const DNS_RETRY_FALLBACK: Duration = Duration::from_secs(30);
const STABLE_CONNECTION: Duration = Duration::from_secs(5);
const EVENT_CAPACITY: usize = 256;

/// Validated values shared by every connection attempt in one engine.
pub(crate) struct Shared<S> {
    pub(crate) service: S,
    pub(crate) options: TunnelEngineOptions,
    pub(crate) identity_verifier: Arc<IdentityVerifier>,
    pub(crate) worker_id: String,
    pub(crate) settings: ConnectionSettings,
    pub(crate) resolver: TargetResolver,
    pub(crate) tls: Option<TlsConnector>,
    pub(crate) status: watch::Sender<Status>,
    pub(crate) events: broadcast::Sender<Event>,
    pub(crate) graceful: CancellationToken,
    pub(crate) force: CancellationToken,
    pub(crate) drain: Arc<DrainState>,
    /// Established connections across every slot.
    pub(crate) connected: AtomicUsize,
}

impl<S> Shared<S> {
    fn emit(&self, event: Event) {
        let _ = self.events.send(event);
    }
}

/// Owns the resolver loop and, transitively through its `JoinSet`s, every
/// slot, attempt, socket, handshake reader, and detached draining session.
pub(crate) struct Engine {
    status: watch::Sender<Status>,
    events: broadcast::Sender<Event>,
    graceful: CancellationToken,
    force: CancellationToken,
    drain: Arc<DrainState>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl Engine {
    pub(crate) fn start<S: TunnelService>(
        service: S,
        options: TunnelEngineOptions,
    ) -> Result<Self, StartError> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| StartError::NoRuntime)?;
        install_identity_crypto_provider();
        let identity_verifier = IdentityVerifier::new(&[options.signing_public_key.as_str()])
            .map_err(StartError::SigningKey)?;
        let resolver =
            TargetResolver::system().map_err(|error| StartError::Resolver(error.to_string()))?;
        let discovery = Discovery::from(options.discovery.clone());
        let tls = match &discovery {
            Discovery::Explicit(targets) if targets.iter().all(Target::is_plaintext) => None,
            _ => Some(build_tls_connector().map_err(StartError::Tls)?),
        };
        let worker_id = options
            .worker_id
            .clone()
            .unwrap_or_else(|| default_worker_id().to_owned());
        let graceful = CancellationToken::new();
        let force = CancellationToken::new();
        let drain = DrainState::new();
        let (status, _) = watch::channel(Status::Starting);
        let (events, _) = broadcast::channel(EVENT_CAPACITY);

        let shared = Arc::new(Shared {
            service,
            settings: ConnectionSettings::from_tuning(&options.tuning),
            options,
            identity_verifier: Arc::new(identity_verifier),
            worker_id,
            resolver,
            tls,
            status: status.clone(),
            events: events.clone(),
            graceful: graceful.clone(),
            force: force.clone(),
            drain: Arc::clone(&drain),
            connected: AtomicUsize::new(0),
        });

        let task = runtime.spawn({
            let status = status.clone();
            async move {
                let result =
                    std::panic::AssertUnwindSafe(supervise(Arc::clone(&shared), discovery))
                        .catch_unwind()
                        .await
                        .unwrap_or_else(|_| Err("tunnel engine panicked".to_owned()));
                if let Err(reason) = &result {
                    shared.emit(Event::Fatal {
                        reason: reason.clone(),
                    });
                }
                let info = match status.borrow().clone() {
                    Status::Ready(info) => Some(info),
                    Status::Terminated { info, .. } => info,
                    Status::Starting => None,
                };
                status.send_replace(Status::Terminated {
                    result: result.map_err(|reason| Error::Fatal { reason }),
                    info,
                });
            }
        });

        Ok(Self {
            status,
            events,
            graceful,
            force,
            drain,
            task: Mutex::new(Some(task)),
        })
    }

    pub(crate) fn status(&self) -> watch::Receiver<Status> {
        self.status.subscribe()
    }

    pub(crate) fn events(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    pub(crate) async fn wait_ready(&self) -> Result<TunnelInfo, Error> {
        let mut status = self.status.subscribe();
        loop {
            match status.borrow_and_update().clone() {
                Status::Starting => {}
                Status::Ready(info) => return Ok(info),
                Status::Terminated {
                    info: Some(info), ..
                } => return Ok(info),
                Status::Terminated { result: Ok(()), .. } => return Err(Error::Closed),
                Status::Terminated {
                    result: Err(error), ..
                } => return Err(error),
            }
            if status.changed().await.is_err() {
                return Err(Error::Closed);
            }
        }
    }

    pub(crate) async fn wait_terminal(&self) -> Result<(), Error> {
        let mut status = self.status.subscribe();
        loop {
            if let Status::Terminated { result, .. } = status.borrow_and_update().clone() {
                return result;
            }
            if status.changed().await.is_err() {
                return Ok(());
            }
        }
    }

    /// Atomically refuse raced requests before waking the supervisor and
    /// connection tasks. This is intentionally synchronous.
    pub(crate) fn begin_shutdown(&self) {
        self.drain.begin();
        self.graceful.cancel();
    }

    /// Force tokens are observed by every live task and socket owner.
    pub(crate) fn force_close(&self) {
        self.drain.begin();
        self.force.cancel();
    }

    pub(crate) fn abort(&self) {
        self.force_close();
        if let Some(task) = self
            .task
            .lock()
            .expect("engine task lock poisoned")
            .as_ref()
        {
            task.abort();
        }
    }

    pub(crate) async fn reap(&self) {
        let task = self.task.lock().expect("engine task lock poisoned").take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.abort();
    }
}

struct Slot {
    generation: u64,
    /// Stop dialing and drain the live connections (a child of the engine's
    /// graceful token, so an engine shutdown stops every slot too). Cutting
    /// everything at once is the engine-wide `force` token.
    stop: CancellationToken,
}

fn discovery_label(discovery: &Discovery) -> String {
    match discovery {
        Discovery::Srv(name) => name.clone(),
        Discovery::Explicit(targets) => targets
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(","),
    }
}

enum SlotResult {
    Stopped,
    /// The reconnect policy's bounds were hit while other servers kept serving.
    GaveUp(String),
    Fatal(String),
}

async fn supervise<S: TunnelService>(
    shared: Arc<Shared<S>>,
    discovery: Discovery,
) -> Result<(), String> {
    let mut slots = HashMap::<Target, Slot>::new();
    let mut tasks = JoinSet::<(Target, u64, SlotResult)>::new();
    let mut generation = 0_u64;
    let mut refresh_at = Instant::now();
    let force = shared.force.clone();
    let graceful = shared.graceful.clone();
    let label = discovery_label(&discovery);
    // Resolutions in a row that yielded nothing to dial, while nothing is connected:
    // they count towards the reconnect policy like failed attempts do, so a wrong SRV
    // name or a host without DNS does not retry silently forever.
    let mut resolution_failures = 0_u32;
    let mut resolution_streak_started_at: Option<Instant> = None;
    let mut resolution_backoff = Backoff::new(shared.options.tuning.reconnect.clone());

    loop {
        if force.is_cancelled() {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            return Ok(());
        }
        if graceful.is_cancelled() {
            while !tasks.is_empty() {
                tokio::select! {
                    _ = force.cancelled() => {
                        tasks.abort_all();
                        while tasks.join_next().await.is_some() {}
                        return Ok(());
                    }
                    joined = tasks.join_next() => {
                        if joined.is_none() {
                            break;
                        }
                    }
                }
            }
            tracing::debug!(
                in_flight = shared.drain.count(),
                "Tunnel connections closed; waiting for in-flight responses"
            );
            tokio::select! {
                _ = force.cancelled() => return Ok(()),
                _ = shared.drain.wait_empty() => return Ok(()),
            }
        }

        if refresh_at <= Instant::now() {
            let resolution = tokio::select! {
                _ = force.cancelled() => continue,
                _ = graceful.cancelled() => continue,
                result = shared.resolver.resolve(&discovery) => result,
            };

            let (failure, retry_in) = match resolution {
                Ok(resolution) => {
                    let desired = resolution.targets.into_iter().collect::<HashSet<_>>();
                    for target in &desired {
                        if slots.contains_key(target) {
                            continue;
                        }
                        generation = generation.wrapping_add(1);
                        let stop = graceful.child_token();
                        tasks.spawn({
                            let target = target.clone();
                            let shared = Arc::clone(&shared);
                            let stop = stop.clone();
                            let slot_generation = generation;
                            async move {
                                let result = run_slot(target.clone(), shared, stop).await;
                                (target, slot_generation, result)
                            }
                        });
                        slots.insert(target.clone(), Slot { generation, stop });
                    }
                    slots.retain(|target, slot| {
                        if desired.contains(target) {
                            true
                        } else {
                            // The server left the record set: stop dialing it and let
                            // its live connections finish their work.
                            tracing::debug!(target = %target, "Tunnel server no longer resolved; draining its connections");
                            slot.stop.cancel();
                            false
                        }
                    });
                    let retry_in = resolution
                        .refresh_after
                        .unwrap_or(Duration::from_secs(365 * 24 * 60 * 60));
                    if desired.is_empty() {
                        (
                            Some("resolution returned no tunnel servers".to_owned()),
                            retry_in,
                        )
                    } else {
                        (None, retry_in)
                    }
                }
                Err(error) => (Some(error.to_string()), DNS_RETRY_FALLBACK),
            };
            refresh_at = Instant::now() + retry_in;

            match failure {
                Some(reason) if slots.is_empty() => {
                    // Nothing to dial: retry like a failed connection attempt would,
                    // with the reconnect backoff rather than the DNS refresh cadence.
                    let retry_in = resolution_backoff.next_delay();
                    refresh_at = Instant::now() + retry_in;
                    resolution_failures = resolution_failures.saturating_add(1);
                    resolution_streak_started_at.get_or_insert_with(Instant::now);
                    tracing::warn!(discovery = %label, %reason, retry_in = ?retry_in, "Tunnel target resolution yielded nothing; retrying");
                    if let Some(gave_up) = give_up_reason(
                        &shared.options.tuning.reconnect,
                        resolution_failures,
                        resolution_streak_started_at,
                    ) {
                        if shared.connected.load(Ordering::SeqCst) == 0 {
                            tasks.abort_all();
                            while tasks.join_next().await.is_some() {}
                            return Err(format!("resolving {label}: {reason} ({gave_up})"));
                        }
                    }
                    shared.emit(Event::AttemptFailed {
                        target: label.clone(),
                        reason,
                        retry_in,
                    });
                }
                Some(reason) => {
                    tracing::warn!(discovery = %label, %reason, "Tunnel target resolution failed; retaining healthy connections");
                }
                None => {
                    resolution_failures = 0;
                    resolution_streak_started_at = None;
                    resolution_backoff.reset();
                }
            }
        }

        tokio::select! {
            _ = force.cancelled() => {},
            _ = graceful.cancelled() => {},
            _ = sleep_until(refresh_at) => {},
            joined = tasks.join_next(), if !tasks.is_empty() => {
                match joined {
                    Some(Ok((target, joined_generation, SlotResult::Stopped))) => {
                        if slots.get(&target).is_some_and(|slot| slot.generation == joined_generation) {
                            slots.remove(&target);
                            // A slot should not vanish by itself; resolve again so the
                            // next loop recreates it from the current discovery set.
                            refresh_at = Instant::now();
                        }
                    }
                    Some(Ok((target, joined_generation, SlotResult::GaveUp(reason)))) => {
                        if slots.get(&target).is_some_and(|slot| slot.generation == joined_generation) {
                            slots.remove(&target);
                            // Retried with a fresh streak after the longest backoff (a DNS
                            // refresh due earlier brings it back earlier).
                            refresh_at = refresh_at.min(Instant::now() + shared.options.tuning.reconnect.max_interval);
                        }
                        tracing::warn!(target = %target, %reason, "Gave up reconnecting to a tunnel server; other connections keep serving");
                        shared.emit(Event::GaveUp { target: target.to_string(), reason });
                    }
                    Some(Ok((_, _, SlotResult::Fatal(reason)))) => {
                        force.cancel();
                        tasks.abort_all();
                        while tasks.join_next().await.is_some() {}
                        return Err(reason);
                    }
                    Some(Err(join_error)) if join_error.is_panic() => {
                        force.cancel();
                        tasks.abort_all();
                        while tasks.join_next().await.is_some() {}
                        return Err(format!("tunnel slot panicked: {join_error}"));
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn run_slot<S: TunnelService>(
    target: Target,
    shared: Arc<Shared<S>>,
    slot_stop: CancellationToken,
) -> SlotResult {
    let mut attempts = JoinSet::<(u64, AttemptOutcome)>::new();
    let (events_tx, mut events_rx) = tokio::sync::mpsc::channel(32);
    let mut next_attempt_id = 1_u64;
    let mut current = None;
    let mut established = HashSet::<u64>::new();
    let mut backoff = Backoff::new(shared.options.tuning.reconnect.clone());
    let mut streak_attempts = 0_u32;
    let mut streak_started_at: Option<Instant> = None;
    let mut next_dial = Instant::now();
    let mut stopping_gracefully = false;
    // When this slot alone is stopped (its server left the record set), its live
    // connections get the server-drain grace to finish; an engine shutdown bounds
    // the wait itself.
    let mut removal_deadline: Option<Instant> = None;
    let force = shared.force.clone();
    let graceful = shared.graceful.clone();
    let target_label = target.to_string();

    loop {
        if force.is_cancelled() {
            attempts.abort_all();
            while attempts.join_next().await.is_some() {}
            release_established(&shared, &mut established);
            return SlotResult::Stopped;
        }
        if slot_stop.is_cancelled() {
            stopping_gracefully = true;
            if !graceful.is_cancelled() && removal_deadline.is_none() {
                removal_deadline = Some(Instant::now() + shared.options.tuning.server_drain_grace);
            }
        }
        if stopping_gracefully && attempts.is_empty() {
            release_established(&shared, &mut established);
            return SlotResult::Stopped;
        }
        if let Some(deadline) = removal_deadline {
            if deadline <= Instant::now() {
                tracing::warn!(target = %target, "Tunnel server left the record set and its connections did not drain in time; cutting them");
                attempts.abort_all();
                while attempts.join_next().await.is_some() {}
                release_established(&shared, &mut established);
                return SlotResult::Stopped;
            }
        }

        if !stopping_gracefully && current.is_none() && next_dial <= Instant::now() {
            let attempt_id = next_attempt_id;
            next_attempt_id = next_attempt_id.wrapping_add(1);
            current = Some(attempt_id);
            streak_attempts = streak_attempts.saturating_add(1);
            streak_started_at.get_or_insert_with(Instant::now);
            shared.emit(Event::Connecting {
                target: target_label.clone(),
            });
            attempts.spawn({
                let target = target.clone();
                let shared = Arc::clone(&shared);
                let events = events_tx.clone();
                let slot_stop = slot_stop.clone();
                async move {
                    let outcome =
                        run_one_attempt(attempt_id, target, shared, events, slot_stop).await;
                    (attempt_id, outcome)
                }
            });
        }

        let removal_cut = async {
            match removal_deadline {
                Some(deadline) => sleep_until(deadline).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            _ = force.cancelled() => {},
            _ = slot_stop.cancelled(), if !stopping_gracefully => {},
            _ = removal_cut => {},
            _ = sleep_until(next_dial), if !stopping_gracefully && current.is_none() => {},
            Some(event) = events_rx.recv() => {
                match event {
                    AttemptEvent::Established { attempt_id, info } => {
                        established.insert(attempt_id);
                        shared.connected.fetch_add(1, Ordering::SeqCst);
                        if !graceful.is_cancelled() && !force.is_cancelled() {
                            shared.status.send_if_modified(|state| {
                                if matches!(state, Status::Starting) {
                                    *state = Status::Ready(info.clone());
                                    true
                                } else {
                                    false
                                }
                            });
                        }
                        tracing::debug!(attempt_id, target = %target, "Tunnel connection established");
                        shared.emit(Event::Connected { target: target_label.clone(), info });
                    }
                    AttemptEvent::ServerDrain { attempt_id, uptime } => {
                        if current == Some(attempt_id) {
                            current = None;
                        }
                        if uptime >= STABLE_CONNECTION {
                            backoff.reset();
                            streak_attempts = 0;
                            streak_started_at = None;
                        }
                        // A server drain is a handover request, not a failed
                        // connection. Keep this attempt serving while its
                        // replacement starts without a retry delay.
                        if current.is_none() {
                            next_dial = Instant::now();
                        }
                        tracing::debug!(attempt_id, target = %target, "Tunnel server asked to drain; reconnecting");
                        shared.emit(Event::ServerDrain { target: target_label.clone() });
                    }
                }
            }
            joined = attempts.join_next(), if !attempts.is_empty() => {
                match joined {
                    Some(Ok((attempt_id, AttemptOutcome::Fatal(reason)))) => {
                        if established.remove(&attempt_id) {
                            shared.connected.fetch_sub(1, Ordering::SeqCst);
                        }
                        attempts.abort_all();
                        while attempts.join_next().await.is_some() {}
                        release_established(&shared, &mut established);
                        return SlotResult::Fatal(reason);
                    }
                    Some(Ok((attempt_id, outcome))) => {
                        let was_established = established.remove(&attempt_id);
                        if was_established {
                            shared.connected.fetch_sub(1, Ordering::SeqCst);
                        }
                        if current != Some(attempt_id) {
                            // A drained predecessor finished; nothing to schedule.
                            if was_established {
                                shared.emit(Event::Disconnected { target: target_label.clone(), reason: None });
                            }
                            continue;
                        }
                        current = None;
                        let mut failure = None;
                        match outcome {
                            AttemptOutcome::Served { uptime } => {
                                if uptime >= STABLE_CONNECTION {
                                    backoff.reset();
                                    streak_attempts = 0;
                                    streak_started_at = None;
                                }
                                tracing::debug!(attempt_id, target = %target, "Tunnel connection ended; reconnecting");
                                shared.emit(Event::Disconnected { target: target_label.clone(), reason: None });
                            }
                            AttemptOutcome::Retryable(reason) => {
                                tracing::warn!(attempt_id, target = %target, %reason, "Tunnel connection attempt failed; retrying");
                                failure = Some(reason);
                            }
                            AttemptOutcome::Cancelled | AttemptOutcome::Fatal(_) => {}
                        }
                        if !stopping_gracefully {
                            if let Some(reason) = give_up_reason(
                                &shared.options.tuning.reconnect,
                                streak_attempts,
                                streak_started_at,
                            ) {
                                attempts.abort_all();
                                while attempts.join_next().await.is_some() {}
                                release_established(&shared, &mut established);
                                if shared.connected.load(Ordering::SeqCst) > 0 {
                                    return SlotResult::GaveUp(reason);
                                }
                                return SlotResult::Fatal(reason);
                            }
                            let delay = backoff.next_delay();
                            next_dial = Instant::now() + delay;
                            if let Some(reason) = failure {
                                shared.emit(Event::AttemptFailed { target: target_label.clone(), reason, retry_in: delay });
                            }
                        }
                    }
                    Some(Err(error)) if error.is_panic() => {
                        attempts.abort_all();
                        while attempts.join_next().await.is_some() {}
                        release_established(&shared, &mut established);
                        return SlotResult::Fatal(format!("connection task panicked: {error}"));
                    }
                    _ => {}
                }
            }
        }
    }
}

fn release_established<S>(shared: &Shared<S>, established: &mut HashSet<u64>) {
    if !established.is_empty() {
        shared
            .connected
            .fetch_sub(established.len(), Ordering::SeqCst);
        established.clear();
    }
}

fn give_up_reason(
    policy: &ReconnectPolicy,
    attempts: u32,
    streak_started_at: Option<Instant>,
) -> Option<String> {
    if let Some(max_attempts) = policy.max_attempts {
        if attempts >= max_attempts {
            return Some(format!(
                "gave up reconnecting after {attempts} attempt(s) (reconnect.max_attempts)"
            ));
        }
    }
    if let (Some(max_duration), Some(started_at)) = (policy.max_duration, streak_started_at) {
        let elapsed = started_at.elapsed();
        if elapsed >= max_duration {
            return Some(format!(
                "gave up reconnecting after {elapsed:?} (reconnect.max_duration)"
            ));
        }
    }
    None
}

async fn run_one_attempt<S: TunnelService>(
    attempt_id: u64,
    target: Target,
    shared: Arc<Shared<S>>,
    events: tokio::sync::mpsc::Sender<AttemptEvent>,
    slot_stop: CancellationToken,
) -> AttemptOutcome {
    let authorization = match shared.options.bearer() {
        Ok(value) => match HeaderValue::from_str(&value) {
            Ok(mut value) => {
                value.set_sensitive(true);
                value
            }
            Err(_) => {
                return AttemptOutcome::Retryable(
                    "auth token contains characters that cannot travel in an HTTP header".into(),
                )
            }
        },
        Err(error) => return AttemptOutcome::Retryable(error.to_string()),
    };
    let connection_id = uuid::Uuid::new_v4().simple().to_string();
    let io = match dial_target(&target, &shared, &slot_stop).await {
        Ok(io) => io,
        Err(error) => return AttemptOutcome::Retryable(error),
    };

    run_attempt(
        io,
        AttemptContext {
            attempt_id,
            service: shared.service.clone(),
            identity_verifier: Arc::clone(&shared.identity_verifier),
            authorization,
            environment_id: shared.options.environment_id.clone(),
            tunnel_name: shared.options.name.clone(),
            tunnel_worker_id: shared.worker_id.clone(),
            tunnel_connection_id: connection_id,
            target: target.to_string(),
            drain: Arc::clone(&shared.drain),
            graceful: slot_stop,
            force: shared.force.clone(),
            events,
            settings: shared.settings,
        },
    )
    .await
}

async fn dial_target<S>(
    target: &Target,
    shared: &Shared<S>,
    graceful: &CancellationToken,
) -> Result<BoxIo, String> {
    let force = &shared.force;
    let addresses = tokio::select! {
        _ = graceful.cancelled() => return Err("tunnel is draining".into()),
        _ = force.cancelled() => return Err("tunnel closed".into()),
        result = shared.resolver.resolve_dial_addresses(target) => result.map_err(|error| error.to_string())?,
    };
    let deadline = Instant::now() + shared.options.tuning.connect_timeout;
    let mut last_error = None;

    for address in addresses {
        let connect = timeout_at(deadline, TcpStream::connect(address));
        let stream = match tokio::select! {
            _ = graceful.cancelled() => return Err("tunnel is draining".into()),
            _ = force.cancelled() => return Err("tunnel closed".into()),
                result = connect => result,
        } {
            Ok(Ok(stream)) => stream,
            Ok(Err(error)) => {
                last_error = Some(error.to_string());
                continue;
            }
            Err(_) => return Err("TCP/TLS connect deadline elapsed".into()),
        };
        if let Err(error) = configure_tcp(&stream) {
            last_error = Some(error.to_string());
            continue;
        }
        if target.is_plaintext() {
            return Ok(Box::new(stream));
        }

        let tls = shared
            .tls
            .as_ref()
            .ok_or_else(|| "TLS connector is unavailable".to_owned())?;
        let server_name = ServerName::try_from(target.server_name().to_owned())
            .map_err(|_| format!("invalid TLS server name {:?}", target.server_name()))?;
        let connect = timeout_at(deadline, tls.connect(server_name, stream));
        let stream = match tokio::select! {
            _ = graceful.cancelled() => return Err("tunnel is draining".into()),
            _ = force.cancelled() => return Err("tunnel closed".into()),
                result = connect => result,
        } {
            Ok(Ok(stream)) => stream,
            Ok(Err(error)) => {
                last_error = Some(error.to_string());
                continue;
            }
            Err(_) => return Err("TCP/TLS connect deadline elapsed".into()),
        };
        if stream.get_ref().1.alpn_protocol() != Some(b"h2") {
            return Err("tunnel server did not negotiate h2 ALPN".into());
        }
        return Ok(Box::new(stream));
    }

    Err(last_error.unwrap_or_else(|| "no address could be connected".into()))
}

fn configure_tcp(stream: &TcpStream) -> io::Result<()> {
    stream.set_nodelay(true)?;
    SockRef::from(stream).set_tcp_keepalive(&TcpKeepalive::new().with_time(TCP_KEEPALIVE))
}

/// Select the same deterministic precedence as the tunnel's rustls provider.
/// jsonwebtoken cannot infer a provider when Cargo unifies both backend
/// features, so install it before any signed request can reach the verifier.
/// An application-installed provider wins.
fn install_identity_crypto_provider() {
    #[cfg(feature = "rust_crypto")]
    let _ = jsonwebtoken::crypto::rust_crypto::DEFAULT_PROVIDER.install_default();

    #[cfg(all(not(feature = "rust_crypto"), feature = "aws_lc_rs"))]
    let _ = jsonwebtoken::crypto::aws_lc::DEFAULT_PROVIDER.install_default();
}

fn build_tls_connector() -> Result<TlsConnector, String> {
    #[cfg(feature = "rust_crypto")]
    let provider = rustls::crypto::ring::default_provider();

    #[cfg(all(not(feature = "rust_crypto"), feature = "aws_lc_rs"))]
    let provider = rustls::crypto::aws_lc_rs::default_provider();

    #[cfg(not(any(feature = "rust_crypto", feature = "aws_lc_rs")))]
    return Err("the tunnel feature requires either the rust_crypto or aws_lc_rs feature".into());

    #[cfg(any(feature = "rust_crypto", feature = "aws_lc_rs"))]
    {
        let native = rustls_native_certs::load_native_certs();
        let mut roots = rustls::RootCertStore::empty();
        let (added, _) = roots.add_parsable_certificates(native.certs);
        if added == 0 {
            return Err(format!(
                "no usable native TLS roots were found{}",
                if native.errors.is_empty() {
                    String::new()
                } else {
                    format!(" ({:?})", native.errors)
                }
            ));
        }
        if !native.errors.is_empty() {
            tracing::warn!(errors = ?native.errors, "Some native TLS roots could not be loaded");
        }
        let mut config = rustls::ClientConfig::builder_with_provider(provider.into())
            .with_safe_default_protocol_versions()
            .map_err(|error| format!("TLS protocol configuration failed: {error}"))?
            .with_root_certificates(roots)
            .with_no_client_auth();
        config.alpn_protocols = vec![b"h2".to_vec()];
        Ok(TlsConnector::from(Arc::new(config)))
    }
}

fn default_worker_id() -> &'static str {
    static WORKER_ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    WORKER_ID.get_or_init(|| {
        let host = hostname::get()
            .ok()
            .and_then(|host| host.into_string().ok())
            .unwrap_or_else(|| "worker".to_owned());
        let host = sanitize_worker_id_segment(&host);
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        format!("{host}-{}", &suffix[..8])
    })
}

const DEFAULT_WORKER_HOST_MAX_BYTES: usize = 96;

fn sanitize_worker_id_segment(value: &str) -> String {
    let mut result = String::with_capacity(value.len().min(DEFAULT_WORKER_HOST_MAX_BYTES));
    let mut last_was_dash = false;
    for byte in value.bytes() {
        let allowed = byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-');
        let byte = if allowed { byte } else { b'-' };
        if byte == b'-' && last_was_dash {
            continue;
        }
        result.push(char::from(byte));
        last_was_dash = byte == b'-';
        if result.len() == DEFAULT_WORKER_HOST_MAX_BYTES {
            break;
        }
    }
    let trimmed = result.trim_matches('-');
    if trimmed.is_empty() {
        "worker".to_owned()
    } else {
        trimmed.to_owned()
    }
}

struct Backoff {
    policy: ReconnectPolicy,
    next: Duration,
}

impl Backoff {
    fn new(policy: ReconnectPolicy) -> Self {
        Self {
            next: policy.initial_interval,
            policy,
        }
    }

    fn reset(&mut self) {
        self.next = self.policy.initial_interval;
    }

    fn next_delay(&mut self) -> Duration {
        let millis = self.next.as_millis().min(u128::from(u64::MAX)) as u64;
        let lower = millis / 2;
        let upper = millis.saturating_mul(3) / 2;
        let jittered = Duration::from_millis(rand::thread_rng().gen_range(lower..=upper));
        self.next = self
            .next
            .saturating_mul(self.policy.factor.max(1))
            .min(self.policy.max_interval);
        jittered
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_is_bounded_and_resets() {
        let policy = ReconnectPolicy::default();
        let mut backoff = Backoff::new(policy.clone());
        for _ in 0..64 {
            assert!(backoff.next_delay() <= policy.max_interval + policy.max_interval / 2);
        }
        backoff.reset();
        assert!(backoff.next_delay() <= policy.initial_interval + policy.initial_interval / 2);
    }

    #[test]
    fn give_up_follows_the_policy() {
        let unbounded = ReconnectPolicy::default();
        assert!(give_up_reason(&unbounded, 1_000, Some(Instant::now())).is_none());

        let bounded = ReconnectPolicy {
            max_attempts: Some(3),
            ..ReconnectPolicy::default()
        };
        assert!(give_up_reason(&bounded, 2, None).is_none());
        assert!(give_up_reason(&bounded, 3, None).is_some());

        let timed = ReconnectPolicy {
            max_duration: Some(Duration::from_secs(1)),
            ..ReconnectPolicy::default()
        };
        assert!(give_up_reason(&timed, 1, Some(Instant::now())).is_none());
        assert!(give_up_reason(&timed, 1, Some(Instant::now() - Duration::from_secs(2))).is_some());
    }

    #[test]
    fn sanitizes_worker_host_segments() {
        assert_eq!(
            sanitize_worker_id_segment(" pod name / one "),
            "pod-name-one"
        );
        assert_eq!(sanitize_worker_id_segment("///"), "worker");
        assert_eq!(sanitize_worker_id_segment("a---b"), "a-b");
        assert!(
            super::super::options::parse_server_address(&format!("{}:1", default_worker_id()))
                .is_ok()
        );
    }

    #[tokio::test]
    async fn terminal_state_retains_raced_readiness() {
        let info = TunnelInfo::from_handshake(
            "requested".into(),
            "https://proxy.example/environment/requested".into(),
            "https://tunnel.example/requested".into(),
        )
        .unwrap();
        let (status, _) = watch::channel(Status::Terminated {
            result: Ok(()),
            info: Some(info.clone()),
        });
        let (events, _) = broadcast::channel(1);
        let engine = Engine {
            status,
            events,
            graceful: CancellationToken::new(),
            force: CancellationToken::new(),
            drain: DrainState::new(),
            task: Mutex::new(None),
        };

        assert_eq!(engine.wait_ready().await.unwrap(), info);
    }
}
