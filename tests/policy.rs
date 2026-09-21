use base64::{engine::general_purpose::STANDARD, Engine as _};
use cpa_stg::config::Config;
use cpa_stg::engine::{Decision, Engine, Rejection};
use cpa_stg::protocol::handle;
use cpa_stg::runtime::Runtime;
use serde_json::{json, Value};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

fn unlimited_rate() -> Config {
    let mut config = Config::default();
    config.credentials.default.requests_per_minute = 0;
    config.credentials.default.max_concurrency = 1;
    config
}

fn attempt(engine: &mut Engine, id: &str, credential: &str, now: Instant) -> Decision {
    engine.begin(id).unwrap();
    engine.start_attempt(id, credential, now)
}

#[test]
fn token_bucket_refills_without_refunding_completed_requests() {
    let now = Instant::now();
    let mut engine = Engine::new(Config::default());
    assert_eq!(attempt(&mut engine, "a", "channel", now), Decision::Admit);
    engine.complete("a");
    assert_eq!(
        attempt(&mut engine, "b", "channel", now),
        Decision::WaitUntil(now + Duration::from_secs(1))
    );
    assert!(matches!(
        engine.poll("b", now + Duration::from_millis(999)),
        Decision::WaitUntil(_)
    ));
    assert_eq!(
        engine.poll("b", now + Duration::from_secs(1)),
        Decision::Admit
    );
}

#[test]
fn credentials_have_independent_limits_and_queues() {
    let now = Instant::now();
    let mut engine = Engine::new(unlimited_rate());
    assert_eq!(attempt(&mut engine, "a", "one", now), Decision::Admit);
    assert!(matches!(
        attempt(&mut engine, "b", "one", now),
        Decision::WaitUntil(_)
    ));
    assert_eq!(attempt(&mut engine, "c", "two", now), Decision::Admit);
    assert_eq!(engine.counts(), (3, 2, 1));
}

#[test]
fn fifo_prevents_new_requests_from_barging() {
    let now = Instant::now();
    let mut engine = Engine::new(unlimited_rate());
    attempt(&mut engine, "a", "one", now);
    attempt(&mut engine, "b", "one", now);
    attempt(&mut engine, "c", "one", now);
    engine.complete("a");
    assert!(matches!(
        attempt(&mut engine, "d", "one", now),
        Decision::WaitUntil(_)
    ));
    assert!(matches!(engine.poll("c", now), Decision::WaitUntil(_)));
    assert_eq!(engine.poll("b", now), Decision::Admit);
    engine.complete("b");
    assert_eq!(engine.poll("c", now), Decision::Admit);
}

#[test]
fn queue_bound_timeout_and_duplicate_completion_do_not_leak() {
    let now = Instant::now();
    let mut cfg = unlimited_rate();
    cfg.credentials.default.max_queue = 1;
    cfg.credentials.default.queue_timeout_ms = 50;
    let mut engine = Engine::new(cfg);
    attempt(&mut engine, "a", "one", now);
    attempt(&mut engine, "b", "one", now);
    assert_eq!(
        attempt(&mut engine, "c", "one", now),
        Decision::Reject(Rejection::QueueFull)
    );
    assert_eq!(
        engine.poll("b", now + Duration::from_millis(50)),
        Decision::Reject(Rejection::QueueTimeout)
    );
    engine.complete("a");
    engine.complete("a");
    engine.complete("b");
    engine.complete("c");
    assert_eq!(engine.counts(), (0, 0, 0));
}

#[test]
fn cancellation_removes_head_and_cannot_resurrect_request() {
    let now = Instant::now();
    let mut engine = Engine::new(unlimited_rate());
    attempt(&mut engine, "a", "one", now);
    attempt(&mut engine, "b", "one", now);
    attempt(&mut engine, "c", "one", now);
    engine.complete("b");
    engine.complete("a");
    assert_eq!(
        engine.poll("b", now),
        Decision::Reject(Rejection::RequestEnded)
    );
    assert_eq!(
        engine.start_attempt("b", "two", now),
        Decision::Reject(Rejection::RequestEnded)
    );
    assert_eq!(engine.poll("c", now), Decision::Admit);
}

#[test]
fn retries_charge_again_even_when_credential_is_unchanged() {
    let now = Instant::now();
    let mut engine = Engine::new(Config::default());
    assert_eq!(attempt(&mut engine, "a", "one", now), Decision::Admit);
    assert!(matches!(
        engine.start_attempt("a", "one", now),
        Decision::WaitUntil(_)
    ));
    assert_eq!(engine.counts(), (1, 0, 1));
    assert_eq!(
        engine.poll("a", now + Duration::from_secs(1)),
        Decision::Admit
    );
    assert_eq!(
        engine.start_attempt("a", "two", now + Duration::from_secs(1)),
        Decision::Admit
    );
    assert_eq!(engine.counts(), (1, 1, 0));
    engine.complete("a");
    assert_eq!(engine.counts(), (0, 0, 0));
}

#[test]
fn concurrent_duplicate_wait_does_not_replace_queue_position() {
    let now = Instant::now();
    let mut engine = Engine::new(unlimited_rate());
    attempt(&mut engine, "a", "one", now);
    attempt(&mut engine, "b", "one", now);
    assert_eq!(
        engine.start_attempt("b", "two", now),
        Decision::Reject(Rejection::DuplicateWait)
    );
    assert_eq!(engine.counts(), (2, 1, 1));
}

#[test]
fn reconfigure_preserves_debt_active_slots_and_original_queue_deadline() {
    let now = Instant::now();
    let mut cfg = Config::default();
    let mut engine = Engine::new(cfg.clone());
    attempt(&mut engine, "a", "one", now);
    attempt(&mut engine, "b", "one", now);
    cfg.credentials.default.max_concurrency = 1;
    cfg.credentials.default.queue_timeout_ms = 300_000;
    engine.reconfigure(cfg, now).unwrap();
    assert!(matches!(engine.poll("b", now), Decision::WaitUntil(_)));
    engine.complete("a");
    assert!(matches!(engine.poll("b", now), Decision::WaitUntil(_)));
    assert_eq!(
        engine.poll("b", now + Duration::from_secs(30)),
        Decision::Reject(Rejection::QueueTimeout)
    );
}

#[test]
fn disabling_policy_releases_waiters_without_resetting_active_accounting() {
    let now = Instant::now();
    let mut cfg = unlimited_rate();
    let mut engine = Engine::new(cfg.clone());
    attempt(&mut engine, "a", "one", now);
    attempt(&mut engine, "b", "one", now);
    cfg.credentials.default.enabled = false;
    engine.reconfigure(cfg, now).unwrap();
    assert_eq!(engine.poll("b", now), Decision::Admit);
    assert_eq!(engine.counts(), (2, 1, 0));
    engine.complete("a");
    engine.complete("b");
    assert_eq!(engine.counts(), (0, 0, 0));
}

#[test]
fn bucket_eviction_never_resets_rate_debt() {
    let now = Instant::now();
    let cfg = Config {
        max_credentials: 1,
        ..Config::default()
    };
    let mut engine = Engine::new(cfg);
    attempt(&mut engine, "a", "one", now);
    engine.complete("a");
    assert_eq!(
        attempt(&mut engine, "b", "two", now),
        Decision::Reject(Rejection::Capacity)
    );
    assert_eq!(
        engine.start_attempt("b", "two", now + Duration::from_secs(1)),
        Decision::Admit
    );
}

#[test]
fn zero_queue_rejects_immediately_and_zero_limits_disable_only_that_limit() {
    let now = Instant::now();
    let mut cfg = unlimited_rate();
    cfg.credentials.default.max_queue = 0;
    let mut engine = Engine::new(cfg.clone());
    attempt(&mut engine, "a", "one", now);
    assert_eq!(
        attempt(&mut engine, "b", "one", now),
        Decision::Reject(Rejection::QueueFull)
    );
    cfg.credentials.default.max_concurrency = 0;
    engine.reconfigure(cfg, now).unwrap();
    assert_eq!(engine.start_attempt("b", "one", now), Decision::Admit);
}

#[test]
fn tracking_is_bounded_and_stopping_rejects_waiters() {
    let now = Instant::now();
    let mut cfg = unlimited_rate();
    cfg.max_tracked_requests = 2;
    let mut engine = Engine::new(cfg);
    attempt(&mut engine, "a", "one", now);
    attempt(&mut engine, "b", "one", now);
    assert_eq!(engine.begin("c"), Err(Rejection::Capacity));
    engine.stop();
    assert_eq!(engine.poll("b", now), Decision::Reject(Rejection::Stopping));
    assert_eq!(engine.begin("c"), Err(Rejection::Stopping));
}

fn call(runtime: &Runtime, method: &str, request: Value) -> Value {
    handle(runtime, method, request.to_string().as_bytes())
}

#[test]
fn go_wire_protocol_base64_yaml_pascalcase_and_explicit_termination() {
    let runtime = Runtime::default();
    let registered = call(
        &runtime,
        "plugin.register",
        json!({"schema_version": 6,
        "config_yaml": STANDARD.encode("credentials:\n  default:\n    max_queue: 0\n") }),
    );
    assert_eq!(registered["ok"], true);
    assert_eq!(
        registered["result"]["capabilities"]["request_lifecycle_plugin"],
        true
    );
    for id in ["a", "b"] {
        call(
            &runtime,
            "request.intercept_before",
            json!({"RequestID": id, "Body": "aGk="}),
        );
    }
    let req =
        |id| json!({"RequestID": id, "Metadata": {"selected_auth_id": "channel"}, "Body": "aGk="});
    assert_eq!(
        call(&runtime, "request.intercept_after", req("a"))["result"],
        json!({})
    );
    let rejected = call(&runtime, "request.intercept_after", req("b"));
    assert_eq!(rejected["ok"], true);
    assert_eq!(rejected["result"]["Terminate"], true);
    assert_eq!(rejected["result"]["StatusCode"], 429);
    let body: Value = serde_json::from_slice(
        &STANDARD
            .decode(rejected["result"]["ResponseBody"].as_str().unwrap())
            .unwrap(),
    )
    .unwrap();
    assert_eq!(body["error"]["code"], "cpa_queue_full");
    call(
        &runtime,
        "request.complete",
        json!({"RequestID": "a", "Outcome": "succeeded", "Stream": true}),
    );
    assert_eq!(runtime.counts().1, 0);
}

#[test]
fn invalid_config_and_malformed_identity_fail_explicitly() {
    for yaml in [
        "credentials:\n  default:\n    burst: 0",
        "api_keys:\n  key: {}",
        "credentails: {}",
        "credentials:\n  default:\n    queue_timeout_ms: 0",
    ] {
        assert!(Config::parse(yaml.as_bytes()).is_err(), "{yaml}");
    }
    let runtime = Runtime::default();
    assert_eq!(
        call(&runtime, "plugin.register", json!({"schema_version": 1}))["ok"],
        false
    );
    call(
        &runtime,
        "request.intercept_before",
        json!({"RequestID": "a"}),
    );
    let outer = call(
        &runtime,
        "request.intercept_after",
        json!({"RequestID":"a"}),
    );
    assert_eq!(outer["result"], json!({}));
    assert_eq!(runtime.counts(), (1, 0, 0));
    let rejected = call(
        &runtime,
        "request.intercept_after",
        json!({"RequestID": "a", "Metadata":{"selected_auth_id":""}}),
    );
    assert_eq!(rejected["ok"], true);
    assert_eq!(rejected["result"]["StatusCode"], 503);
    let malformed = handle(&runtime, "request.intercept_after", b"{broken");
    assert_eq!(malformed["result"]["Terminate"], true);
}

#[test]
fn credential_overrides_use_exact_opaque_ids() {
    let cfg = Config::parse(b"credentials:\n  default:\n    max_concurrency: 9\n  overrides:\n    channel-a:\n      max_concurrency: 1\n").unwrap();
    assert_eq!(cfg.policy("channel-a").max_concurrency, 1);
    assert_eq!(cfg.policy("channel-b").max_concurrency, 9);
    assert_eq!(cfg.policy("channel-a").requests_per_minute, 60);
}

fn wait_for_queue(runtime: &Runtime) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.counts().2 == 0 {
        assert!(Instant::now() < deadline, "waiter did not enqueue");
        std::thread::yield_now();
    }
}

#[test]
fn blocking_runtime_wakes_on_completion_and_cancellation() {
    for cancel in [false, true] {
        let runtime = Arc::new(Runtime::default());
        runtime.configure(unlimited_rate()).unwrap();
        runtime.begin("a").unwrap();
        runtime.acquire("a", "one").unwrap();
        runtime.begin("b").unwrap();
        let worker = runtime.clone();
        let (tx, rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            tx.send(worker.acquire("b", "one")).unwrap();
        });
        wait_for_queue(&runtime);
        runtime.complete(if cancel { "b" } else { "a" });
        let result = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(
            result,
            if cancel {
                Err(Rejection::RequestEnded)
            } else {
                Ok(())
            }
        );
        thread.join().unwrap();
        runtime.complete("a");
        runtime.complete("b");
        assert_eq!(runtime.counts(), (0, 0, 0));
    }
}

#[test]
fn blocking_runtime_wakes_on_quiesce() {
    let runtime = Arc::new(Runtime::default());
    runtime.configure(unlimited_rate()).unwrap();
    runtime.begin("a").unwrap();
    runtime.acquire("a", "one").unwrap();
    runtime.begin("b").unwrap();
    let worker = runtime.clone();
    let (tx, rx) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        tx.send(worker.acquire("b", "one")).unwrap();
    });
    wait_for_queue(&runtime);
    runtime.stop();
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(5)).unwrap(),
        Err(Rejection::Stopping)
    );
    thread.join().unwrap();
}

#[test]
fn parent_cancellation_removes_nested_queue_and_rejects_late_callbacks() {
    let now = Instant::now();
    let mut cfg = Config::default();
    cfg.credentials.default.requests_per_minute = 0;
    cfg.credentials.default.max_concurrency = 1;
    let mut engine = Engine::new(cfg);
    engine.begin("holder").unwrap();
    assert_eq!(
        engine.start_attempt("holder", "channel", now),
        Decision::Admit
    );
    engine.begin("outer").unwrap();
    engine.begin_child("nested", "outer").unwrap();
    assert!(matches!(
        engine.start_attempt("nested", "channel", now),
        Decision::WaitUntil(_)
    ));
    engine.complete("outer");
    assert_eq!(engine.counts(), (1, 1, 0));
    assert_eq!(
        engine.poll("nested", now),
        Decision::Reject(Rejection::RequestEnded)
    );
    assert_eq!(
        engine.begin_child("late", "outer"),
        Err(Rejection::RequestEnded)
    );
    engine.complete("nested");
    assert_eq!(engine.counts(), (1, 1, 0));
}
