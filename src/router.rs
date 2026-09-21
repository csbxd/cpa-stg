//! Responses-only execution wrapper; credential admission lives in cpa-stg.
use crate::config::Config;
use crate::error_mapping::{failure_from_message, sse, ErrorMapping, SseMapper};
use crate::host::{self, Fault};
use crate::protocol::{error, ok};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::{json, Value};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread::JoinHandle;

struct Worker {
    nested: String,
    downstream: String,
    thread: JoinHandle<()>,
}
struct Router {
    config: Mutex<Config>,
    workers: Mutex<Vec<Worker>>,
    active: AtomicUsize,
    stopping: AtomicBool,
}
static ROUTER: OnceLock<Router> = OnceLock::new();
fn state() -> &'static Router {
    ROUTER.get_or_init(|| Router {
        config: Mutex::new(Config::default()),
        workers: Mutex::new(Vec::new()),
        active: AtomicUsize::new(0),
        stopping: AtomicBool::new(false),
    })
}
struct Active;
impl Drop for Active {
    fn drop(&mut self) {
        state().active.fetch_sub(1, Ordering::SeqCst);
    }
}

pub fn handle(method: &str, raw: &[u8]) -> Value {
    let req: Value = match serde_json::from_slice(raw) {
        Ok(v) => v,
        Err(_) => return error("Invalid router payload"),
    };
    match method {
        "plugin.register" | "plugin.reconfigure" => {
            if req["schema_version"].as_u64().unwrap_or(0) < 2 {
                return error("CPA schema >= 2 required");
            }
            let bytes = match STANDARD.decode(req["config_yaml"].as_str().unwrap_or("")) {
                Ok(b) => b,
                Err(_) => return error("Invalid config encoding"),
            };
            let cfg = match Config::parse(&bytes) {
                Ok(c) => c,
                Err(e) => return error(&e),
            };
            if state().stopping.load(Ordering::SeqCst) {
                return error("Router is stopping");
            }
            *state().config.lock().unwrap() = cfg;
            ok(
                json!({"schema_version": 2, "metadata": {"Name":"cpa-stg-router",
                "Version":env!("CARGO_PKG_VERSION"), "Author":"csbxd", "GitHubRepository":"https://github.com/csbxd/cpa-stg", "ConfigFields":[
                    {"Name":"error_mapping", "Type":"object", "Description":"Ordered Codex Responses retryable error rules"}]},
                "capabilities":{"model_router":true,"executor":true,"executor_model_scope":"static",
                    "executor_input_formats":["openai-response"], "executor_output_formats":["openai-response"]}}),
            )
        }
        "model.route" => {
            let cfg = state().config.lock().unwrap();
            let handled = cfg.enabled
                && cfg.error_mapping.enabled
                && !state().stopping.load(Ordering::SeqCst)
                && req["SourceFormat"] == "openai-response"
                && req["Stream"] == true;
            ok(json!({"Handled":handled,"TargetKind":"self"}))
        }
        "executor.identifier" => ok(json!({"identifier":"cpa-stg-router"})),
        "executor.execute_stream" => match execute(req) {
            Ok(v) => ok(v),
            Err(e) => e.envelope(),
        },
        "plugin.quiesce" | "plugin.shutdown" => {
            shutdown();
            ok(json!({}))
        }
        _ => error("Unsupported router method"),
    }
}

fn execute(req: Value) -> Result<Value, Fault> {
    if req["SourceFormat"] != "openai-response" || req["Stream"] != true {
        return Err(Fault::new("Router only accepts streaming Codex Responses"));
    }
    let cfg = state().config.lock().unwrap().clone();
    if state().stopping.load(Ordering::SeqCst) {
        return Err(Fault::new("Router is stopping"));
    }
    let count = state().active.fetch_add(1, Ordering::SeqCst);
    let active = Active;
    if count >= cfg.max_tracked_requests {
        return Err(Fault::new("Router execution capacity reached"));
    }
    let downstream = req["stream_id"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Fault::new("Missing output stream id"))?
        .to_owned();
    let callback = req["host_callback_id"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Fault::new("Missing callback scope"))?;
    // Always forward the callback scope. CPA excludes this router from nested
    // routing but still executes the separate admission plugin for each attempt.
    let result = host::call(
        "host.model.execute_stream",
        json!({
        "host_callback_id":callback, "entry_protocol":"openai-response", "exit_protocol":"openai-response",
        "model":req["Model"], "stream":true, "body":req["Payload"],
        "headers":req["Headers"], "query":req["Query"], "alt":req["Alt"]}),
    );
    let upstream = match result {
        Ok(v) => v,
        Err(fault) => {
            let failure = failure_from_message(&fault.message);
            if let Some(mapped) = cfg.error_mapping.map(fault.status, &failure) {
                return Ok(
                    json!({"headers":{"Content-Type":["text/event-stream"],"Cache-Control":["no-cache"]},
                    "chunks":[{"Payload":STANDARD.encode(sse(&mapped.rewrite(failure)))}]}),
                );
            }
            return Err(fault);
        }
    };
    let nested = upstream["stream_id"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Fault::new("Missing host model stream id"))?
        .to_owned();
    let mut workers = state().workers.lock().unwrap();
    let mut index = 0;
    while index < workers.len() {
        if workers[index].thread.is_finished() {
            let _ = workers.swap_remove(index).thread.join();
        } else {
            index += 1;
        }
    }
    if state().stopping.load(Ordering::SeqCst) {
        close("host.model.stream_close", &nested);
        return Err(Fault::new("Router is stopping"));
    }
    let nested_worker = nested.clone();
    let downstream_worker = downstream.clone();
    let thread = std::thread::Builder::new()
        .name("cpa-stg-stream".into())
        .spawn(move || {
            let _active = active;
            let result = catch_unwind(AssertUnwindSafe(|| {
                pump(&nested_worker, &downstream_worker, cfg.error_mapping)
            }));
            close("host.model.stream_close", &nested_worker);
            let message = match result {
                Ok(Ok(())) => String::new(),
                Ok(Err(e)) => e.message,
                Err(_) => "Error mapping worker failed".into(),
            };
            let _ = host::call(
                "host.stream.close",
                json!({"stream_id":downstream_worker,"error":message}),
            );
        })
        .map_err(|_| {
            close("host.model.stream_close", &nested);
            Fault::new("Unable to start stream worker")
        })?;
    workers.push(Worker {
        nested,
        downstream,
        thread,
    });
    Ok(json!({"headers":upstream["headers"]}))
}

fn close(method: &str, id: &str) {
    let _ = host::call(method, json!({"stream_id":id}));
}
fn emit(id: &str, payload: &[u8]) -> Result<(), Fault> {
    if !payload.is_empty() {
        host::call(
            "host.stream.emit",
            json!({"stream_id":id,"payload":STANDARD.encode(payload)}),
        )?;
    }
    Ok(())
}
fn pump(nested: &str, downstream: &str, mapping: ErrorMapping) -> Result<(), Fault> {
    let mut mapper = SseMapper::new(mapping.clone());
    loop {
        if state().stopping.load(Ordering::SeqCst) {
            return Ok(());
        }
        let chunk = host::call("host.model.stream_read", json!({"stream_id":nested}))?;
        if let Some(message) = chunk["error"].as_str().filter(|s| !s.is_empty()) {
            emit(downstream, &mapper.finish())?;
            let failure = failure_from_message(message);
            // CPA exposes only the error string for terminal reads: status 0
            // deliberately cannot match a configured HTTP status condition.
            if let Some(mapped) = mapping.map(0, &failure) {
                emit(downstream, &sse(&mapped.rewrite(failure)))?;
                return Ok(());
            }
            return Err(Fault::new(message));
        }
        if let Some(encoded) = chunk["payload"].as_str() {
            let bytes = STANDARD
                .decode(encoded)
                .map_err(|_| Fault::new("Invalid host stream encoding"))?;
            emit(downstream, &mapper.push_host_chunk(&bytes))?;
        }
        if chunk["done"] == true {
            emit(downstream, &mapper.finish())?;
            return Ok(());
        }
    }
}

pub fn shutdown() {
    state().stopping.store(true, Ordering::SeqCst);
    let workers = std::mem::take(&mut *state().workers.lock().unwrap());
    for worker in &workers {
        close("host.stream.close", &worker.downstream);
        close("host.model.stream_close", &worker.nested);
    }
    for worker in workers {
        let _ = worker.thread.join();
    }
}
