use crate::config::Config;
use crate::engine::Rejection;
use crate::runtime::Runtime;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Deserialize)]
struct Registration {
    schema_version: u32,
    #[serde(default)]
    config_yaml: Option<String>,
}

#[derive(Deserialize)]
struct Request {
    #[serde(rename = "RequestID")]
    id: String,
    #[serde(rename = "Metadata", default)]
    metadata: Value,
    #[serde(rename = "SourceFormat", default)]
    source_format: String,
    #[serde(rename = "Stream", default)]
    stream: bool,
    #[serde(rename = "Headers", default)]
    headers: Value,
}

const PARENT_HEADER: &str = "X-Cpa-Stg-Parent";

pub fn ok(result: Value) -> Value {
    json!({"ok": true, "result": result})
}

pub fn error(message: &str) -> Value {
    json!({"ok": false, "error": {"code": "cpa_stg_error", "message": message}})
}

pub fn terminate(status: u16, code: &str, message: &str) -> Value {
    let body = json!({"error": {"type": if status == 429 { "rate_limit_error" } else { "server_error" },
        "code": code, "message": message}});
    let mut headers = json!({"Content-Type": ["application/json"]});
    if status == 429 {
        headers["Retry-After"] = json!(["1"]);
    }
    // Go []byte fields are base64 strings in the JSON ABI.
    ok(json!({"Terminate": true, "StatusCode": status,
        "ResponseHeaders": headers, "ResponseBody": STANDARD.encode(body.to_string())}))
}

fn rejected(reason: Rejection) -> Value {
    let (status, code, message) = match reason {
        Rejection::QueueFull => (429, "cpa_queue_full", "Credential waiting queue is full"),
        Rejection::QueueTimeout => (429, "cpa_queue_timeout", "Credential queue wait timed out"),
        Rejection::RequestEnded => (
            499,
            "cpa_request_ended",
            "Request already completed or was not registered",
        ),
        Rejection::DuplicateWait => (
            409,
            "cpa_duplicate_wait",
            "Request already has a pending admission",
        ),
        Rejection::Capacity => (503, "cpa_capacity", "Policy tracking capacity reached"),
        Rejection::Stopping => (503, "cpa_stopping", "Policy plugin is stopping"),
    };
    terminate(status, code, message)
}

fn request(raw: &[u8]) -> Result<Request, Value> {
    let req: Request = serde_json::from_slice(raw).map_err(|_| {
        terminate(
            400,
            "cpa_invalid_request",
            "Invalid request interception payload",
        )
    })?;
    if req.id.is_empty() || req.id.len() > 1024 {
        return Err(terminate(
            400,
            "cpa_invalid_request_id",
            "A valid RequestID is required",
        ));
    }
    Ok(req)
}

pub fn registration(schema_version: u32) -> Value {
    ok(json!({
        "schema_version": schema_version,
        "metadata": {
            "Name": "cpa-stg", "Version": env!("CARGO_PKG_VERSION"), "Author": "csbxd", "GitHubRepository": "https://github.com/csbxd/cpa-stg",
            "ConfigFields": [
                {"Name": "max_tracked_requests", "Type": "integer", "Description": "Maximum tracked model executions"},
                {"Name": "max_credentials", "Type": "integer", "Description": "Maximum credential buckets"},
                {"Name": "error_mapping", "Type": "object", "Description": "Configure error mapping on cpa-stg-router"}
            ]
        },
        "capabilities": {"request_interceptor": true, "request_lifecycle_plugin": true}
    }))
}

pub fn handle(runtime: &Runtime, method: &str, raw: &[u8]) -> Value {
    match method {
        "plugin.register" | "plugin.reconfigure" => {
            let result: Result<u32, String> = (|| {
                let req: Registration = serde_json::from_slice(raw)
                    .map_err(|_| "Invalid registration payload".to_string())?;
                if req.schema_version < 2 {
                    return Err("Host schema version 2 or newer is required".into());
                }
                let bytes = STANDARD
                    .decode(req.config_yaml.unwrap_or_default())
                    .map_err(|_| "Invalid base64 config_yaml".to_string())?;
                let cfg = Config::parse(&bytes)?;
                if cfg.error_mapping.enabled {
                    return Err("configure error_mapping on cpa-stg-router, not cpa-stg".into());
                }
                let schema = 2;
                runtime.configure(cfg)?;
                Ok(schema)
            })();
            match result {
                Ok(schema) => registration(schema),
                Err(message) => error(&message),
            }
        }
        "request.intercept_before" => {
            let req = match request(raw) {
                Ok(req) => req,
                Err(response) => return response,
            };
            let responses = req.source_format == "openai-response" && req.stream;
            let parent = if responses && req.metadata["source"] == "plugin_host_model_callback" {
                req.headers
                    .as_object()
                    .and_then(|headers| {
                        headers
                            .iter()
                            .find(|(name, _)| name.eq_ignore_ascii_case(PARENT_HEADER))
                    })
                    .and_then(|(_, values)| values.get(0))
                    .and_then(Value::as_str)
            } else {
                None
            };
            let admission = match parent {
                Some(parent) => runtime.begin_child(&req.id, parent),
                None => runtime.begin(&req.id),
            };
            match admission {
                Ok(()) if responses => ok(json!({"Headers":{PARENT_HEADER:[req.id]}})),
                Ok(()) => ok(json!({})),
                Err(reason) => rejected(reason),
            }
        }
        "request.intercept_after" => {
            let req = match request(raw) {
                Ok(req) => req,
                Err(response) => return response,
            };
            // CPA also invokes after-auth for direct plugin routes before any
            // credential is selected. Only real credential attempts consume quota.
            let Some(value) = req.metadata.get("selected_auth_id") else {
                return ok(json!({}));
            };
            let Some(credential) = value
                .as_str()
                .filter(|id| !id.is_empty() && id.len() <= 1024 && id.trim() == *id)
            else {
                return terminate(503, "cpa_invalid_credential", "Invalid selected_auth_id");
            };
            match runtime.acquire(&req.id, credential) {
                Ok(())
                    if req.headers.as_object().is_some_and(|headers| {
                        headers
                            .keys()
                            .any(|name| name.eq_ignore_ascii_case(PARENT_HEADER))
                    }) =>
                {
                    ok(json!({"ClearHeaders":[PARENT_HEADER]}))
                }
                Ok(()) => ok(json!({})),
                Err(reason) => rejected(reason),
            }
        }
        "request.complete" => match request(raw) {
            Ok(req) => {
                runtime.complete(&req.id);
                ok(json!({}))
            }
            Err(_) => error("Invalid completion payload"),
        },
        "plugin.quiesce" | "plugin.shutdown" => {
            runtime.stop();
            ok(json!({}))
        }
        _ => error("Unsupported plugin method"),
    }
}
