//! Opt-in rewriting of structured Responses failures for Codex clients.
use serde::{Deserialize, Deserializer};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::time::Duration;

pub const RETRYABLE_CODE: &str = "cpa_retryable";

/// CPA's errors often preserve a JSON error body in their message. Do not
/// invent an upstream code or status if only a plain string survived.
pub fn failure_from_message(message: &str) -> Value {
    if let Ok(value) = serde_json::from_str::<Value>(message) {
        if value["type"] == "response.failed"
            && value
                .pointer("/response/error")
                .is_some_and(Value::is_object)
        {
            return value;
        }
        if value.get("error").is_some_and(Value::is_object) {
            return json!({"type":"response.failed","response":{"status":"failed","error":value["error"]}});
        }
    }
    json!({"type":"response.failed","response":{"status":"failed","error":{"message":message}}})
}

pub fn sse(event: &Value) -> Vec<u8> {
    format!("event: response.failed\ndata: {event}\n\n").into_bytes()
}

/// Frame-aware rewrite: handles split and coalesced LF/CRLF SSE events and
/// preserves unmatched frames byte-for-byte. Oversized events switch payload
/// rewriting off for this stream instead of buffering an unbounded response.
pub struct SseMapper {
    mapping: ErrorMapping,
    pending: Vec<u8>,
    bypass: bool,
    scan: usize,
}
impl SseMapper {
    pub fn new(mapping: ErrorMapping) -> Self {
        Self {
            mapping,
            pending: Vec::new(),
            bypass: false,
            scan: 0,
        }
    }
    /// CPA's Codex translator returns individual SSE lines without newline
    /// delimiters. Reframe these host records before applying SSE buffering.
    /// Already-framed or fragmented byte streams use the normal parser.
    pub fn push_host_chunk(&mut self, bytes: &[u8]) -> Vec<u8> {
        if !bytes.contains(&b'\n') && !bytes.contains(&b'\r') {
            if let Some(data) = bytes.strip_prefix(b"data:") {
                let text = std::str::from_utf8(data).unwrap_or("").trim();
                if text == "[DONE]" || serde_json::from_slice::<Value>(data).is_ok() {
                    let mut framed = bytes.to_vec();
                    framed.extend_from_slice(b"\n\n");
                    return self.push(&framed);
                }
            } else if bytes.starts_with(b"event:") {
                let mut line = bytes.to_vec();
                line.push(b'\n');
                return self.push(&line);
            }
        }
        self.push(bytes)
    }
    pub fn push(&mut self, bytes: &[u8]) -> Vec<u8> {
        if self.bypass {
            return bytes.to_vec();
        }
        self.pending.extend_from_slice(bytes);
        let mut output = Vec::new();
        let mut start = 0;
        let mut cursor = self.scan;
        while cursor < self.pending.len() {
            let tail = &self.pending[cursor..];
            let delimiter = if tail.starts_with(b"\r\n\r\n") {
                4
            } else if tail.starts_with(b"\n\n") {
                2
            } else {
                cursor += 1;
                continue;
            };
            let end = cursor + delimiter;
            let frame = &self.pending[start..end];
            output.extend(self.rewrite_frame(frame));
            start = end;
            cursor = end;
        }
        self.pending.drain(..start);
        self.scan = self.pending.len().saturating_sub(3);
        if self.pending.len() > 1024 * 1024 {
            self.bypass = true;
            output.append(&mut self.pending);
        }
        output
    }
    fn rewrite_frame(&self, frame: &[u8]) -> Vec<u8> {
        let Ok(text) = std::str::from_utf8(frame) else {
            return frame.to_vec();
        };
        let data = text
            .lines()
            .filter_map(|line| {
                line.strip_prefix("data:")
                    .map(|v| v.strip_prefix(' ').unwrap_or(v))
            })
            .collect::<Vec<_>>()
            .join("\n");
        let Ok(value) = serde_json::from_str::<Value>(&data) else {
            return frame.to_vec();
        };
        let failure = match value["type"].as_str() {
            Some("response.failed") => value,
            Some("error") if value["error"].is_object() => failure_from_message(&data),
            _ => return frame.to_vec(),
        };
        match self.mapping.map(0, &failure) {
            Some(mapped) => sse(&mapped.rewrite(failure)),
            None => frame.to_vec(),
        }
    }
    pub fn finish(&mut self) -> Vec<u8> {
        self.scan = 0;
        std::mem::take(&mut self.pending)
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ErrorMapping {
    pub enabled: bool,
    pub rules: Vec<ErrorRule>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorRule {
    pub name: String,
    #[serde(rename = "match")]
    pub matcher: ErrorMatch,
    pub retryable: RetryableError,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ErrorMatch {
    pub codes: Vec<String>,
    pub types: Vec<String>,
    pub http_statuses: Vec<u16>,
    pub message_contains: Option<String>,
}

/// The client-side target is ApiError::Retryable { message, delay }.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryableError {
    pub message: String,
    #[serde(default, deserialize_with = "deserialize_delay")]
    pub delay: Option<Duration>,
}

fn deserialize_delay<'de, D: Deserializer<'de>>(de: D) -> Result<Option<Duration>, D::Error> {
    let Some(raw) = Option::<String>::deserialize(de)? else {
        return Ok(None);
    };
    let (digits, multiplier) = if let Some(n) = raw.strip_suffix("ms") {
        (n, 1)
    } else if let Some(n) = raw.strip_suffix('s') {
        (n, 1000)
    } else if let Some(n) = raw.strip_suffix('m') {
        (n, 60_000)
    } else {
        return Err(serde::de::Error::custom("delay requires ms, s, or m"));
    };
    if digits.is_empty() || !digits.bytes().all(|c| c.is_ascii_digit()) {
        return Err(serde::de::Error::custom(
            "delay requires a nonnegative integer",
        ));
    }
    let ms = digits
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(multiplier))
        .filter(|ms| *ms <= 300_000)
        .ok_or_else(|| serde::de::Error::custom("delay must not exceed 5 minutes"))?;
    Ok(Some(Duration::from_millis(ms)))
}

impl ErrorMapping {
    pub fn validate(&self) -> Result<(), String> {
        if self.rules.len() > 128 {
            return Err("error_mapping supports at most 128 rules".into());
        }
        if self.enabled && self.rules.is_empty() {
            return Err("enabled error_mapping requires rules".into());
        }
        let mut names = HashSet::new();
        for rule in &self.rules {
            if rule.name.trim().is_empty() || rule.name.len() > 128 || !names.insert(&rule.name) {
                return Err(
                    "error mapping rule names must be nonempty, unique, and at most 128 bytes"
                        .into(),
                );
            }
            let m = &rule.matcher;
            if m.codes.is_empty()
                && m.types.is_empty()
                && m.http_statuses.is_empty()
                && m.message_contains.is_none()
            {
                return Err("error mapping requires at least one match condition".into());
            }
            if m.codes.len() > 64
                || m.types.len() > 64
                || m.http_statuses.len() > 200
                || m.codes
                    .iter()
                    .chain(m.types.iter())
                    .any(|v| v.is_empty() || v.len() > 256)
                || m.http_statuses.iter().any(|s| !(400..=599).contains(s))
                || m.message_contains
                    .as_ref()
                    .is_some_and(|v| v.is_empty() || v.len() > 4096)
            {
                return Err("invalid error mapping match condition".into());
            }
            if rule.retryable.message.trim().is_empty() || rule.retryable.message.len() > 4096 {
                return Err("retryable message must be nonempty and at most 4096 bytes".into());
            }
            if rule
                .retryable
                .delay
                .is_some_and(|d| d > Duration::from_secs(300) || d.subsec_nanos() % 1_000_000 != 0)
            {
                return Err(
                    "retryable delay must use millisecond precision and not exceed 5 minutes"
                        .into(),
                );
            }
        }
        Ok(())
    }

    pub fn map(&self, status: u16, failure: &Value) -> Option<RetryableError> {
        if !self.enabled || failure.get("type")?.as_str()? != "response.failed" {
            return None;
        }
        let error = failure.pointer("/response/error")?.as_object()?;
        // A mapped event must not be rewritten a second time by a broader rule.
        if error.get("code").and_then(Value::as_str) == Some(RETRYABLE_CODE) {
            return None;
        }
        self.rules
            .iter()
            .find(|rule| {
                let m = &rule.matcher;
                (m.codes.is_empty()
                    || error
                        .get("code")
                        .and_then(Value::as_str)
                        .is_some_and(|s| m.codes.iter().any(|c| c == s)))
                    && (m.types.is_empty()
                        || error
                            .get("type")
                            .and_then(Value::as_str)
                            .is_some_and(|s| m.types.iter().any(|t| t == s)))
                    && (m.http_statuses.is_empty() || m.http_statuses.contains(&status))
                    && m.message_contains.as_ref().is_none_or(|needle| {
                        error
                            .get("message")
                            .and_then(Value::as_str)
                            .is_some_and(|s| s.contains(needle))
                    })
            })
            .map(|rule| rule.retryable.clone())
    }
}

impl RetryableError {
    pub fn rewrite(&self, mut failure: Value) -> Value {
        // Replace the original classification entirely. Keeping a quota/policy
        // code or details would route Codex to another ApiError variant.
        let mut detail =
            json!({"type": "server_error", "code": RETRYABLE_CODE, "message": self.message});
        if let Some(delay) = self.delay {
            detail["retry_after_ms"] = json!(delay.as_millis() as u64);
        }
        failure["response"]["error"] = detail;
        failure["response"]["status"] = json!("failed");
        failure
    }
}
