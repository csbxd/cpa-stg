use cpa_stg::config::Config;
use cpa_stg::error_mapping::{failure_from_message, sse, SseMapper};
use serde_json::json;
use std::time::Duration;

fn config() -> Config {
    Config::parse(
        br#"
error_mapping:
  enabled: true
  rules:
    - name: specific
      match:
        codes: [context_length_exceeded, insufficient_quota]
        message_contains: retry
      retryable: {message: configured, delay: 1500ms}
    - name: status
      match: {http_statuses: [429, 503]}
      retryable: {message: busy}
"#,
    )
    .unwrap()
}
#[test]
fn ordered_matching_requires_all_fields_and_preserves_optional_delay() {
    let cfg = config();
    let failure = failure_from_message(
        r#"{"error":{"code":"context_length_exceeded","message":"retry this"}}"#,
    );
    let mapped = cfg.error_mapping.map(429, &failure).unwrap();
    assert_eq!(mapped.message, "configured");
    assert_eq!(mapped.delay, Some(Duration::from_millis(1500)));
    let result = mapped.rewrite(failure);
    assert_eq!(result["response"]["error"]["code"], "cpa_retryable");
    assert_eq!(result["response"]["error"]["retry_after_ms"], 1500);
    assert!(cfg.error_mapping.map(429, &result).is_none());
    let generic = failure_from_message("retry this");
    assert!(cfg.error_mapping.map(0, &generic).is_none());
    let mapped = cfg.error_mapping.map(503, &generic).unwrap();
    assert_eq!(mapped.delay, None);
    assert!(mapped.rewrite(generic)["response"]["error"]
        .get("retry_after_ms")
        .is_none());
}
#[test]
fn sse_fragmentation_unmatched_frames_and_bounded_buffer() {
    let failure =
        failure_from_message(r#"{"error":{"code":"insufficient_quota","message":"retry now"}}"#);
    let wire = [
        b":keepalive\r\n\r\n".to_vec(),
        sse(&failure),
        b"data: [DONE]\n\n".to_vec(),
    ]
    .concat();
    let mut mapper = SseMapper::new(config().error_mapping);
    let mut output = Vec::new();
    for byte in wire {
        output.extend(mapper.push(&[byte]));
    }
    output.extend(mapper.finish());
    assert!(output.starts_with(b":keepalive\r\n\r\n"));
    assert!(output.ends_with(b"data: [DONE]\n\n"));
    assert!(String::from_utf8(output).unwrap().contains("cpa_retryable"));
    let huge = vec![b'x'; 1024 * 1024 + 1];
    assert_eq!(mapper.push(&huge), huge);
    assert_eq!(mapper.push(b"tail"), b"tail");
}
#[test]
fn invalid_rules_rejected_and_zero_delay_retained() {
    for setting in ["-1s", "1.5s", "301s", "100", "1h"] {
        let yaml = format!("error_mapping:\n  rules:\n    - name: a\n      match: {{codes: [x]}}\n      retryable: {{message: m, delay: {setting}}}");
        assert!(Config::parse(yaml.as_bytes()).is_err(), "{setting}");
    }
    let cfg = Config::parse(b"error_mapping:\n  enabled: true\n  rules:\n    - name: zero\n      match: {types: [server_error]}\n      retryable: {message: m, delay: 0ms}").unwrap();
    let event = failure_from_message(r#"{"error":{"type":"server_error"}}"#);
    assert_eq!(
        cfg.error_mapping.map(0, &event).unwrap().delay,
        Some(Duration::ZERO)
    );
    assert!(Config::parse(b"error_mapping: {enabled: true}").is_err());
    assert!(Config::parse(
        b"error_mapping:\n  rules:\n    - name: a\n      match: {}\n      retryable: {message: x}"
    )
    .is_err());
    assert!(cfg
        .error_mapping
        .map(503, &json!({"type":"response.completed"}))
        .is_none());
}

#[test]
fn codex_host_records_are_reframed_without_waiting_for_stream_end() {
    let mut mapper = SseMapper::new(config().error_mapping);
    assert!(mapper
        .push_host_chunk(b"event: response.created")
        .is_empty());
    let created =
        mapper.push_host_chunk(br#"data: {"type":"response.created","response":{"id":"r"}}"#);
    assert!(created.ends_with(b"\n\n"));
    assert!(String::from_utf8(created)
        .unwrap()
        .contains("response.created\ndata:"));
    assert!(mapper.push_host_chunk(b"event: response.failed").is_empty());
    let failed = mapper.push_host_chunk(br#"data: {"type":"response.failed","response":{"error":{"code":"insufficient_quota","message":"retry now"}}}"#);
    assert!(String::from_utf8(failed).unwrap().contains("cpa_retryable"));
    assert!(mapper.finish().is_empty());
}
