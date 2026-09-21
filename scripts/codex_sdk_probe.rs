//! Uses the unmodified official codex-api crate. This is an API client, not an
//! agent: it never executes model-generated tools or reads user authentication.
use codex_api::{
    ApiError, AuthProvider, Compression, Provider, ReqwestTransport, ResponseEvent,
    ResponsesClient, RetryConfig,
};
use futures::StreamExt;
use http::{HeaderMap, HeaderValue};
use serde_json::json;
use std::{sync::Arc, time::Duration};
struct LocalAuth;
impl AuthProvider for LocalAuth {
    fn add_auth_headers(&self, headers: &mut HeaderMap) {
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer e2e-frontend-key"),
        );
    }
}
#[tokio::main]
async fn main() {
    let url = std::env::args().nth(1).expect("local CPA base URL");
    assert!(url.starts_with("http://127.0.0.1:"));
    let mut headers = HeaderMap::new();
    headers.insert(
        "user-agent",
        HeaderValue::from_static("codex_cli_rs/0.154.0"),
    );
    let provider = Provider {
        name: "stg-e2e".into(),
        base_url: url,
        query_params: None,
        headers,
        retry: RetryConfig {
            max_attempts: 1,
            base_delay: Duration::ZERO,
            retry_429: false,
            retry_5xx: false,
            retry_transport: false,
        },
        stream_idle_timeout: Duration::from_secs(5),
    };
    let client = ResponsesClient::new(
        ReqwestTransport::new(reqwest::Client::new()),
        provider,
        Arc::new(LocalAuth),
    );
    for (case, expected) in [
        ("http503", "configured startup"),
        ("context", "configured terminal"),
        ("quota", "configured terminal"),
        ("streamboom", "configured text"),
    ] {
        let mut stream = client
            .stream(
                json!({"model":"a/test-model","stream":true,"input":case}),
                HeaderMap::new(),
                Compression::None,
                None,
            )
            .await
            .expect("CPA HTTP stream");
        let mut verified = false;
        while let Some(event) = stream.next().await {
            match event {
                Err(ApiError::Retryable { message, delay }) => {
                    assert_eq!(message, expected);
                    assert_eq!(delay, None); // Current Codex ignores retry_after_ms.
                    verified = true;
                    println!("verified ApiError::Retryable: {case}: {message}");
                    break;
                }
                Err(other) => panic!("unexpected client classification for {case}: {other:?}"),
                Ok(_) => {}
            }
        }
        assert!(verified, "no Retryable error for {case}");
    }
    let mut stream = client
        .stream(
            json!({"model":"a/test-model","stream":true,"input":"success"}),
            HeaderMap::new(),
            Compression::None,
            None,
        )
        .await
        .unwrap();
    let mut completed = false;
    while let Some(event) = stream.next().await {
        if matches!(
            event.expect("successful stream"),
            ResponseEvent::Completed { .. }
        ) {
            completed = true;
        }
    }
    assert!(completed);
    println!("verified normal Codex response completion");
}
