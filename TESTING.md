# Verification record — cpa-stg 0.2.0

Verified on Linux x86_64 on 2026-09-21. The tests use real native plugin libraries,
an unmodified CPA server, and a deterministic local HTTP upstream. All API keys
are synthetic test values. No production model credentials were used.
Toolchain: Rust 1.98.1, Go 1.26.0, Python 3.12.14.

## Source versions

| Component | Revision |
| --- | --- |
| CPA | `csbxd/CLIProxyAPI@61fdfc341b96178a8dcb53f2efc46cbc341d267c` |
| Official Codex API library | `openai/codex@f07aaf920b14d7a746e435add34b8bbd37da5da6` |
| Native components | `cpa-stg` and `cpa-stg-router`, version `0.2.0` |

Both reference checkouts had clean Git working trees after verification. Neither
CPA nor Codex was patched. The API probe preserves the official dependency lock
and the dependency overrides already present in Codex's workspace manifest.

## Checks

- `cargo fmt --all --check`: passed.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `cargo test --workspace`: 22 tests passed (18 admission/lifecycle, 4 mapping/SSE).
- `cargo build --workspace --release`: both native libraries built.
- Native C ABI smoke: registration metadata, 16 concurrent callers, allocation
  release, explicit rejection, invalid/oversized RPC handling, and shutdown passed.
- Real CPA integration: 17 checks; machine-readable evidence is in
  [test-results/e2e.json](test-results/e2e.json).

| Real-host coverage | Assertion |
| --- | --- |
| Startup HTTP 401 / 429 / 503 | Configured message and 1500 ms extension delivered as `response.failed` |
| Terminal context / quota errors | CPA-exposed codes matched; absent delay preserved |
| Terminal plain-message rule | Configured message and explicit zero delay emitted |
| Successful/unmatched requests | Normal completion and unmatched HTTP 400 retained |
| Endpoint scope | Chat Completions and non-streaming Responses errors unchanged |
| Eight parallel requests | Upstream concurrency never exceeds one; exactly eight upstream executions |
| RPM 300, burst one | Four upstream starts separated by at least 180 ms |
| Queue overflow/timeout | Rejected requests never reach upstream; later requests recover |
| Separate channels | Channel B remains available while A is held and queued |
| Active cancellation | Credential slot released while upstream stream is held |
| Queued cancellation | Queue capacity reclaimed; abandoned request never executes upstream |
| Hot configuration reload | Increasing concurrency wakes waiter while first stream remains active |
| CPA retry | Retry of the same credential reacquires admission and spends another rate token |
| Held-stream shutdown | Both plugins unload without a crash after CPA's HTTP drain deadline |
| Responses WebSocket | Mapped failure delivered as a WebSocket Responses event |
| Official Codex API | Four actual `ApiError::Retryable` variants and normal completion asserted |

Requests include a spoofed internal parent header; the upstream checks that the
plugin removes it. Nested admission trusts the parent link only for CPA-generated
host-callback metadata. Unit tests also cover canceled parents and late callbacks.

## Codex client evidence

The official library probe's captured output is in
[test-results/codex-sdk.stdout](test-results/codex-sdk.stdout). It checks the actual
Rust enum, not just a JSON string:

```text
verified ApiError::Retryable: http503: configured startup
verified ApiError::Retryable: context: configured terminal
verified ApiError::Retryable: quota: configured terminal
verified ApiError::Retryable: streamboom: configured text
verified normal Codex response completion
```

The probe uses the standard Codex User-Agent and local test authentication. It
asserts that the current client still returns `delay: None`, even when the plugin
emits `retry_after_ms`. Client-side delay support was explicitly outside the task.

## Reproduction and boundaries

Follow the build and test commands in [README.md](README.md). To include the
official client check, build `scripts/build_codex_probe.py` against the exact
Codex revision above and pass the resulting binary through `--codex-probe`.

The complete Codex CLI could not launch in this environment: its sandbox setup
failed with a NETLINK_ROUTE permission error before any API request. No sandbox
bypass was used. The API-only probe exercises the official Responses parser and
error classification; it does not test the CLI agent's automatic retry loop.

The held-stream shutdown deliberately keeps an HTTP response open. CPA waits for
its 30-second HTTP drain deadline before unloading plugins and logs the expected
HTTP shutdown deadline error; this is host behavior, not a plugin unload hang.

The local run does not establish production-provider behavior, multi-process
quota coordination, or macOS/Windows compatibility. Cross-platform build/ABI
checks and the Linux CPA suite are configured in GitHub Actions but have not yet
run on GitHub for this release. CLIProxyAPIHome mode does not support the router.

Reference CPA may perform its own background metadata refreshes despite local
model mode. All model execution traffic in these tests targets the local mock.
