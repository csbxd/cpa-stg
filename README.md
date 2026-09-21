# CPA STG

An independent Rust plugin project for CLIProxyAPI. It applies token-bucket
rate limiting, concurrency limits, and bounded FIFO waiting queues per credential,
and configurable error mapping for Codex Responses streams.
API Key policy configuration is reserved for a later phase and rejects non-empty
rules instead of silently ignoring them.

[中文说明](README_CN.md)

## Compatibility

The integration contract was checked against
[`csbxd/CLIProxyAPI` at `61fdfc3`](https://github.com/csbxd/CLIProxyAPI/tree/61fdfc341b96178a8dcb53f2efc46cbc341d267c).
It requires native ABI 1, RPC schema 2+, `request_interceptor`, and
`request_lifecycle_plugin`, plus after-auth metadata `selected_auth_id`.
Use a build containing those interfaces; support for other CPA distributions is
not assumed. The host must deliver `request.complete` on every terminal path,
including cancellation while a native interceptor is waiting.

## Build and install

Install the current stable Rust toolchain and a platform C linker, then run:

```sh
cargo test --workspace --locked
cargo build --workspace --release --locked
python3 scripts/package.py
```

The package script creates `dist/cpa-stg_<version>_<os>_<arch>.zip` and its SHA-256 file.
Unzip it **inside CPA's configured plugins directory** (normally `plugins/`). The
archive contains **two components from this one project**, for example on Linux:

```text
plugins/linux/amd64/cpa-stg.so
plugins/linux/amd64/cpa-stg-router.so
```

`cpa-stg` owns credential admission; `cpa-stg-router` wraps streaming
`openai-response` execution using `model.route`, `executor.execute_stream`, and
`host.model.*`. CPA skips the calling plugin during nested execution; separate
plugin IDs ensure the admission plugin still runs. Both components are compiled
from the same Rust source with isolated native state. macOS uses `.dylib`, Windows
uses `.dll`. The limiter also works alone if error mapping is not needed.

Merge [config.example.yaml](config.example.yaml) into CPA's configuration and
restart CPA or use its plugin reload flow. The filename determines the plugin ID:
rename Rust's `libcpa_stg.so` to `cpa-stg.so`, as the package script does.
Do not overwrite a loaded dynamic library; drain requests before replacement.

## Policy configuration

Each `credentials.overrides` key matches one exact host credential ID. Labels,
model names, client API Keys, and upstream token values are not identifiers here.
Unlisted credentials use `credentials.default`. Overrides are complete policies;
omitted fields use the built-in defaults shown below, not the configured default.

| Field | Default | Meaning |
| --- | ---: | --- |
| `enabled` | `true` | Enable this credential's admission policy |
| `requests_per_minute` | `60` | Token refill rate; `0` disables rate limiting |
| `burst` | `1` | Token capacity and initial burst; must be positive |
| `max_concurrency` | `2` | Active upstream attempts; `0` disables this limit |
| `max_queue` | `100` | Waiting requests, excluding active attempts; `0` rejects immediately when busy |
| `queue_timeout_ms` | `30000` | Maximum admission wait; valid range 1–300000 ms |

The token bucket is **not a strict rolling-minute cap**: it refills continuously
at `requests_per_minute / 60` tokens per second and allows up to `burst` initially.
Tokens are spent when an attempt is admitted, never when it merely queues. Tokens
are not refunded on completion, upstream failure, or cancellation after admission.

`max_tracked_requests` and `max_credentials` default to 10000 and bound process
memory. Idle credential buckets may be removed only after fully refilling so that
eviction cannot reset rate debt. Capacity exhaustion returns HTTP 503.

## Runtime behavior

- `request.intercept_before` registers the execution's `RequestID`.
- `request.intercept_after` acquires a slot for `Metadata.selected_auth_id`. The
  mutex is released during condition-variable waits. Credentials have separate
  buckets and FIFO queues; new requests cannot jump existing waiters.
- Every after-auth invocation is a new upstream attempt. A retry releases the
  previous concurrency slot and consumes a fresh rate token, even for the same
  credential. Changing credentials moves accounting to the new credential.
- `request.complete` releases the active slot or removes the queued request.
  Duplicate terminal events are harmless. A late after-auth call cannot recreate
  an execution that already completed. The admission plugin associates nested
  executions with their outer request using a host-only parent link. Canceling the
  outer request removes nested waiters immediately, preventing ghost upstream
  execution after client disconnect. The link is stripped before upstream calls.
  For streams, the slot lasts until the
  host's terminal event, not until the first chunk.
- Live reconfiguration preserves active counts, rate debt, and queue order.
  Existing waiters keep their original deadlines. Lower limits drain naturally;
  new capacity and disabled credential policies wake waiters. Invalid updates
  leave the previous configuration intact.
- `plugin.quiesce` and shutdown reject new work and wake queued calls. No timeout
  is imposed on an admitted upstream request or stream.

Queue overflow and timeout produce HTTP 429 with `Retry-After: 1` (a retry hint,
not a promised admission time) and distinct `cpa_queue_full` / `cpa_queue_timeout`
codes. Credential-free outer plugin routes do not consume quota; malformed
credential IDs return 503 and malformed interception payloads return 400. Policy rejections are explicit successful-RPC termination responses,
because CPA logs ordinary interceptor RPC errors and continues execution.

## Codex error mapping

Configure rules under `plugins.configs.cpa-stg-router.error_mapping`; see the
complete [example](config.example.yaml). Mapping applies only to **streaming
Responses requests** (`openai-response`, including downstream Responses WebSocket).
Chat Completions, Claude/Gemini endpoints, non-streaming Responses and compact
requests retain their existing response/error handling. Credential admission
continues to apply to their selected credentials.

Rules are ordered; the first match wins. Nonempty fields within `match` are ANDed,
values inside each list are ORed. Matching is case-sensitive:

| Field | Matches |
| --- | --- |
| `codes` | Error code available from CPA |
| `types` | Error type available from CPA |
| `http_statuses` | Startup callback HTTP status (400–599) |
| `message_contains` | Literal substring of the available error message |
| `retryable.message` | Required replacement message |
| `retryable.delay` | Optional integer duration, e.g. `1500ms`, `2s`, `1m`; max 5 minutes |

Omitted/null delay is `None`; `0ms` is `Some(Duration::ZERO)`. Mapped errors are
returned as SSE `response.failed` with `error.code=cpa_retryable`,
`error.type=server_error`, and the configured message. A supplied delay is emitted
as `retry_after_ms`; current Codex ignores this extension and reads `delay=None`.
Neither CPA nor Codex needs a patch. The plugin does not sleep for this delay.
The verified client sends Codex's `codex_cli_rs/...` User-Agent, which selects
CPA's Codex Responses framing; custom API clients should preserve that header.

CPA can normalize upstream codes (for example `context_length_exceeded` becomes
`context_too_large`). Configure the codes exposed by CPA. Stream-read failures
carry only a string: JSON error details are recovered when present, otherwise only
message matching is possible. Unknown HTTP status is never guessed for matching.
Headers/raw error fields discarded by CPA cannot be reconstructed. Unmatched
startup errors preserve their status/message; other host error metadata may
already have been lost by the callback API.

The wrapper retains CPA scheduling, credential failover and retry behavior. It
maps the failure that CPA finally exposes, not every failed internal attempt.
Each live stream snapshots its rules so a reload does not change its meaning
halfway through. Valid host line records are reframed; normal SSE payloads are
preserved. Incomplete SSE events are buffered up to 1 MiB; oversized events switch
payload rewriting to passthrough for that stream (terminal callback errors can
still be mapped). Workers close host streams on completion/cancellation/shutdown.

**CLIProxyAPIHome mode is incompatible with this router:** the checked CPA version
explicitly rejects plugin executor routes while Home is enabled. API-key policies,
distributed quota and arbitrary raw upstream interception are outside this release.

## Scope and limitations

This release is an **in-process** limiter. Multiple CPA processes maintain
independent budgets; distributed limits require a shared backend in a later phase.
State resets on process restart or plugin unload. Cancellation cleanup depends
on the host terminal callback; on older hosts that do not deliver cancellation
during a blocked native call, queued cleanup is bounded by `queue_timeout_ms`.
No TTL reclaims active streams, because doing so could over-admit live traffic.

Admission happens after CPA chooses a credential. It waits on that credential
and does not reroute a busy request to another channel. The limit counts host
after-auth attempts; hidden retries inside an executor and paths that skip those
hooks cannot be counted by this plugin. One `RequestID` must execute attempts
sequentially, as the referenced CPA conductor does. RPC inputs are
limited to 64 MiB, including base64 encoding of model payloads.

No API secret or request body is stored in engine state or logged. The phase-two
API Key integration should use an opaque host principal, with a second scope in
the engine and transactional multi-scope admission so one policy cannot consume
capacity while another rejects it. It is intentionally not implemented yet.

## Verification

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --release --locked
python3 scripts/abi_smoke.py target/release/libcpa_stg.so
```

Use `.dylib` on macOS and `cpa_stg.dll` on Windows. Tests exercise deterministic
clock transitions, FIFO admission, independent credentials, retries, queue bounds,
timeouts, cancellation, reconfiguration, shutdown, and Go's JSON/base64 wire shape.
The smoke test loads the real dynamic library through the C ABI, checks memory
release and multithreaded admission, and never contacts an upstream model.

CI is configured to run checks, builds, native ABI smoke tests, and artifact packaging on Linux,
macOS, and Windows. Every run uploads its native build artifacts. The Linux
end-to-end job downloads and tests those exact `.so` files without rebuilding.

To release, push a tag matching `Cargo.toml` (for example `v0.2.0`), or run the
CI workflow on the default branch with `publish` enabled. Only after all build
and test jobs pass will CI create the release and attach the platform ZIPs,
standalone Linux `.so` files, SHA-256 checksums, build provenance, and current
end-to-end evidence. Existing releases are never overwritten. A normal branch
push only builds and tests.
The end-to-end suite builds on an **unmodified CPA executable**, loads both real
native libraries, and uses a local synthetic Codex upstream (no live credentials).
It verifies HTTP failures, in-stream failures, untouched endpoints, concurrency,
rate refill, queue bounds/timeouts, cancellation, credential isolation, hot reload,
CPA retries, and Responses WebSocket delivery:

```sh
python3 scripts/e2e.py --cpa /path/to/cli-proxy-api
```

The optional API-only probe uses the **unmodified official `codex-api` crate** at
`f07aaf920b14d7a746e435add34b8bbd37da5da6` to assert the actual
`ApiError::Retryable { message, delay }` variant and normal response completion.
It runs no agent tools and uses only the local test key. Build with a recent Rust
toolchain and Codex's native build prerequisites (OpenSSL development libraries):

```sh
python3 scripts/build_codex_probe.py --codex-source /path/to/pinned/codex-checkout
python3 scripts/e2e.py --cpa /path/to/cli-proxy-api \
  --codex-probe target/codex-sdk-probe/target/debug/cpa-codex-sdk-probe
```

Evidence is written to `target/e2e/report.json` and per-scenario logs. `--filter`
selects a named check while diagnosing a failure. See [TESTING.md](TESTING.md) for
recorded results and coverage limits.
