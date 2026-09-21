## CPA STG 0.2.0

- Per-credential token bucket rate limits, concurrency limits, FIFO queues,
  cancellation cleanup, and hot reload.
- A separate router maps configured failures to retryable SSE errors for
  streaming Codex Responses requests. Other endpoints retain their behavior.
- No changes to CPA or Codex are required. The reference CPA revision is
  `csbxd/CLIProxyAPI@61fdfc341b96178a8dcb53f2efc46cbc341d267c`.

All native assets are built by GitHub Actions. Publication requires Rust format,
Clippy, unit tests, native ABI smoke tests on Linux/macOS/Windows, and the Linux
CPA end-to-end suite using the exact Linux libraries in these release assets.
The optional official Codex SDK probe is not part of this CI gate.

Use the platform ZIP for installation: extract it inside CPA's configured
plugins directory. Standalone Linux `.so` assets are also included; rename them
to `cpa-stg.so` and `cpa-stg-router.so` under `plugins/linux/amd64/`.
The Linux build uses Ubuntu 24.04 and dynamically links the system C runtime.
Check `checksums.txt` for SHA-256 hashes and `build-info.json` for provenance.

Limits are per process; CLIProxyAPIHome mode does not support the router.
See README and TESTING for configuration, supported hooks, and coverage limits.
