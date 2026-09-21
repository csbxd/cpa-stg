# CPA STG

独立的 Rust CPA 插件。实现**凭证（渠道）维度的限流与等待排队**，以及 **Codex Responses 流式错误映射**。
API Key 维度预留 `api_keys` 配置入口，当前只接受空配置，非空会明确报错。

## 接入版本

按 [csbxd/CLIProxyAPI 的 61fdfc3 版本](https://github.com/csbxd/CLIProxyAPI/tree/61fdfc341b96178a8dcb53f2efc46cbc341d267c)
核对接口：原生 C ABI 1、RPC schema ≥ 2、请求拦截及终态通知，且选凭证后的
`Metadata.selected_auth_id` 必须存在。未宣称所有 CPA 分支均兼容。

## 功能与规则

- 每个凭证独立令牌桶、并发计数和 FIFO 队列，忙碌凭证不会占用其他凭证的额度。
- 令牌桶按配置的 RPM 连续补充，支持突发容量；**不是严格的滚动 60 秒请求数上限**。
- 请求等到令牌和并发名额都可用才放行。排队期间释放锁，新请求不能插队。
- 队列满或等待超时返回 HTTP 429，附带 `Retry-After: 1` 和明确的错误码。
- 每次重试重新计费；换凭证时释放旧凭证的并发名额，并进入新凭证的限流流程。
- 非流式请求结束、流式请求终止、失败或取消时，通过 `request.complete` 释放名额。
- 热更新保留已占用名额、剩余令牌和队列顺序；已入队请求保留原等待截止时间。
- `plugin.quiesce` / shutdown 唤醒等待中的请求并停止新请求进入。

## 构建与安装

需要当前 stable Rust 和平台 C 链接器。在项目根目录执行：

```sh
cargo test --workspace --locked
cargo build --workspace --release --locked
python3 scripts/package.py
```

将 `dist/cpa-stg_<version>_<os>_<arch>.zip` 解压到 CPA 的插件目录（通常为 `plugins/`）。
例如 Linux x86_64 会得到 `plugins/linux/amd64/cpa-stg.so` 和
`plugins/linux/amd64/cpa-stg-router.so`，两个文件都需要安装。
其他平台对应 `darwin/arm64/cpa-stg.dylib` 或 `windows/amd64/cpa-stg.dll`。
文件名必须保持 `cpa-stg` 和 `cpa-stg-router`，CPA 用文件名识别插件 ID。
同一项目编译两个组件：前者负责限流，后者通过宿主模型回调包装执行并映射错误。
CPA 的嵌套调用会跳过调用插件，拆成两个 ID 后仍会执行限流组件，不需要修改 CPA。

将 [config.example.yaml](config.example.yaml) 合并进 CPA 配置，然后重启或加载插件。
替换动态库前先排空请求，不要覆盖正在加载的库文件。

## 配置

`credentials.default` 应用于未单独配置的凭证。`credentials.overrides` 以实际
`selected_auth_id` 为键，不能填 API 密钥、显示名或模型名。每个 override 是完整策略：
缺失字段采用下表中的内置默认值，**不会继承自定义 default**。

| 字段 | 默认值 | 含义 |
| --- | ---: | --- |
| `enabled` | `true` | 是否限制该凭证 |
| `requests_per_minute` | `60` | 每分钟令牌补充量，`0` 关闭频率限制 |
| `burst` | `1` | 初始令牌数和令牌桶容量，必须大于 0 |
| `max_concurrency` | `2` | 同时执行的请求上限，`0` 关闭并发限制 |
| `max_queue` | `100` | 排队请求上限，`0` 表示忙碌时直接拒绝 |
| `queue_timeout_ms` | `30000` | 仅限制等待准入时间，范围 1–300000 毫秒 |

令牌在放行时扣减，不在入队时扣减；请求结束、上游错误或放行后取消均不返还令牌。
`max_tracked_requests`、`max_credentials` 默认各 10000，用于限制状态内存；超限返回 503。
没有选凭证的外层插件包装请求不计费；凭证 ID 存在但格式错误时返回 503。拒绝使用显式 Terminate 响应，避免触发 CPA
对普通拦截器 RPC 错误的继续执行行为。`Retry-After: 1` 是重试提示，不保证一秒后可放行。

## 范围与限制

首期为单 CPA 进程内策略，不含 Redis、多实例共享配额、TPM、预算和管理 UI。
插件在选定凭证后等待，不会主动切到空闲渠道。重启或卸载插件会丢失内存状态。
取消清理由宿主的终态事件触发；旧宿主若无法在原生插件阻塞期间发出取消通知，
等待项最迟在排队超时后清理。已开始的流式请求不设置 TTL，避免错误释放名额。

只统计经过 after-auth 的上游执行尝试，执行器内部隐藏重试或绕过该 hook 的路径
不在统计范围。一个 RequestID 的尝试应按参考 CPA 的行为顺序执行。
通过宿主生成的元数据和内部父请求关联，取消外层请求会同步清理嵌套等待项，
避免断开连接后又向上游发起请求；关联头在真正访问上游前移除。RPC 输入最大 64 MiB（含请求体的 Base64 编码）。

## Codex 错误映射

配置在 `plugins.configs.cpa-stg-router.error_mapping`，完整示例见
[config.example.yaml](config.example.yaml)。仅处理 `openai-response` 流式请求，
包括 Responses 的 HTTP SSE 和 WebSocket；Chat Completions、Claude/Gemini、
非流式 Responses 和 compact 不做错误映射，凭证限流仍照常生效。

规则按顺序匹配，首条命中生效。`match` 中的不同字段为 AND，列表内部为 OR，
匹配区分大小写：

| 字段 | 含义 |
| --- | --- |
| `codes` / `types` | CPA 实际暴露的错误码 / 类型 |
| `http_statuses` | 流启动失败时的 HTTP 状态码 |
| `message_contains` | 错误消息包含的字面文本 |
| `retryable.message` | 替换后的消息，必填 |
| `retryable.delay` | 可选，如 `1500ms`、`2s`、`1m`，上限 5 分钟 |

省略或 `null` 表示 `None`，`0ms` 表示 `Some(Duration::ZERO)`。
命中规则后输出 `response.failed`，错误码为 `cpa_retryable`、类型为 `server_error`，
使 Codex 进入 `ApiError::Retryable` 分支。延迟写入 `retry_after_ms`；当前 Codex
不读取它，客户端的 `delay` 仍为 `None`。按需求不修改 Codex，也不在服务端休眠。
验证使用 Codex 的 `codex_cli_rs/...` User-Agent，以启用 CPA 的 Codex Responses
事件格式；自定义客户端应保留该请求头。

需注意 CPA 的真实接口边界：

- CPA 可能改写原始错误码，例如 `context_length_exceeded` 变成 `context_too_large`。
- 流中错误回调只有字符串。能解析出 JSON 时匹配其中的码和类型，否则只能匹配消息；
  流中错误不能按已经丢失的 HTTP 状态码匹配。
- 保留 CPA 原有的选凭证、切换和重试，映射的是 CPA 最后返回的错误。
- 每个流使用启动时的规则快照，热更新不改变已开始流的规则。
- SSE 支持拆分、合并和 CPA 无换行的独立记录。不完整事件缓存上限 1 MiB；超限后
  该流的数据事件透传，终态错误回调仍能映射。
- **CLIProxyAPIHome 模式不支持错误映射组件**：参考 CPA 明确禁止该模式下的插件执行器路由。

## 验证

除 Rust 测试和真实动态库 ABI 测试外，项目包含真实 CPA 端到端测试，使用本地可控上游，
不需要生产凭证：

```sh
python3 scripts/e2e.py --cpa /path/to/cli-proxy-api
```

覆盖错误映射、端点隔离、并发、RPM、排队满/超时、跨渠道隔离、取消清理、热更新、
CPA 重试和 WebSocket。还提供基于未修改的官方 `codex-api` 库的客户端探针，
直接断言实际 `ApiError::Retryable` 枚举；构建步骤见 [README.md](README.md)。
测试结果见 [TESTING.md](TESTING.md)。

## CI 构建与发布

普通 push 会在 Linux、macOS、Windows 上构建、测试并上传产物。
Linux 端到端测试直接加载 CI 产物中的两个 `.so`，不重复编译插件。
推送与 `Cargo.toml` 版本一致的标签（例如 `v0.2.0`），或在默认分支手动运行
CI 并勾选 `publish`，所有检查通过后才会发布 GitHub Release。
附件包括各平台 ZIP、独立 Linux `.so`、SHA-256 校验和、构建来源和本次端到端报告。
现有 Release 不会被覆盖；官方 Codex SDK 探针仍为可选测试，不属于此次 CI 发布门禁。
