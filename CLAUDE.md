# CLAUDE.md

This file provides guidance to AI coding agents when working with code in this repository.

> 最后校准：2026-08-07（基于代码实际状态重写，替代早期"纯无状态网关"的过时描述）

## 项目定位

**Silievo 多协议 LLM 网关中转平台**（artifactId `silievo-api-gateway-service`，与老项目同名）。
接收 OpenAI / Anthropic / Responses / Vertex(Gemini) 协议请求，统一转换为内部标准模型后路由到多家上游渠道，并完成 **API Key 认证、余额预占/结算、8 种计费模式、双层请求日志、用户折扣/套餐** 等平台能力。老项目参考：`RuoYi-Cloud/ruoyi-silievo/silievo-api-gateway`。

## Build & Run

```bash
mvn clean compile                  # 编译
mvn clean package -DskipTests      # 打包
java -jar target/silievo-api-gateway-service-1.0-SNAPSHOT.jar   # 运行（默认 dev 环境，端口 3003）
```

**环境选择（配置文件按 profile 区分）：**
- `application.yml` —— 公共配置（OSS/连接池/默认激活 dev）。**本文件 gitignore、含真实密钥，不推送 GitHub**；仓库上传的是模板 `application.yml.example`
- `application-dev.yml` —— 开发（本地 MySQL `pt_carpool` + Redis localhost，端口 3003，默认激活）。**同样 gitignore**，模板为 `application-dev.yml.example`

**IDEA 里选择环境**：Maven 工具窗口 → Profiles → 勾选 `dev`，再运行 `spring-boot:run`（pom.xml 的 Maven profiles 会把所选环境作为 `spring.profiles.active` 传给应用；dev 默认激活）。

```bash
java -jar target/silievo-api-gateway-service-1.0-SNAPSHOT.jar   # 开发环境（默认激活 dev，端口 3003）
```

**密钥与配置**：真实密钥不推送 GitHub。如何从 `.example` 模板生成本地配置文件、环境变量注入方式，见 `README.md`「配置与密钥」一节。Log4j2 持久日志在 `logs/gateway.log`。

- **必须由用户在终端前台运行 jar**：本机 360 安全卫士会杀后台启动的 java 进程
- 重启前先 `Get-Process java` 杀干净旧进程（dev 3003 端口占用）
- 依赖本机 MySQL（库 `pt_carpool`，R2DBC）+ Redis（localhost:6379）

## 技术栈

| 维度 | 选型 |
|------|------|
| 框架 | Spring Boot 3.3.4 + WebFlux（全链路响应式非阻塞，禁止 `.block()`） |
| 语言 | Java 21 |
| 数据库 | MySQL via R2DBC（`io.asyncer:r2dbc-mysql`） |
| 缓存 | Redis Reactive（余额预占/扣减用 Lua/原子操作，分布式锁） |
| 日志 | Log4j2（排除 Logback），`@Log4j2` + `LogBox` 结构化日志块；micrometer context-propagation 桥接 requestId 到 MDC |
| JSON | Jackson，全局 `default-property-inclusion: non_null` |
| 其他 | Lombok、阿里云 OSS SDK（生图 base64→URL） |

## 架构：协议层 ⊥ 渠道层

```
Client → Controller（协议入口）
  → ProtocolManager.bindProtocol()（ProtocolType 写入 exchange attributes）
  → ProtocolConverter.toInternalRequest()（外部 DTO → LlmChatRequest）
  → LlmGateway.chat()/chatStream()/executeVideo()（路由 + 编排预占/结算）
    → ModelRouter/ChannelResolver → ProviderRegistry → ProviderAdapter（上游调用）
      → converter/upstream/*FormatConverter（内部模型 ↔ 上游格式）
  → ProtocolConverter.toExternalResponse()/toExternalStream()
→ JSON / SSE 响应
```

- 内部统一模型：`LlmChatRequest / LlmChatResponse / LlmStreamChunk`（dto 包），含 `requestType`、`tools/toolChoice/thinking/topP/topK/stopSequences`、`extraParams`（`@JsonAnySetter` 兜底捕获未建模字段，上游白名单过滤后透传）
- 渠道配置**全部来自数据库**（`proxy_channels` / `proxy_channel_tokens` / `proxy_channel_models`），`ProviderRegistry` 启动加载 + 支持动态 reload；每个 Adapter 持有渠道全部 Token（apiKeys/tokenIds），按秒级时间戳取模做负载均衡（方案 C，已实现）
- `ProviderFactory.createByAlias`：`openai_bearer / openai_azure / anthropic / vertex / openai_image / dashscope_image / gemini_image / dashscope_video / volcengine_video`

## HTTP 端点

| 端点 | Controller | 说明 |
|------|-----------|------|
| `/v1/chat/completions` | OpenAiCompatibleController | OpenAI Chat 协议 |
| `/v1/me[CLAUDE.md](CLAUDE.md)ssages` | AnthropicCompatibleController | Anthropic 协议（SSE 严格遵守官方事件序列） |
| `/v1/responses` | ResponsesCompatibleController | OpenAI Responses 协议（Codex 用） |
| Vertex 端点 | VertexCompatibleController | Gemini generateContent（流式按 URL 后缀 `:streamGenerateContent` 判定） |
| `/v1/models` | ModelController | 模型列表 |
| `/v1/images/generations`、`/v1/images/edits` | ImageController | 编辑仅 multipart（照老项目参数设计：`image`/`mask`/`prompt`/`model`） |
| `/v1/videos/generations`（POST/GET {taskId}） | VideoController | 异步任务，`VideoTaskPoller` 定时轮询 + 原子 claimBilling 防双结算 |
| `/v1/embeddings/text_embeddings`、`/multimodal_embeddings` | EmbeddingController | 向量（轻量透传） |

## 请求横切链路（filter/ + service/）

1. `RequestIdWebFilter` → `ApiKeyAuthWebFilter`（`proxy_tokens` 校验，`UserContext` 进 Reactor Context）→ `ModelPermissionWebFilter` → `RequestLoggingWebFilter`（`RequestLogging`/`ModelPermission` **都禁止预读 multipart body**，否则 `getMultipartData()` 报 "Could not find first boundary"）
2. 计费分层：`BillingService`（reserve/release/deduct/settle* 全生命周期）+ `BillingCalculator`（8 种计费模式）+ `SettlementService`（`proxy_logs` + `proxy_request_logs` 双层落库）+ `StreamUsageAccumulator`
3. 折扣：API Key markup / 套餐（PackageService，按完整模型 ID 匹配）/ 用户模型折扣（UserModelDiscountsService）三者取 min

## 硬性约定（违反必出 bug，都是踩过的坑）

### 计费
- `QUOTA_ROUNDING_MODE = RoundingMode.UP`，非零费用至少扣 1 额度，**禁止改**
- 价格未配置（0/null/负）不计费；渠道专属+全局都查不到 → `PRICE_NOT_CONFIGURED`(400) 拒绝
- 预占只锁额度不扣余额；`deductBalance` 必须归还**完整预占**（不只是实际金额）
- 扣减用原子相对更新：Redis `increment(key, -amount)` + DB `SET balance = balance - :amount`
- proxy_logs 模型 ID 用 `routing.getModelId()`（含渠道前缀），勿用 upstreamModel

### Reactor / WebFilter
- **禁止** `switchIfEmpty(chain.filter(...))`——整条链会执行两次（计费、上游调用重复）
- **禁止** `defaultIfEmpty(null)`——抛 NPE，用 `.map(Optional::ofNullable).defaultIfEmpty(Optional.empty())`
- Redis 锁必须显式释放（only-if-still-owner），不能只靠 TTL——生视频逐任务锁泄漏教训
- 全链路无 `.block()`；任何阻塞 SDK 调用包 `subscribeOn(boundedElastic)`

### 流式
- 流式判定**只看** body 的 `stream` 字段（或 Vertex URL 后缀），绝不靠 Accept 头
- OpenAI 兼容上游必须发 `stream_options.include_usage=true`；`[DONE]` 前无条件下发 usage 包
- **上游流错误禁止吞成空 chunk / 伪装成 deltaContent 文本**——必须 `Flux.error(e)` 向上传播，由 Converter/Controller 转成 SSE error 事件（否则错误文本被客户端存入历史，毒化后续对话）
- Anthropic SSE 必须完整：`message_start`(content:[]) → `content_block_start` → `content_block_delta` → `content_block_stop` → `message_delta` → `message_stop`

### 上游适配
- `WebClient.create()` 不跟随 302，下载图床 URL 用 `HttpClient.create().followRedirect(true)`
- DB 里的 endpointPath 可能带前导斜杠且含 `{model}` 占位符，必须 `.replace("{model}", model)`
- Gemini 严格校验字段：extraParams 走 `GEMINI_EXTRA_ALLOWED` 白名单；tool JSON Schema 走 `sanitizeJsonSchema` **深度递归**清洗（`$` 前缀键、const、propertyNames 等方言键，含 anyOf/oneOf/allOf 数组元素）
- Anthropic `tool_use.input` 必须是 JSON 对象不能是字符串；`tool` 角色消息转为 user 消息 + `tool_result` 块紧跟对应 tool_use
- DashScope 图像编辑：base64 必须先 `OssService.uploadBytes` 换 URL；生图/编辑同一个 multimodal-generation 接口
- `AbstractProviderAdapter` 统一 `maxInMemorySize(64MB)`；`maskForLog()` 递归脱敏 base64，新适配器**禁止**往日志打原始 base64
- SettlementService 落库前对 images/mask base64 置 null（`request_body` TEXT 只有 64KB）

### 日志
- 所有请求类型统一 LogBox 固定顺序块：请求入口 → 余额预占 → 结算记录(开始) → 上游请求 → 上游响应 → 用量提取 → 计费明细/计算 → 余额释放/扣减/DB更新 → 结算记录(完成) → 请求响应
- 日志路径禁止任何截断（除 errorMessage 2000）

## 数据库表

`user_users` / `proxy_tokens` / `proxy_channels` / `proxy_channel_tokens` / `proxy_channel_models` / `model_prices` / `model_library` / `model_templates` / `model_channel_config` / `proxy_logs` / `proxy_request_logs` / `user_model_discounts` / `packages` + `user_packages` / `exchange_rates` / `video_generation_tasks`（含 36 行历史数据勿删）/ `unified_stats` / `user_usage_stats` / 分层定价三表（`model_price_tiers` 等）/ `user_model_permissions` / `provider_capabilities`

DDL 与设计详见 `docs/migration-plan.md`、`docs/表结构.md`、`docs/*.sql`。

## 文档索引（docs/）

- `migration-plan.md` —— 从老项目迁移的总计划（含完整 DDL）
- `session-export-INTEGRATED-2026-08-03.md` —— 07-30~08-03 四次会话整合（基础设施/计费/生图/生视频/TTS/向量）
- `session-export-2026-08-04.md` —— 跨协议字段零遗漏透传 + Gemini 400 清洗 + 流错误传播 + SSE 协议补全
- `api接口定义.md` / `接口测试用例-全功能.md` / `生图接口测试-curl示例.md` / `生视频接口测试-curl示例.md`
- `渠道配置.md` / `测试场景.md`

## 当前状态（2026-08-07）

- 最近提交聚焦 **Codex 客户端函数调用兼容**：工具调用/工具结果的 OpenAI↔Anthropic 双向上行转换、`input_json_delta`/`thinking_delta` 流事件建模、thinking+tools 冲突规避（Qwen 思考循环）、流错误 `Flux.error` 传播
- 工作区有少量未提交改动（同属 Codex 修复主题），`mvn clean compile` BUILD SUCCESS
- 已知遗留：OpenAI 系 Provider 流式吞错误模式待清理、tool_use content block index 未递增、视频编辑 `/v1/videos/edits` 未迁移、分层定价/Redis 监控统计未做
