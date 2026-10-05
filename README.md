# Webhook Delivery Service（本地事件订阅与投递服务）

基于 Spring Boot 3 的本地 webhook 投递服务：订阅方按事件类型注册本地 HTTP 回调地址，
发布事件后服务负责把事件可靠地投递给所有匹配的订阅方，并提供状态查询、幂等发布、
失败重试、死信、顺序保证与背压能力。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/webhooks/subscriptions` | 注册订阅：`subscriberId` + `eventType` + `callbackUrl` |
| GET  | `/api/webhooks/subscriptions` | 列出全部订阅 |
| POST | `/api/webhooks/events` | 发布事件：`eventId` + `eventType` + `payload`（JSON 字符串） |
| GET  | `/api/webhooks/events/{eventId}` | 查询事件及其全部投递状态 |
| GET  | `/api/webhooks/events/{eventId}/deliveries` | 只查事件的投递状态列表 |

投递请求以 `POST` 调用回调，请求头携带 `X-Event-Id`、`X-Event-Type`、`X-Delivery-Attempt`，
请求体为事件原始 payload。

错误响应统一为 `{"code": "...", "message": "..."}`，错误码稳定可机读：

| 错误码 | 状态码 | 含义 |
| --- | --- | --- |
| `EVENT_CONTENT_CONFLICT` | 409 | 同一 `eventId` 重复发布但 `eventType` 或 payload 不一致 |
| `SUBSCRIPTION_CONFLICT` | 409 | 同一（订阅方, 事件类型）已注册到不同回调地址 |
| `BACKPRESSURE_QUEUE_FULL` | 429 | 目标订阅方待投递队列积压已满，拒绝本次发布 |
| `EVENT_NOT_FOUND` / `INVALID_*` / `MALFORMED_JSON` | 400/404 | 事件不存在或请求非法 |

## 投递语义

- **至少一次（at-least-once）投递**：超时、连接失败、5xx 都会重试，订阅方可能在极端情况下
  （如 ACK 丢失）收到重复事件；订阅方应基于 `X-Event-Id` 自行做消费幂等。
- **发布幂等**：发布以调用方提供的 `eventId` 为幂等键。
  - 同一 `eventId` 重复或并发提交，且 `eventType` 与 payload 的 **JSON 语义**一致
    （字段顺序、空白差异不影响），服务不重复投递，返回首次发布时创建的原有投递结果
    （响应中 `created=false`）。
  - 内容不一致时返回 `409 EVENT_CONTENT_CONFLICT` 并说明原因，不会覆盖原事件。
- **状态可查**：每个（事件, 订阅方）对都有一条投递记录，可随时查询
  `PENDING / IN_FLIGHT / RETRYING / SUCCEEDED / DEAD`、已尝试次数、最后尝试时间、
  下次重试时间与失败原因。

## 顺序保证与故障隔离

- 每个 `subscriberId` 拥有**独立的有界队列和单线程投递 worker**：
  - 同一订阅方严格按事件发布顺序投递。队首事件处于退避重试期间会暂时挡住后续事件，
    不会为了赶进度而乱序超车；
  - 不同订阅方的 worker 互相独立、并行运行。某个订阅方持续超时或失败只影响它自己的队列，
    不会拖慢其他订阅方。

## 重试与死信规则

- 以下情况视为**可重试失败**：单次请求超时、连接超时/连接被拒绝等传输异常、订阅方返回 `5xx`。
- 订阅方返回 `4xx` 等业务性失败**不重试**，仅尝试一次直接进入死信。
- 退避策略为**指数退避**：`delay(attempt) = min(backoff-initial-ms * multiplier^(attempt-1), backoff-max-ms)`，
  每次重试日志都会打印失败原因与下次重试间隔。
- 尝试次数（含首次）达到 `max-attempts` 后投递进入死信（`DEAD`），
  状态查询可看到总尝试次数与最终失败原因（最后一次网络/HTTP 错误）。

## 背压

- 每个订阅方的待投递队列容量由 `webhook.delivery.subscriber-queue-capacity` 限定（默认 200）。
- 发布时若**任一**目标订阅方队列已满，整次发布在入队前即被拒绝，
  返回 `429 BACKPRESSURE_QUEUE_FULL` 并指明是哪个订阅方积压，不产生部分投递，
  调用方可稍后重试。由此保证内存占用有上界，不会被积压拖垮。

## 配置项（`application.properties`，前缀 `webhook.delivery`）

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `http-timeout-ms` | `5000` | 单次回调请求超时，超时按可重试失败处理 |
| `max-attempts` | `5` | 最大尝试次数（首投 + 重试），耗尽进入死信 |
| `backoff-initial-ms` | `1000` | 指数退避初始间隔 |
| `backoff-multiplier` | `2.0` | 指数退避倍率 |
| `backoff-max-ms` | `60000` | 退避间隔上限 |
| `subscriber-queue-capacity` | `200` | 每个订阅方待投递队列容量，满了触发背压拒绝 |

## 存储说明

当前实现为**单机内存版**：事件、订阅与投递记录保存在进程内存中，重启即清空，
适用于本地开发与集成测试。投递中的消息不会在进程崩溃后恢复。

## 测试

集成测试位于 `src/test/java/.../tests/WebhookDeliveryIntegrationTests.java`，
使用 JDK 内置 HTTP 服务器（`StubHttpServer`）起本地桩模拟订阅方的延迟、5xx、4xx 等行为，
`ScenarioLoggingExtension` 会在日志中显式打印每个业务场景的开始与执行结果。
覆盖场景：

1. 重复/并发发布幂等，内容不一致明确拒绝（`EVENT_CONTENT_CONFLICT`）；
2. 订阅方先 5xx 后恢复，按指数退避重试后成功；
3. 订阅方持续超时，重试耗尽进入死信并可查到原因；
4. 4xx 业务性失败不重试，直接进死信；
5. 持续失败的订阅方不影响健康订阅方，且同一订阅方按序投递；
6. 积压超过队列上限时发布被背压拒绝（`BACKPRESSURE_QUEUE_FULL`）。

运行：

```bash
mvn test
```

测试环境配置（`src/test/resources/application.properties`）把超时、退避和队列容量调小，
使各场景快速收敛；日志中搜索 `BUSINESS-SCENARIO` 可看到场景清单与通过/失败结果。
