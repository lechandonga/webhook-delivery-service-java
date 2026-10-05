# Webhook Delivery Service (Java / Spring Boot)

本地事件订阅与投递服务：订阅方按事件类型注册回调地址，事件发布后异步、可靠地推送到订阅方，并提供幂等发布、失败重试、死信、顺序保证与背压。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/subscriptions` | 注册订阅：`{"eventType": "...", "callbackUrl": "http://..."}` |
| POST | `/api/events` | 发布事件：`{"eventId": "...", "eventType": "...", "payload": "..."}` |
| GET | `/api/events/{eventId}/deliveries` | 查询事件在各订阅方上的投递状态 |
| GET | `/api/dead-letters` | 查询死信记录（含失败原因） |

## 投递语义与顺序保证

- **至少一次（at-least-once）**：订阅方超时或返回 5xx 时会重试，订阅方需自行按 `X-Event-Id` 请求头去重。
- **同一订阅方内保序**：每个订阅方对应一个单线程投递执行器，事件按发布顺序串行投递；不同订阅方之间相互隔离、并行投递，单个订阅方持续失败不会拖慢其他订阅方。
- **投递状态**：`PENDING`（待投递/重试中）→ `DELIVERED`（2xx）/ `FAILED_PERMANENT`（4xx，不重试）/ `DEAD_LETTER`（重试耗尽）。

## 幂等发布

发布以 `eventId` 为幂等键（对事件类型 + 内容做 SHA-256 指纹，并发提交通过原子操作保证安全）：

- 同一 `eventId` 内容一致：返回原事件，不重复投递。
- 同一 `eventId` 内容不一致：返回 `409`，`error=DUPLICATE_EVENT_CONFLICT`，并说明原因。

## 重试与死信规则

- **可重试失败**：连接失败、读超时、订阅方返回 5xx。按指数退避重试：第 n 次重试等待 `backoff-base-ms * 2^(n-1)`。
- **不可重试失败**：订阅方返回 4xx 视为业务性拒绝，立即标记 `FAILED_PERMANENT`，不重试。
- **死信**：重试次数（`max-attempts`）耗尽后进入 `DEAD_LETTER`，记录 `deadLetterReason`（含最后一次错误），可通过 `/api/dead-letters` 排查。

## 背压

全局待投递事件数超过 `webhook.delivery.max-pending` 时，新的发布请求被拒绝，返回 `429`，`error=BACKPRESSURE_LIMIT_EXCEEDED`，与业务错误可区分，避免内存积压失控。

## 配置项

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `webhook.delivery.max-attempts` | 3 | 最大尝试次数（含首次） |
| `webhook.delivery.backoff-base-ms` | 200 | 指数退避基数（毫秒） |
| `webhook.delivery.connect-timeout-ms` | 2000 | 连接超时（毫秒） |
| `webhook.delivery.request-timeout-ms` | 3000 | 请求/响应超时（毫秒） |
| `webhook.delivery.max-pending` | 10000 | 全局待投递上限（背压阈值） |

## 运行与测试

```bash
mvn spring-boot:run   # 启动服务
mvn test              # 运行集成测试
```

集成测试（`WebhookDeliveryIntegrationTest`）使用本地 HTTP 桩模拟订阅方的延迟与错误响应，覆盖：重复发布幂等、超时重试进死信、4xx/5xx 区分处理、订阅方隔离与顺序保证。测试日志会输出各场景的执行与通过情况。
