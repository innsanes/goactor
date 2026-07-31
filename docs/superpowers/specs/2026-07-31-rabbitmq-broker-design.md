# RabbitMQ Broker 与 Actor 接入设计

## 目标

在不改变 `mq.Broker` 调用方式的前提下，增加 RabbitMQ 实现，与 JetStream 并列。两种 Broker 都承载持久化 Actor 命令、显式确认和同步最终结果回复。

本阶段不接入 etcd owner、Redis 幂等、死信队列、延迟重试或完整 Router 实现。

## RabbitMQ 映射

`Envelope.Subject` 是逻辑路由键。例如 `actor.request.042`：

- JetStream 直接作为 subject 发布；
- RabbitMQ 发布到可配置的 durable topic exchange，默认 `ACTOR_REQUEST`，并作为 routing key；
- `ConsumerConfig.Durable` 是 RabbitMQ durable queue 名；
- `ConsumerConfig.Subject` 是该 queue 的 binding key；
- `ConsumerConfig.MaxAckPending` 映射为该 channel 的 QoS prefetch。

RabbitMQ 发布消息设置 `DeliveryMode=Persistent`，并开启 publisher confirms；只有 broker confirm 成功，`Publish` 才返回成功。

消费使用 manual ACK：

| Broker 方法 | RabbitMQ 行为 |
| --- | --- |
| `Delivery.Ack()` | `Ack(false)` |
| `Delivery.Nak(0)` | `Nack(false, true)`，立即重新入队 |
| `Delivery.Nak(delay > 0)` | 返回明确的“不支持延迟重试”错误 |
| `Delivery.Term()` | `Reject(false)`；若基础设施配置 DLX，消息进入死信路径 |

本阶段不隐藏 RabbitMQ 没有原生 delayed NACK 的事实。将来如需延迟重试，使用 TTL 加 dead-letter exchange、RabbitMQ delayed-message 插件，或让 Redis timer 投递新命令。

## 同步请求和回复

`Request` 建立 exclusive、auto-delete 的临时 reply queue，先开始消费，再发布持久化请求。请求消息携带：

- `ReplyTo`：临时 queue 名；
- `CorrelationId`：`Envelope.RequestID`；
- JSON 编码的 `Envelope`。

`Reply` 将 JSON 编码的 `Result` 发布到默认 exchange，routing key 为 `ReplyTo`，并设置相同 correlation ID。reply queue 是瞬时通道，最终结果的可重试持久化仍由后续 Redis `requestID` 记录负责。

## Actor 循环接入

不为每个玩家 Actor 创建 RabbitMQ 或 JetStream consumer。玩家可离线、Actor 会回收；百万个 Actor consumer 会使连接、channel 和 queue 数量失控。

每个 Go 进程只由 Router 持有一个或少数 `Broker.Consume` 循环：

```text
RabbitMQ / JetStream Delivery
  -> Router 根据 actorID 查询或激活本地 owner
  -> Router 将本地消息和 completion 句柄写入 Actor mailbox
  -> Actor 单协程执行业务
  -> Actor 把业务结果回传 Router
  -> Router 提交 Redis state + messageID/result
  -> Router Reply 同步调用方
  -> Router Ack MQ Delivery
```

Actor 的消息通道不再只保存裸 `core.Message`，而应逐步演变为内部投递包装：

```go
type routedMessage struct {
    Message    core.Message
    Envelope   mq.Envelope
    Complete   chan<- ActorResult
}
```

Actor loop 仍保持单协程，不直接调用 MQ ACK：

```go
case item := <-a.mailbox:
    result := a.handle(item.Message)
    item.Complete <- result
```

Router 才拥有 `Delivery`，因此只有 Router 在 Actor 成功、Redis 成功、reply 发出后调用 `Ack`。当 Actor mailbox 满、正在启动、远程 owner 转发失败或 Redis 失败时，Router 调用 `Nak`，而不是丢弃消息。

## 共享代码与验证

JSON `Envelope` / `Result` 编解码和字段校验移动到 `mq` 通用文件，JetStream 与 RabbitMQ 实现共同使用。新增 AMQP 0-9-1 Go 客户端依赖。

用户明确要求不新增测试文件。实现后运行 `go test ./mq`、`go vet ./mq`、`go test ./...` 与 `git diff --check`；不启动 RabbitMQ 集成环境。
