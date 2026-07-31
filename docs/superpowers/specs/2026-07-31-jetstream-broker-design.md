# JetStream Broker 设计

## 目标

为分布式 Actor 运行时提供最小的 JetStream 消息接口：持久化发布、durable pull 消费、显式 ACK/NAK/TERM，以及基于 `replyTo` 和 `requestID` 的同步最终结果回复。

本阶段只实现 MQ 边界，不实现 Router、Actor 激活、Redis 幂等、业务序列化或 timer 调度。

## 选择

使用 NATS JetStream，而非当前空的 RabbitMQ 骨架。JetStream 的 subject 分片、file stream、显式 ACK 和 pull consumer 适合大量 Actor 命令的高吞吐可靠投递。

MQ 层对外暴露 `Broker` 接口，Router 不直接依赖 NATS 的 `Msg`、订阅或 ACK 类型。

## 数据模型

`Envelope` 是持久化命令：

- `MessageID`：重投稳定的单条消息 ID；后续由 Redis 幂等逻辑使用；
- `RequestID`：同步请求和最终结果的关联 ID；
- `TraceID`：链路追踪 ID；
- `ActorID`：目标 Actor；
- `Subject`：JetStream 发布目标，例如 `actor.request.042`；
- `ReplyTo`：发送方临时 Core NATS inbox；
- `Deadline`：调用方接受的最晚完成时间；
- `Payload`：MQ 不解释的业务字节。

`Result` 是最终业务回复，包含 `RequestID`、状态码和业务字节。

## 接口与消费语义

```go
type Broker interface {
    Publish(context.Context, Envelope) error
    Consume(context.Context, ConsumerConfig, func(context.Context, Delivery)) error
    Request(context.Context, Envelope) (Result, error)
    Reply(context.Context, string, Result) error
    Close() error
}
```

`Consume` 创建或复用 durable pull consumer，并在上下文存活期间持续拉取消息。每个 `Delivery` 由处理方显式决定：

- Actor 处理完成且状态/幂等结果已经提交后，调用 `Ack`；
- 可恢复失败调用 `Nak(delay)`，由 JetStream 以后重投；
- 不可恢复消息调用 `Term`，停止自动重投并留下可观测错误。

本阶段 `Consume` 的 handler 不自动 ACK。这样后续 Router 可以把 ACK 放在 Redis 成功提交之后。

## 同步请求

`Request` 必须先创建并订阅一个 Core NATS inbox，再写入 JetStream。它把 inbox 写入 `Envelope.ReplyTo`，发布后等待同 `RequestID` 的 `Result`，直到调用上下文结束。

`Reply` 只向 inbox 发布最终结果；它不是持久化结果。后续 Router 必须在 Redis 中以 `RequestID` 持久化结果，使发送方超时、断线或重试时能够查询或重发同一个请求。

## JetStream 默认配置

```text
Stream name: ACTOR_REQUEST
Subjects: actor.request.*
Storage: File
Retention: WorkQueue
Replicas: 3 (可配置)

Consumer: durable pull consumer
AckPolicy: explicit
AckWait / MaxDeliver / MaxAckPending: 可配置
```

生产部署按 `hash(actorID)` 固定分片选择 subject；本阶段 Broker 接收调用方指定的 subject，不承担 Actor 分片策略。

## 非目标与验证

不实现 RabbitMQ 兼容层、死信流、延迟 timer、publisher de-duplication、Redis 幂等或 NATS 服务自动部署。

用户明确要求不新增测试。实现完成后至少运行 `go test ./mq`、`go vet ./mq` 和 `git diff --check`，前提是本地 Go 标准库环境可用。
