# JetStream 新 API 迁移设计

## 目标

将 `mq.JetStream` 从旧 `nats.JetStreamContext` API 迁移到官方 `github.com/nats-io/nats.go/jetstream` API。对外保持 `Broker`、`Envelope`、`Result` 和 `Delivery` 不变，不修改 RabbitMQ、Router 或 Actor。

当前依赖 `github.com/nats-io/nats.go v1.52.0` 已是官方 Go 模块当前最新版本。

## API 映射

| 旧实现 | 新实现 |
| --- | --- |
| `conn.JetStream()` | `jetstream.New(conn)` |
| `StreamInfo` + `AddStream` | `CreateOrUpdateStream` |
| `AddConsumer` | `stream.CreateOrUpdateConsumer` |
| `PullSubscribe` + `Fetch` 循环 | `consumer.Consume` |
| `nats.Msg` | `jetstream.Msg` |
| `PublishMsg` | `js.Publish` |

Stream 仍为 durable、file storage、work-queue retention，并保留配置中的副本数、subject 和 duplicate window。

## 消费语义

`Consume` 创建或更新 durable pull consumer，并调用 `consumer.Consume` 持续接收消息。回调仍不自动 ACK：

- Router 在业务成功、Redis 提交、同步 reply 后调用 `Delivery.Ack`；
- 可恢复失败调用 `Nak(delay)`；
- 不可恢复消息调用 `Term`；
- JSON 不合法的消息直接 `Term`，避免 poison-message 重投循环。

`Consumer.Consume` 返回 `ConsumeContext`。`Broker.Consume` 监听调用 context、Broker `Close` 和 `ConsumeContext.Closed()`；调用方取消或 Broker 关闭时执行 `Stop()`，不再使用手写 `Fetch` 轮询。

`ConsumerConfig.MaxAckPending` 映射为 `jetstream.PullMaxMessages`，限制客户端的在途缓冲。`BatchSize` 与 `FetchTimeout` 为兼容已有公共结构而保留，但新 JetStream 实现不使用它们。

## 发布与回复

`Publish` 使用：

```go
js.Publish(ctx, subject, data,
    jetstream.WithMsgID(messageID),
    jetstream.WithExpectStream(streamName),
)
```

它保留 broker 短窗口 publish 去重与目标 stream 校验。`Request` 和 `Reply` 继续通过 Core NATS Inbox 传递瞬时最终结果；持久化结果仍由未来 Router/Redis 的 `requestID` 记录负责。

## 验证边界

用户明确要求不新增测试文件。实现后运行 `go test ./mq`、`go vet ./mq`、`go test ./...` 和 `git diff --check`。本阶段不启动 NATS 服务，因此不声明运行时 broker 集成已验证。
