# 最小 Kafka adapter

本版按调用方的 goroutine 分工工作，不再提供 lifecycle、control gate、消费代次、
后台 channel 或额外 mutex。franz-go 自己负责连接、预取和客户端内部并发。

## 调用约定

| 调用方 | 调用方法 |
| --- | --- |
| 一个控制 goroutine | Subscribe、Unsubscribe、SeekMessage、PausePartition、ResumePartition、CommitOffset |
| 一个 Poll goroutine | Poll(ctx) 获取一批 → 遍历发送 Node mailbox → 继续 Poll |
| 多个生产 goroutine | Publish；需要时也可调用 DLQ |

`Poll(ctx)` 返回 `([]mm.Message, error)`；内部直接调用 `PollFetches(ctx)`，
转换本次取出的所有记录，不覆盖或丢弃前面的消息。没有额外的 max 参数、
batch 类型、定时器或缓冲队列。一批可以包含多个分区的多条消息。
投递方式仍是逐条发送到 Node mailbox，不需要把整个 mailbox 改成批次 channel。
这不是等待凑满固定数量，而是取当前可用的一批；暂未投递完的批次由 Poll 协程持有。
消息和错误可能同时返回，调用方必须分别检查；不能遇错 continue 后跳过未提交的消息。
最小示例采用遇错停止并交给上层恢复的策略，不自动重试。

控制方维护已订阅 shard，避免重复 Subscribe。Subscribe 内部查询固定 Topic 的
checkpoint；没有 checkpoint 从头开始。key 仍为 Actor ID，partition 仍为 int32
shard ID，value 为 Payload，框架与扩展元数据在 headers 中。

CommitOffset 接收下一条 offset，原值提交，不再在 adapter 内维护提交上界。
控制方保证按连续完成位置单调提交并处理失败。DLQ 成功也不会自动提交源 offset。
同步网络方法受调用方 ctx 和 KafkaConfig.Timeout 限制；ctx 取消不是 broker 回滚。

## Pause、Seek、Unsubscribe、关闭

- Pause/Resume 可以与 Poll 并行，但 Pause 不撤回已经取出或进入 mailbox 的消息。
- Seek/Unsubscribe 前：取消并等待 Poll goroutine 退出，停用旧 mailbox/在途工作，
  再在控制 goroutine 切换位置或释放 shard。需要继续读取时，用新 ctx 启动 Poll。
- Seek 只改变读取位置，保留暂停状态；需要继续拉取时显式 Resume。
- 这些跨调用协调由 Node 管理，本 adapter 不自动过滤旧消息或做 ownership fencing。
- 停机先停止新业务操作、取消 Poll 的投递循环；控制方调用 Close 一次，唤醒 SDK 内
  的等待；调用方等待自己启动的 goroutine 结束。不要关闭仍有发送者的 mailbox。
- Publish 的不同 goroutine 没有应用层先后顺序保证。需要因果顺序时由发送方串行调用。

## 可运行示例

入口：`/Users/bole/Projects/goactor/examples/kafka/main.go`。

准备一个测试 Kafka 和业务 topic，例如使用 Kafka 自带工具：

```sh
kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic goactor-demo --partitions 1 --replication-factor 1

cd /Users/bole/Projects/goactor
go run ./examples/kafka -brokers localhost:9092 -topic goactor-demo -group goactor-demo
```

默认 Actor ID `demo-1996` 在现有 FNV-1a/1024-shard 映射下落到 shard 0，
因此演示只需要一个分区；更改 Actor ID 后，topic 必须有对应编号的分区。
不要使用生产 topic/group：示例会发布消息并提交 checkpoint。

示例主 goroutine 负责订阅、处理 mailbox、提交、退订；一个 goroutine Poll，
两个生产 goroutine 各发布三条消息。程序会持续等待消息，Ctrl+C 退出。
没有自动建 topic、自动重试、自动 rebalance 或 Actor 持久化。
示例把打印当作处理完成；真实 Node 仍需在快照与连续完成条件满足后提交。

DLQ 为可选配置；使用时设置与业务 topic 不同的 DLQTopic，并确保目标分区存在。

## 验证边界

离线检查覆盖转换、原始错误保留、参数/取消校验和并发编码；示例编译与帮助入口
不需要 Kafka。真实发布、消费、checkpoint、Seek 和关闭行为仍需连接测试 broker 联调。
