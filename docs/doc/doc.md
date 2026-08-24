
- golang分布式模型，每个进程是一个node，其中可以运行多个actor，分为node shard actor三个层级
- shard固定数量一般在1024左右不变，shard会分配到不同node上，每个shard在etcd上lease，用于服务发现和负载均衡
- 哪个shard分配到哪个node上需要etcd来分配，rebalance和故障转移等机制，使用中心化controller决策
- shard分配到node上采用一致性哈希
- actor会hash到固定的shard上，每个actor由一个goroutine承载，内部串行执行

- 每个shard上会有一个shard runtime用于consume mq的消息，以及负责active actor
- actor之间通过mq传递消息，mq会持久化，shard runtime收到消息后如果对应的actor是回收状态，会主动激活
- 一个shard一个mq分区，比如Kafka，需要assign机制，消息在分区内保证顺序，本身会持久化offset
- actor有已处理消息的持久化机制，有界的循环数组，如果是actor id+message seq 相同则代表已经处理，直接返回结果
- actor的message channel有界，满了阻塞了就进入mq的pause/resume流程
- 消息执行失败，分类，重试，或者达到重试上限和遇到无法执行的情况，包括无法反序列化等，将消息推到死信队列，继续推进offset

- actor空闲一定时间后会进行自我回收，回收前会存档，每隔一段时间也会进行存档，使用异步快照和批量罗盘，即便过程中失败也是安全的
- 存档会比较fencing，每次active都会获得增加的fencing，只有大于才能成功写，fencing也在持久化内容中
- actor存档即snapshot，在snapshot时保存所有数据以及最新的offset，还有对已处理的消息有持久化幂等
- 存档成功之后actor将处理过的消息传给shard runtime，维护shard内所有actor都已落盘覆盖的连续最小offset，如果有推进给予提交到mq
- actor重新激活时加载snapshot，shard runtime将所有actor所属的消息推入actor的channel，actor根据snapshot中的offset判断是否重放
- 需要保证下游和所有的actor消息幂等，外部系统应当提供幂等键，业务处理完成包括下游消息发送之后才会进行snapshot等操作

- shard迁移时停止所有actor，尽量提交offset，释放lease

- 可观测性，Prometheus监控node、shard和actor的关键指标，OpenTelemetry记录message的分布式链路，ELK记录服务器日志
