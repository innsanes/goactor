# 分布式 Actor 运行时可靠性设计

## 目标

为“一个 Actor 对应一个玩家”的服务提供一套低延迟、可恢复且不过度依赖强事务的运行时架构。Actor 只在玩家在线或有消息需要处理时激活，空闲一段时间后自动退出。

设计优先处理进程崩溃、消息重投、Actor 懒启动和定时器恢复。Redis 整体故障、跨存储严格事务和金融级 exactly-once 不属于当前阶段的强保证范围。

## 组件职责

### etcd：Actor 唯一性与节点发现

etcd 属于控制面，不承载业务消息或 Actor 状态。它负责：

- 注册存活的 Go 服务进程；
- 通过 Txn CAS 保证同一 `actorID` 同时只有一个 owner；
- 提供 `actorID -> nodeID` 查询；
- 在进程 lease 到期时自动删除该进程拥有的 Actor 注册。

每个 Go 服务进程只创建一个 `NodeSession` 和一个 lease。进程内所有已激活 Actor 的 key 复用该 lease：

```text
/goactor/nodes/node-B          -> {rpcAddr, bootID}       lease-B
/goactor/actors/player-42      -> {nodeID, actorEpoch}    lease-B
/goactor/actors/player-10086   -> {nodeID, actorEpoch}    lease-B
```

只注册当前已激活的 Actor，不为全部玩家预创建 etcd key。

### Router：节点 lease 与 Actor 生命周期

每个 Go 服务进程有一个 Router。Router 持有 `NodeSession`，但 etcd keepalive 由 etcd client 的后台 goroutine 维护，不进入 Router 的消息循环。

Router 负责：

- 本地 Actor 的 single-flight 激活；
- 调用 etcd Claim、GetOwner 和 Release；
- 保存本地 `actorID -> Actor` 映射及 Activating、Ready、Stopping 状态；
- 将消息投递给本地 Actor，或转发给远程 owner；
- Actor 空闲回收；
- 进程正常退出时先停止 Actor，最后关闭 `NodeSession`。

### MQ：持久化消息与背压

业务消息和需要强制唤醒的 timer 消息进入持久化 MQ。MQ 提供至少一次投递、未 ACK 重投和 Actor 离线期间的消息保存，但不直接承诺业务 exactly-once。

每条消息包含：

```text
messageID   单条消息的稳定幂等 ID，重投时不变
traceID     端到端链路追踪 ID；一次 trace 可产生多条 messageID
actorID
payload
deadline    可选
replyTo     同步请求可选
```

MQ 消息只有在 Actor 完成业务处理并成功提交 Redis 状态后才能 ACK。mailbox 满、Actor 正在激活、远程转发失败或进程退出时不得提前 ACK。

当 MQ consumer 所在节点不是 Actor owner 时，该节点保留原消息的 ACK 权限并转发给 owner；只有收到 owner 的成功处理结果后才 ACK 原消息。Actor 仍在 Activating 时，owner Router 暂存投递关系并等待 Ready，不把“已找到 owner”当作“业务已完成”。

### Redis：运行期热状态、Timer 与幂等

Redis 保存：

- Actor 最新热状态和 `stateVersion`；
- timer 元数据与到期索引；
- 必要的 `messageID` 幂等记录和同步请求结果；
- 进程异常退出后的快速恢复数据。

一次消息处理通过 Lua 原子完成：

```text
检查 messageID 是否已经处理
  -> 已处理：返回已保存结果
  -> 未处理：更新 Actor 状态和 stateVersion
              记录 messageID/result
              返回结果
```

Redis 成功后才 ACK MQ。若 Redis 成功而 MQ ACK 前进程崩溃，消息会重投，Lua 根据相同 `messageID` 返回已有结果，不重复产生业务效果。

幂等记录 TTL 至少覆盖 MQ 保留期、最大重试时间、ACK 超时、死信重放窗口和调用方重试窗口。高吞吐场景可以对严格有序消息使用 `lastProcessedSeq`，timer 使用 `timerID + timerVersion`，只对非幂等关键操作保存独立 messageID。

### Storage：长期快照与降级恢复

Storage 指 MySQL、PostgreSQL 或其他长期存储，保存 Actor 数据快照和 timer 快照。它不是每条普通消息的同步热路径。

持久化触发条件包括：

- 周期 checkpoint；
- 累计一定数量的脏消息；
- Actor 长时间空闲；
- Actor 正常 Stop；
- 金币、购买、发奖等关键操作需要时立即持久化。

Redis 与 Storage 都保存递增 `stateVersion`。恢复时优先使用有效且版本更新的 Redis 状态；Redis 不存在时回退到 Storage 快照。Storage checkpoint 成功后更新 `persistedVersion`；若 Actor 在保存期间继续产生新版本，它仍保持 dirty，等待下一次 checkpoint。

## Actor 激活流程

```text
MQ 消息到达某节点 Router
  -> Router 检查本地激活表
  -> 本地不存在时，对 /goactor/actors/{actorID} 执行 Txn Claim
      -> Claim 成功：本节点成为 owner
      -> Claim 冲突：读取 owner node 并转发消息
  -> owner 从 Redis 恢复状态
      -> Redis 无数据时从 Storage 恢复
  -> 恢复 timer
  -> Actor 进入 Ready
  -> 处理消息
  -> Redis 原子提交状态与幂等记录
  -> ACK MQ 并回复同步调用方
```

Actor Claim 使用进程共享 lease：

```text
IF Version(actorKey) == 0
THEN Put(actorKey, {nodeID, actorEpoch}, WithLease(nodeLeaseID))
ELSE Get(actorKey)
```

`actorEpoch` 是单次 Actor 激活的随机 UUID，用于区分同一节点上的不同实例。Router 在本地还需要 single-flight，避免同一进程中的并发创建流程互相竞争。

## Actor 退出流程

### 正常退出

```text
Ready -> Stopping
  -> Router 停止向 Actor 投递新消息
  -> 排空已受理的本地消息
  -> 执行最终 Storage checkpoint
  -> 设置或刷新 Redis TTL
  -> 持久化成功后 CAS 删除 Actor key
  -> 删除本地 Actor
```

Release 必须比较 `nodeID + actorEpoch` 后再删除，避免迟到的旧实例删除新 owner：

```text
IF Value(actorKey) == myOwnerValue
THEN Delete(actorKey)
```

单个 Actor 正常退出只删除自己的 key，不能关闭共享 node lease。

### 进程正常退出

```text
Router 停止接收新的激活请求
  -> 停止并持久化所有本地 Actor
  -> Actor 分别 Release
  -> 最后关闭 NodeSession
```

### 进程异常退出或 lease 丢失

进程崩溃后 keepalive 停止，node lease 到期，节点 key 和该进程拥有的所有 Actor key 自动删除。其他节点随后可以 Claim，并通过 Redis 和未 ACK MQ 消息恢复。

`NodeSession.Done()` 表示本进程已经无法证明 owner 身份。最低限度处理是停止新的 Actor 激活和消息接收，不在失主后继续 drain 并执行业务。当前阶段不提供跨 etcd、Redis 和 MQ 的严格 fencing 事务。

## Timer 设计

所有 timer 都写入 Redis，进程内 timer heap 只是活跃 Actor 的调度加速器。Actor 启动时从 Redis 恢复未完成 timer：已到期任务立即补处理，未到期任务重新放入本地 timer heap。

Timer 包含：

```text
timerID
timerVersion
fireAt
wakePolicy     on_activation | required
status         pending | due | dispatching | retry_wait | done | cancelled
payload
attempts
nextRetryAt
leaseUntil
```

`on_activation` timer 到期后保留在 Redis，不主动唤醒 Actor；玩家登录或其他消息激活 Actor 后批量补处理。离线收益、体力恢复等可计算任务优先保存 `lastCalculatedAt`，避免生成大量逐次 timer。

`required` timer 由 Scheduler 扫描到期记录并投递 MQ，Router 收到后强制激活 Actor。Timer 成功处理后才标记 done 并 ACK MQ。失败时进入 `retry_wait`，按退避时间重试；不可恢复错误进入可观测的 failed/dead 状态。

mailbox 满时，on-activation timer 继续保留 due 状态；required timer 不 ACK MQ 并延迟重投。本地 timer 投递失败时设置未来 `nextRetryAt` 后重新放入 timer heap，禁止立即自旋重试。

Timer 重设或取消通过递增 `timerVersion` 完成。旧版本消息即使晚到，也会因为版本不匹配而被忽略。

Redis 写 timer 失败时：required timer 所属业务进行有限重试，仍失败则不能对外承诺成功；best-effort/on-activation timer 可以记录错误并异步重试，同时接受进程崩溃前仍未写入时可能丢失。

## Mailbox 与背压

Actor mailbox 为有界队列。普通同步请求在 mailbox 满时返回 `ActorBusy`；MQ 消息不 ACK 并由 Broker 延迟重投。心跳、位置覆盖等天然幂等消息可以按业务策略合并或丢弃。

Timer 调度通道可以与普通消息通道分开，但 Actor 仍是单协程串行执行业务。一次 timer tick 应限制投递数量，避免大量到期任务长期挤压普通请求。

## 一致性与可靠性边界

当前设计保证或尽力提供：

- etcd Txn 保证正常情况下 Actor 激活唯一；
- MQ 保证未 ACK 消息可以重投；
- Redis Lua 让状态更新与消息幂等记录原子完成；
- Go 进程崩溃且 Redis、MQ 正常时，Actor 可以恢复近期状态、timer 和未完成消息；
- 正常 Stop 和周期 checkpoint 将状态长期保存到 Storage。

当前设计不承诺：

- Redis 整体数据丢失后仍恢复所有已 ACK、尚未 checkpoint 的消息效果；
- etcd、Redis 与 MQ 之间的跨系统严格事务；
- MQ 只投递一次；实际语义为至少一次投递加业务幂等；
- 所有 timer 精确准点触发；系统保证可恢复和最终处理，允许负载导致的延迟。

如果将来需要加强可靠性，可以逐步增加 checkpoint 频率、关键操作同步 Storage、持久化事件日志或 outbox，而不必让所有普通消息从一开始就同步写 Storage。

## 验证重点

实现后至少覆盖以下场景：

- 两个节点并发 Claim 同一 Actor，只有一个成功；
- 单个 Actor Release 不影响同进程其他 Actor 注册；
- node lease 到期后，其节点 key 和所有 Actor key 被删除；
- Actor 正常 Stop 在 Storage checkpoint 成功后才释放注册；
- Redis 更新成功但 MQ ACK 前崩溃，消息重投不重复修改状态；
- mailbox 满时 MQ 消息不丢失且不会立即自旋；
- Actor 重启恢复 Redis timer，过期 timer 补处理；
- timer 重设后，旧 version 消息不会执行业务；
- required timer 失败后进入可恢复重试状态；
- Redis 缺失时从 Storage 快照恢复。
