# franz-go Poll、消费控制与 offset API 核验

核验日期：2026-09-24。范围仅限项目锁定的 **franz-go v1.21.0 / kadm v1.17.2**，依据本机对应版本的一手源码；未连接 Kafka、未做运行时验证。版本见 `/Users/bole/Projects/goactor/go.mod:9-10`。本文解释 SDK 调用，不修改项目接口。

> 项目 adapter 已于 2026-09-24 简化重写。本文的 SDK 说明仍可参考；项目接口和并发约定以 `/Users/bole/Projects/goactor/docs/doc/mq.md` 为准，旧项目源码行号不再代表当前实现。

## 0. Poll 与结果读取

两者都返回 `kgo.Fetches`，不是 `(record, error)`：

```go
func (*Client) PollRecords(ctx context.Context, maxPollRecords int) Fetches
func (*Client) PollFetches(ctx context.Context) Fetches
```

`PollFetches(ctx)` 的函数体就是 `return cl.PollRecords(ctx, 0)`。
`maxPollRecords > 0` 限制一次交付给应用的总记录数（所有 topic/partition 合计）；
`<= 0` 不限制本次条数，取当前可交付的缓存。不会等待凑满 max，也不是读取整个 topic。
普通 ctx 下没有结果时等待数据、错误、取消或关闭；`ctx == nil` 则立即取当前缓存返回。[S12]

底层 fetch 与应用 Poll 是不同层次。默认允许客户端预取；`PollRecords(ctx, 1)`
只限制这次从缓存交付一条，不表示每条消息都单独向 broker 发一个请求。[S12][S14]

### Fetches 常用方法

| 方法 | 用法 |
| --- | --- |
| `RecordIter()` | `Done/Next` 遍历；方便 `break/return`。 |
| `RecordsAll()` | 当前版本支持 `for record := range fetches.RecordsAll()`。 |
| `EachRecord(fn)` | 简单回调遍历；回调内 return 不会退出外层消费循环。 |
| `EachPartition(fn)` | 按分区处理记录、错误、watermark 等信息。 |
| `Records()` | 将记录收集为一个 slice，会额外分配。 |
| `NumRecords()` | 本次返回的记录总数。 |
| `Errors()` / `EachError(fn)` | 遍历全部分区错误，不能被记录遍历替代。 |
| `Err()` | 第一个错误；不是完整错误列表。 |
| `Err0()` | 只检查第一个 fetch/topic/partition，适合快速识别取消或关闭。 |
| `IsClientClosed()` | 判断客户端关闭，用于退出循环。 |

正常记录和某些分区的错误可以共存；不要遇错直接 continue 丢掉有效记录。
关闭/取消通过错误 fetch 表达，也需要检查结果中的错误。[S13]

### 一个 Poll 协程投递 mailbox 的 SDK 示意

这里 mailbox 暂用原始 `*kgo.Record`，项目接入时再转换为 `mm.Message`。
示例采用收到 fetch 错误就返回的策略；上层需要根据错误类型决定恢复方式。
所有返回错误必须被上层消费，不能启动后忽略。未完成工作只能从正确 checkpoint 重放，
不能把“已投递 mailbox”当成“已处理完成并可提交”。[S5][S6][S8]

```go
func pollLoop(ctx context.Context, client *kgo.Client, mailbox chan<- *kgo.Record) error {
    for {
        fetches := client.PollRecords(ctx, 1)
        for it := fetches.RecordIter(); !it.Done(); {
            record := it.Next()
            select {
            case mailbox <- record:
            case <-ctx.Done():
                return ctx.Err()
            }
        }
        var failures []error
        for _, e := range fetches.Errors() {
            failures = append(failures,
                fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err))
        }
        if len(failures) > 0 {
            return errors.Join(failures...)
        }
    }
}
```

示例在项目锁定依赖下仅做了编译检查，没有连接 broker。
如希望一次遍历当前可用的一批，将 `PollRecords(ctx, 1)` 换成 `PollFetches(ctx)` 即可，
但应用手中待投递的批次可能更大。不要对 `PollRecords(nil, ...)` 做无等待的空转循环。[S12]

### Poll 条数不是网络缓存上限

- `FetchMaxBytes` / `FetchMaxPartitionBytes` 控制 broker fetch 的字节目标，
  大 record batch 可能超出配置值，不是整个进程内存的硬上限。[S14]
- `FetchMinBytes` / `FetchMaxWait` 控制 broker 聚合数据和等待时间，不是 Poll 必须凑满的数量。[S14]
- `MaxConcurrentFetches` 控制在途或缓冲 fetch 数；当前默认不人为限制（受 broker 数约束），
  `0` 为仅在 Poll 时发起单个 fetch 的特殊模式。[S14]
- `BufferedFetchRecords()` / `BufferedFetchBytes()` 观察客户端缓存，不包含应用 mailbox。[S15]
- `MaxBufferedRecords` 是 **producer** 配置，不是 consumer 的缓存条数上限。[S14]

## 1. 先区分两种消费模式

- **Direct assignment**：应用自己指定 topic/partition/起始 offset。`ConsumePartitions` 不兼容 consumer group 或 regex；运行中可使用 `AddConsumePartitions` / `RemoveConsumePartitions`。[S1][S2]
- **Group consuming**：配置 `kgo.ConsumerGroup(group)`，由组管理分区；本节后面的 `CommitRecords` 等 kgo 提交便捷方法及 rebalance 控制属于这种模式。[S1][S5][S7]
- **有 broker group checkpoint，不等于参加 consumer group 分配**：direct consumer 可以不配置 `ConsumerGroup`，通过 `kadm.CommitOffsets(ctx, group, offsets)` 保存 Kafka checkpoint，再用 `FetchOffsetsForTopics` 读取。这正是 kadm 明确支持的用途，不需要为存 offset 而加入组。[S1][S8][S9]

## 2. 暂停、恢复、增加、移除与 seek

| 调用 | 适用模式 | 用途与关键限制 |
| --- | --- | --- |
| `PauseFetchPartitions(map[string][]int32)` | Direct / Group | 临时背压；返回**所有当前暂停分区**，传空 map 可查询。暂停状态持续到恢复；topic 级暂停与 partition 级暂停独立。[S2] |
| `ResumeFetchPartitions(map[string][]int32)` | Direct / Group | 恢复指定分区；未暂停则无操作。若 topic 本身仍暂停，仅恢复 partition 不够。[S2] |
| `AddConsumePartitions(map[string]map[int32]kgo.Offset)` | 仅 Direct、非 regex | 添加显式分区及起始位置；不满足模式条件时直接返回，不是 group 手工认领分区的 API。[S2] |
| `RemoveConsumePartitions(map[string][]int32)` | 仅 Direct、非 regex | 移除消费分区；不会清除整个 topic 的元数据跟踪，也不是提交 checkpoint。[S2] |
| `SetOffsets(map[string]map[int32]kgo.EpochOffset)` | Direct / Group，后者约束更强 | 调整本地消费位置，不是 broker checkpoint 提交。按公开契约，只设置**先前已由 Poll 返回/消费过的分区**，其他分区跳过；不能当成首次订阅 API。[S4] |

### Pause：客户端缓存与已交付记录必须分开

**不能笼统地说“Pause 后，客户端预取缓存仍一定会继续返回”。** 此版本在组装 fetch 请求时排除暂停分区；Poll 取缓存时也检查暂停快照，剔除暂停分区的数据，且不推进那些被剔除记录的消费游标。恢复后可从尚未交付的位置重新获取；不是保证原缓存原封不动保留。[S3]

但 Pause **不会追回已经由 Poll 返回的记录**，更不会清空应用自己的 mailbox / 队列。如果在同一批结果的遍历途中暂停，该批剩余记录仍在调用方手中。并发执行 Pause 与 Poll 也不是一个原子屏障：Poll 使用的是其取缓存时读取的暂停快照。这些是由调用位置与实现推导的应用侧边界，不应把 Pause 当作撤回已交付工作。[S2][S3]

### SetOffsets 的额外约束

- Group 模式官方强烈建议在 Poll 循环上下文之外使用，确保不会并发 revoke，也不要与提交并发；事务消费建议使用 `GroupTransactSession`，避免自行调用它。[S4]
- “仅处理 Poll 已见分区”是文档契约；direct 内部转换函数本身不检查 group 的 uncommitted map，不能据此扩大公开保证。首次指定位置应使用 `ConsumePartitions` / `AddConsumePartitions`。[S1][S2][S4]
- seek 与 commit 是两个动作：改变本地读取位置不等于改变 broker 保存的恢复点。[S4][S8]

## 3. Group 的提交方法怎么选

| 调用 | 行为与合适场景 |
| --- | --- |
| `CommitRecords(ctx, records...) error` | 同步提交指定记录；内部转换为 `record.Offset + 1`，同分区按 epoch/offset 选择较新位置。适合禁用自动提交后，提交已完成的一批/部分批次；不建议高吞吐场景逐条请求。不同调用之间仍可把 checkpoint 提交倒退，应按分区顺序提交。[S5] |
| `CommitUncommittedOffsets(ctx) error` | 同步提交 Poll 已更新的未提交位置；适合 **Poll → 本批全部处理完成 → Commit**。它不会识别业务是否真正处理成功；不要在仍有已 Poll 但未完成工作时把它当作“只提交已完成”。[S6] |
| `MarkCommitRecords(records...)` | 需启用 `AutoCommitMarks()`；仅标记，不是一次 broker 提交。内部使用 `Offset + 1`，标记不会倒退；无 group 或未开启 marks 时无操作。[S5][S6] |
| `CommitMarkedOffsets(ctx) error` | 同步提交 marks；可在标记后要求立即落 checkpoint，否则开启自动提交时由自动提交处理。没有 marks 时无操作；若关闭自动提交，则标记本身不会自动落 broker。[S6] |

**这些 API 不是“逐消息 ack 位图”。** 提交/标记同分区较大的 offset，会覆盖此前的消费位置。因此 mailbox 并行处理时，应只推进已连续完成的位置，不能因为后面的某条完成就越过前面未完成的消息。这是从提交 map 与 marks 只记录位置的实现推出的使用约束。[S5][S6]

**Direct 不能套用这组提交方法**：本版本 `CommitRecords` / `CommitUncommittedOffsets` 最终进入 `CommitOffsetsSync`，无 group 返回内部 `errNotGroup`；`MarkCommitRecords` 无操作，`CommitMarkedOffsets` 无 marks 时返回 nil，均不代表 direct checkpoint 已写入。尤其不要依赖 `kgo.CommitOffsets` 注释中“非 group 无操作成功”的旧描述：该版本实际实现也返回 `errNotGroup`。[S5][S6]

## 4. BlockRebalanceOnPoll 与 AllowRebalance

仅面向 Group：配置 `BlockRebalanceOnPoll()` 后，Poll 返回非空 fetch 结果会阻挡相关 rebalance 回调，直到调用 `AllowRebalance()`。推荐顺序是 **Poll → 处理 → 同步提交 → AllowRebalance**；多个 Poll 可以共用一次释放，释放时等待中的 rebalance 优先于下一次 Poll。[S7]

这是防止正常处理/提交跨越分区撤销的工具，**不是无限期 ownership 保证**：处理或回调过慢，超过 rebalance timeout 仍可能被组踢出。用 `PollRecords` 限制单批数量有助于控制阻塞时间；错误/提前退出路径也应释放，别只在成功路径调用。Direct assignment 没有这套组内 rebalance，无需配置这组选项。[S1][S7]

## 5. Direct 的 kadm checkpoint 路径

1. `FetchOffsetsForTopics(ctx, group, topics...)`：查询指定 group 下的已提交 offset；缺少 commit 的实际 topic/partition 补 `At = -1`。默认只返回请求的 topics；内部涉及 metadata 查询及 offset 查询，**一次方法调用不等于一个网络 RPC**。获取/列举中的错误会返回 error。[S9]
2. 对查询结果明确制定无 checkpoint 策略，再交给 `AddConsumePartitions`。**不要直接把未提交标志 `-1` 当作“从头读”**：`kgo.Offset.At(-1)` 是末尾，`At(-2)` 才是开头。本项目将缺失 checkpoint 映射到 `OffsetBeginning`。[S10][S11]
3. 处理完成后调用 `kadm.CommitOffsets(ctx, group, offsets)`，传的是**下一条要读的位置**。完成到记录 100，应保存 101；kadm 将 `Offset.At` 原样写入请求，**不会再 +1**。[S5][S8]
4. 同时检查顶层 `error` 和 `responses.Error()`，或逐分区检查 `OffsetResponse.Err`。提交可部分成功，授权错误也可能放在分区响应里；仅看 `err == nil` 不够。[S8]

当前项目已采用这条路径：构造客户端不设置 `ConsumerGroup`，`GroupID` 用作 checkpoint 命名空间；订阅查询 checkpoint，提交使用 kadm。不要把这个 ID 与自动管理的消费组共用，避免两套位置语义相互覆盖。本次只确认已有用法，不修改实现。[S11]

## 一手源码引用

以下均为本次实际读取的绝对路径与行号；多个行段以分号分隔。

- **[S1]** `/Users/bole/go/pkg/mod/github.com/twmb/franz-go@v1.21.0/pkg/kgo/config.go:1641-1654; 1789-1800` — direct / group 配置边界。
- **[S2]** `/Users/bole/go/pkg/mod/github.com/twmb/franz-go@v1.21.0/pkg/kgo/consumer.go:668-724; 888-965` — Pause/Resume、Add/Remove 的注释及实现。
- **[S3]** `/Users/bole/go/pkg/mod/github.com/twmb/franz-go@v1.21.0/pkg/kgo/consumer.go:511-554`；`/Users/bole/go/pkg/mod/github.com/twmb/franz-go@v1.21.0/pkg/kgo/source.go:466-537; 550-628; 690-732` — 暂停快照、缓存剔除、游标及请求过滤。
- **[S4]** `/Users/bole/go/pkg/mod/github.com/twmb/franz-go@v1.21.0/pkg/kgo/consumer.go:726-777`；`/Users/bole/go/pkg/mod/github.com/twmb/franz-go@v1.21.0/pkg/kgo/consumer_direct.go:46-60` — SetOffsets 契约、模式差异及本地 assignment。
- **[S5]** `/Users/bole/go/pkg/mod/github.com/twmb/franz-go@v1.21.0/pkg/kgo/consumer_group.go:2598-2720` — CommitRecords、MarkCommitRecords 与 +1。
- **[S6]** `/Users/bole/go/pkg/mod/github.com/twmb/franz-go@v1.21.0/pkg/kgo/consumer_group.go:2494-2515; 2538-2564; 2762-2852; 2937-2941; 2982-2989`；`/Users/bole/go/pkg/mod/github.com/twmb/franz-go@v1.21.0/pkg/kgo/config.go:2088-2110; 2127-2136` — 未提交/标记位置、同步提交、非 group 错误、marks 配置。
- **[S7]** `/Users/bole/go/pkg/mod/github.com/twmb/franz-go@v1.21.0/pkg/kgo/config.go:1926-1967`；`/Users/bole/go/pkg/mod/github.com/twmb/franz-go@v1.21.0/pkg/kgo/consumer.go:499-518; 624-634` — rebalance 阻挡、错误 fetch 路径与释放。
- **[S8]** `/Users/bole/go/pkg/mod/github.com/twmb/franz-go/pkg/kadm@v1.17.2/groups.go:775-792; 801-864` — direct checkpoint 提交、原值传递、分区错误及缺失响应。
- **[S9]** `/Users/bole/go/pkg/mod/github.com/twmb/franz-go/pkg/kadm@v1.17.2/groups.go:937-1017` — FetchOffsetsForTopics 的返回值与实现。
- **[S10]** `/Users/bole/go/pkg/mod/github.com/twmb/franz-go@v1.21.0/pkg/kgo/consumer.go:164-173` — -1/-2 的读取位置含义。
- **[S11]** `/Users/bole/Projects/goactor/mq/kafka.go:23-25; 108-128; 176-202; 359-386` — 当前项目的 direct、checkpoint 恢复与提交路径。
- **[S12]** `/Users/bole/go/pkg/mod/github.com/twmb/franz-go@v1.21.0/pkg/kgo/consumer.go:418-622` — PollFetches/PollRecords 等价关系、条数上限、nil ctx、缓存读取和等待。
- **[S13]** `/Users/bole/go/pkg/mod/github.com/twmb/franz-go@v1.21.0/pkg/kgo/record_and_fetch.go:419-481; 501-733` — Fetches 错误及记录遍历方法。
- **[S14]** `/Users/bole/go/pkg/mod/github.com/twmb/franz-go@v1.21.0/pkg/kgo/config.go:628; 1233-1238; 1426-1505` — producer buffer、consumer fetch 配置与并发默认值。
- **[S15]** `/Users/bole/go/pkg/mod/github.com/twmb/franz-go@v1.21.0/pkg/kgo/consumer.go:323-341` — 客户端缓存观测。
