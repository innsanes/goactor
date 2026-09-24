// Copyright (c) 2026 Innnsane. All rights reserved.
// Author: Innnsane
// Co-Author: OpenAI Codex

// 连接现有 Kafka 的最小示例，不自动创建 topic。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"goactor/message/mm"
	"goactor/mq"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "Kafka broker 列表，逗号分隔")
	topic := flag.String("topic", "goactor-demo", "已创建的业务 topic")
	group := flag.String("group", "goactor-demo", "checkpoint 命名空间")
	actor := flag.String("actor", "demo-1996", "Actor ID；默认值映射到 shard 0")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, mq.KafkaConfig{
		Brokers: strings.Split(*brokers, ","), Topic: *topic, GroupID: *group,
	}, *actor); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(parent context.Context, config mq.KafkaConfig, actorID string) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	queue, err := mq.NewKafka(ctx, config)
	if err != nil {
		return err
	}
	shard := mm.ActorShardID(actorID)
	// 主 goroutine 是控制方，所有 Subscribe/Commit/Unsubscribe 都在这里执行。
	if err := queue.Subscribe(ctx, shard); err != nil {
		queue.Close()
		return err
	}
	log.Printf("subscribed topic=%s shard=%d; Ctrl+C to stop", config.Topic, shard)

	mailbox := make(chan mm.Message, 16)
	pollError := make(chan error, 1)
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		for {
			messages, err := queue.Poll(ctx)
			if err != nil {
				// 示例采用遇错停止策略。即使伴随消息也不提交，重启从 checkpoint 重放。
				pollError <- err
				return
			}
			for _, message := range messages {
				select {
				case mailbox <- message:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	var producers sync.WaitGroup
	produceError := make(chan error, 2)
	defer func() {
		cancel() // 先停止 Poll 和投递；mailbox 剩余消息不提交。
		<-pollDone
		if err := queue.Unsubscribe(context.Background(), shard); err != nil {
			log.Printf("unsubscribe: %v", err)
		}
		queue.Close() // 唤醒可能仍在等待 broker 的发布调用。
		producers.Wait()
	}()

	// 两个生产 goroutine，各发布三条消息；Kafka adapter 不串行化它们。
	runID := fmt.Sprint(time.Now().UnixNano())
	for producer := 0; producer < 2; producer++ {
		producers.Add(1)
		go func(id int) {
			defer producers.Done()
			for sequence := 0; sequence < 3; sequence++ {
				message := mm.Message{
					MessageMeta: mm.MessageMeta{
						Receiver:  mm.MessageRef{Type: "demo", Id: actorID},
						MessageId: fmt.Sprintf("%s-%d-%d", runID, id, sequence),
						TraceId:   runID, Type: mm.MessageTypeNetwork,
					},
					Command: "demo.print",
					Payload: []byte(fmt.Sprintf("producer=%d sequence=%d", id, sequence)),
				}
				if _, err := queue.Publish(ctx, message); err != nil {
					produceError <- err
					return
				}
			}
		}(producer)
	}

	for {
		select {
		case message := <-mailbox:
			// 示例以打印代表处理完成；真实 Actor 应在快照/连续完成条件满足后提交。
			log.Printf("received id=%s offset=%d payload=%s", message.MessageId, message.Offset, message.Payload)
			if err := queue.CommitOffset(ctx, mq.ShardOffset{ShardId: shard, Offset: message.Offset + 1}); err != nil {
				return err
			}
		case err := <-pollError:
			if ctx.Err() != nil {
				return nil
			}
			return err
		case err := <-produceError:
			if ctx.Err() != nil {
				return nil
			}
			return err
		case <-ctx.Done():
			return nil
		}
	}
}
