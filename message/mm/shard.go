package mm

import "hash/fnv"

const ShardCount int32 = 1024

// ActorShardID returns the stable shard used to route an Actor's Kafka records.
func ActorShardID(actorID string) int32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(actorID))
	return int32(h.Sum32() % uint32(ShardCount))
}
