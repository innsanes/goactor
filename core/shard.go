package core

import "hash/fnv"

const ShardCount int16 = 1024

func ActorShard(actorID string) int16 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(actorID))
	return int16(h.Sum32() % uint32(ShardCount))
}
