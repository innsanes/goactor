package core

import "goactor/message/mm"

const ShardCount int32 = mm.ShardCount

func ActorShard(actorID string) int32 {
	return mm.ActorShardID(actorID)
}
