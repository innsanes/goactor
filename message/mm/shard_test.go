package mm

import "testing"

func TestActorShardIDInt32AndStableRouting(t *testing.T) {
	// 类型迁移不改变 FNV-1a 和 1024 个 shard 的路由结果。
	cases := map[string]int32{
		"": 453, "actor-1": 460, "receiver": 624,
	}
	for actor, want := range cases {
		var got int32 = ActorShardID(actor)
		if got != want || got < 0 || got >= ShardCount {
			t.Fatalf("ActorShardID(%q) = %d, want %d", actor, got, want)
		}
	}
	var count int32 = ShardCount
	if count != 1024 {
		t.Fatal("shard count changed")
	}
}
