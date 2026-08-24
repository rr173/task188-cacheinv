package service_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"task188-cacheinv/internal/model"
)

// TestConcurrentAppendNoLoss 验证：多个请求并发向同一编辑中的场景追加消息时，
// 每条成功追加都保留唯一的逻辑时钟与消息记录，不发生静默丢失。
// 回归此前 bug：AppendMessage 用应用层 MAX(id)+1 分配 id，并发下多个请求算出
// 相同 id，INSERT OR IGNORE 静默吞掉冲突行，导致“成功数 != DB 消息数”。
func TestConcurrentAppendNoLoss(t *testing.T) {
	st, svc, _ := newTestService(t)
	defer st.Close()

	ks, err := svc.Topo.CreateKeyspace("ks", "conc", nil)
	if err != nil {
		t.Fatalf("create keyspace: %v", err)
	}
	rp1, _ := svc.Topo.RegisterReplica(ks.ID, "a")
	_, _ = svc.Ver.ApplySourceUpdate(ks.ID, "user:1", 5, "v5")
	sc, _, err := svc.CreateScenario(ks.ID, "conc-sc", nil)
	if err != nil {
		t.Fatalf("create scenario: %v", err)
	}

	const N = 60
	var wg sync.WaitGroup
	errs := make([]error, N)
	start := make(chan struct{})
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(idx int) {
			defer wg.Done()
			<-start // 尽量同时触发，最大化竞态窗口。
			_, err := svc.AppendMessage(sc.ID, model.Message{
				MsgID: fmt.Sprintf("m-%d", idx), Kind: model.MsgUpdate,
				Key: "user:1", Version: 5, ReplicaID: rp1.ID,
			})
			errs[idx] = err
		}(i)
	}
	close(start)
	wg.Wait()

	successCount := 0
	for _, e := range errs {
		if e == nil {
			successCount++
		}
	}
	msgs, err := svc.ListMessages(sc.ID)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if successCount != len(msgs) {
		t.Fatalf("reported successful appends %d != DB messages %d (messages lost)",
			successCount, len(msgs))
	}
	if len(msgs) != N {
		t.Fatalf("expected %d messages, got %d", N, len(msgs))
	}

	// 每条成功追加必须持有唯一的逻辑时钟。
	seen := make(map[int64]bool, len(msgs))
	for _, m := range msgs {
		if m.ID == 0 {
			t.Fatalf("message %q has zero logical clock id", m.MsgID)
		}
		if seen[m.ID] {
			t.Fatalf("duplicate logical clock id %d", m.ID)
		}
		seen[m.ID] = true
	}

	// 逻辑时钟必须单调递增（DB 内连续 AUTOINCREMENT，从 1 起）。
	for i, m := range msgs {
		if m.ID != int64(i+1) {
			t.Fatalf("expected monotonic id %d at pos %d, got %d", i+1, i, m.ID)
		}
	}
}

// TestAppendDuplicateMessageRejected 验证：同 (scenario_id, msg_id) 的幂等重试
// 被显式拒绝（ErrDuplicateMessage），而非静默报成功 —— 这是修复后“成功才落库”的语义保障。
func TestAppendDuplicateMessageRejected(t *testing.T) {
	st, svc, _ := newTestService(t)
	defer st.Close()

	ks, _ := svc.Topo.CreateKeyspace("ks", "dup", nil)
	rp1, _ := svc.Topo.RegisterReplica(ks.ID, "a")
	_, _ = svc.Ver.ApplySourceUpdate(ks.ID, "k", 1, "v1")
	sc, _, _ := svc.CreateScenario(ks.ID, "dup-sc", nil)

	first, err := svc.AppendMessage(sc.ID, model.Message{
		MsgID: "dup", Kind: model.MsgUpdate, Key: "k", Version: 1, ReplicaID: rp1.ID,
	})
	if err != nil {
		t.Fatalf("first append: %v", err)
	}
	if first.ID == 0 {
		t.Fatal("first append must allocate a logical clock id")
	}
	// 同 msg_id 再追加应报重复，不再静默“成功”。
	if _, err := svc.AppendMessage(sc.ID, model.Message{
		MsgID: "dup", Kind: model.MsgUpdate, Key: "k", Version: 1, ReplicaID: rp1.ID,
	}); !errors.Is(err, model.ErrDuplicateMessage) {
		t.Fatalf("expected ErrDuplicateMessage, got %v", err)
	}
	msgs, _ := svc.ListMessages(sc.ID)
	if len(msgs) != 1 {
		t.Fatalf("expected exactly 1 message after duplicate, got %d", len(msgs))
	}
}

// TestAppendMessageBatchPreservesLogicalClockOrder 验证批量追加按调用顺序分配递增逻辑时钟。
func TestAppendMessageBatchPreservesLogicalClockOrder(t *testing.T) {
	st, svc, _ := newTestService(t)
	defer st.Close()

	ks, _ := svc.Topo.CreateKeyspace("ks", "batch", nil)
	rp1, _ := svc.Topo.RegisterReplica(ks.ID, "a")
	_, _ = svc.Ver.ApplySourceUpdate(ks.ID, "k", 1, "v1")
	sc, _, _ := svc.CreateScenario(ks.ID, "batch-sc", nil)

	in := []model.Message{
		{MsgID: "a", Kind: model.MsgUpdate, Key: "k", Version: 1, ReplicaID: rp1.ID},
		{MsgID: "b", Kind: model.MsgUpdate, Key: "k", Version: 1, ReplicaID: rp1.ID},
		{MsgID: "c", Kind: model.MsgUpdate, Key: "k", Version: 1, ReplicaID: rp1.ID},
	}
	out, err := svc.AppendMessages(sc.ID, in)
	if err != nil {
		t.Fatalf("append messages: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("expected %d appended, got %d", len(in), len(out))
	}
	for i, m := range out {
		if m.ID != int64(i+1) {
			t.Fatalf("pos %d: expected id %d, got %d", i, i+1, m.ID)
		}
	}
	_ = time.Now
}
