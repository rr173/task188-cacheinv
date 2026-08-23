package service_test

import (
	"strings"
	"testing"

	"task188-cacheinv/internal/model"
)

// TestIsolatedReplicaRejectsUpdate 复现 bug：
// 副本被隔离后仍可接收更新消息，且场景可能被判定为收敛。
// 修复后：隔离副本的更新必须被拒绝并记录可定位的协议违反。
func TestIsolatedReplicaRejectsUpdate(t *testing.T) {
	st, svc, _ := newTestService(t)
	defer st.Close()

	ks, err := svc.Topo.CreateKeyspace("ks", "isol", nil)
	if err != nil {
		t.Fatalf("create keyspace: %v", err)
	}
	rp1, _ := svc.Topo.RegisterReplica(ks.ID, "a")
	rp2, _ := svc.Topo.RegisterReplica(ks.ID, "b")
	_, _ = svc.Ver.ApplySourceUpdate(ks.ID, "k", 3, "v3")

	// 隔离 rp2。
	if _, err := svc.Topo.SetReplicaStatus(rp2.ID, model.ReplicaIsolated); err != nil {
		t.Fatalf("isolate rp2: %v", err)
	}

	sc, _, _ := svc.CreateScenario(ks.ID, "isol", nil)
	// 给隔离副本 rp2 下发更新消息。
	_, _ = svc.AppendMessage(sc.ID, model.Message{
		MsgID: "u-iso", Kind: model.MsgUpdate, Key: "k", Version: 3, ReplicaID: rp2.ID,
	})
	// 让允许副本 rp1 正常收敛，使旧实现可能把整个场景判为收敛。
	_, _ = svc.AppendMessage(sc.ID, model.Message{
		MsgID: "u-ok", Kind: model.MsgUpdate, Key: "k", Version: 3, ReplicaID: rp1.ID,
	})

	res, err := svc.RunReplay(sc.ID)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if res.Status != model.ScenarioMonotonicityViolated {
		t.Fatalf("expected monotonicity_violated for update to isolated replica, got %s (viol=%v)",
			res.Status, res.Violations)
	}
	viols, _ := svc.ListViolations(sc.ID)
	if len(viols) == 0 {
		t.Fatal("expected a violation recording the isolated-replica update")
	}
	var v model.Violation
	for _, vv := range viols {
		if vv.Kind == "isolated_replica" {
			v = vv
			break
		}
	}
	if v.Kind == "" {
		t.Fatalf("expected an isolated_replica violation, got %+v", viols)
	}
	if v.StepSeq == 0 {
		t.Fatalf("violation must carry a locatable step_seq, got 0: %+v", v)
	}
	if !strings.Contains(v.Message, rp2.ID) {
		t.Fatalf("violation message must name the isolated replica %s, got %q", rp2.ID, v.Message)
	}
	// 隔离副本不应被应用该更新。
	if rkv, err := svc.Ver.GetReplicaKeyVersion(rp2.ID, "k"); err == nil && rkv.Version == 3 {
		t.Fatalf("isolated replica must not observe the update, got %+v", rkv)
	}
}

// TestIsolatedReplicaRejectsAllMessageKinds 验证隔离副本对全部携带
// replica_id 的消息类型（更新/失效/租约/确认/重试）均拒绝并记录违反。
func TestIsolatedReplicaRejectsAllMessageKinds(t *testing.T) {
	st, svc, _ := newTestService(t)
	defer st.Close()

	ks, err := svc.Topo.CreateKeyspace("ks", "isol-kinds", nil)
	if err != nil {
		t.Fatalf("create keyspace: %v", err)
	}
	rp1, _ := svc.Topo.RegisterReplica(ks.ID, "a")
	_, _ = svc.Ver.ApplySourceUpdate(ks.ID, "k", 1, "v1")

	if _, err := svc.Topo.SetReplicaStatus(rp1.ID, model.ReplicaIsolated); err != nil {
		t.Fatalf("isolate rp1: %v", err)
	}

	cases := []struct {
		msgID string
		kind  model.MessageKind
	}{
		{"i-upd", model.MsgUpdate},
		{"i-inv", model.MsgInvalidate},
		{"i-lease", model.MsgLease},
		{"i-ack", model.MsgAck},
		{"i-retry", model.MsgRetry},
	}
	sc, _, _ := svc.CreateScenario(ks.ID, "isol-kinds", nil)
	for _, c := range cases {
		_, _ = svc.AppendMessage(sc.ID, model.Message{
			MsgID: c.msgID, Kind: c.kind, Key: "k", Version: 1, ReplicaID: rp1.ID,
		})
	}
	res, err := svc.RunReplay(sc.ID)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if res.Status != model.ScenarioMonotonicityViolated {
		t.Fatalf("expected monotonicity_violated, got %s", res.Status)
	}
	viols, _ := svc.ListViolations(sc.ID)
	if len(viols) != len(cases) {
		t.Fatalf("expected %d isolated_replica violations, got %d (%+v)", len(cases), len(viols), viols)
	}
	for _, v := range viols {
		if v.Kind != "isolated_replica" {
			t.Fatalf("expected isolated_replica kind, got %s: %s", v.Kind, v.Message)
		}
		if v.StepSeq == 0 {
			t.Fatalf("violation must carry a locatable step_seq, got 0: %+v", v)
		}
	}
}
