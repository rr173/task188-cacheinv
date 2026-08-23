package service_test

import (
	"path/filepath"
	"testing"
	"time"

	"task188-cacheinv/internal/model"
	"task188-cacheinv/internal/service"
	"task188-cacheinv/internal/store"
)

// newTestService 创建临时库服务。
func newTestService(t *testing.T) (*store.Store, *service.Service, string) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	st, err := store.Open(store.Options{Path: dbPath})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return st, service.New(st, time.Now), dbPath
}

func TestEndToEndConvergence(t *testing.T) {
	st, svc, _ := newTestService(t)
	defer st.Close()

	ks, err := svc.Topo.CreateKeyspace("ks", "e2e", nil)
	if err != nil {
		t.Fatalf("create keyspace: %v", err)
	}
	rp1, _ := svc.Topo.RegisterReplica(ks.ID, "a")
	rp2, _ := svc.Topo.RegisterReplica(ks.ID, "b")
	if _, err := svc.Ver.ApplySourceUpdate(ks.ID, "user:1", 3, "v3"); err != nil {
		t.Fatalf("source update: %v", err)
	}

	sc, _, err := svc.CreateScenario(ks.ID, "conv", nil)
	if err != nil {
		t.Fatalf("create scenario: %v", err)
	}
	msgs := []model.Message{
		{MsgID: "i1", Kind: model.MsgInvalidate, Key: "user:1", Version: 1, ReplicaID: rp1.ID},
		{MsgID: "l1", Kind: model.MsgLease, Key: "user:1", Version: 1, ReplicaID: rp1.ID},
		{MsgID: "a1", Kind: model.MsgAck, Key: "user:1", Version: 1, ReplicaID: rp1.ID},
		{MsgID: "u1", Kind: model.MsgUpdate, Key: "user:1", Version: 3, ReplicaID: rp1.ID},
		{MsgID: "i2", Kind: model.MsgInvalidate, Key: "user:1", Version: 2, ReplicaID: rp2.ID},
		{MsgID: "l2", Kind: model.MsgLease, Key: "user:1", Version: 2, ReplicaID: rp2.ID},
		{MsgID: "a2", Kind: model.MsgAck, Key: "user:1", Version: 2, ReplicaID: rp2.ID},
		{MsgID: "u2", Kind: model.MsgUpdate, Key: "user:1", Version: 3, ReplicaID: rp2.ID},
	}
	if _, err := svc.AppendMessages(sc.ID, msgs); err != nil {
		t.Fatalf("append: %v", err)
	}
	res, err := svc.RunReplay(sc.ID)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if res.Status != model.ScenarioConverged {
		t.Fatalf("expected converged, got %s (viol=%v)", res.Status, res.Violations)
	}
	p, err := svc.Proof.Build(sc.ID)
	if err != nil {
		t.Fatalf("proof: %v", err)
	}
	if p.Conclusion != "converged" {
		t.Fatalf("expected converged proof, got %s", p.Conclusion)
	}
}

func TestAckWithoutLeaseViolation(t *testing.T) {
	st, svc, _ := newTestService(t)
	defer st.Close()

	ks, _ := svc.Topo.CreateKeyspace("ks", "viol", nil)
	rp1, _ := svc.Topo.RegisterReplica(ks.ID, "a")
	_, _ = svc.Ver.ApplySourceUpdate(ks.ID, "k", 3, "v3")

	sc, _, _ := svc.CreateScenario(ks.ID, "bad", nil)
	// 无租约直接 ACK。
	_, _ = svc.AppendMessage(sc.ID, model.Message{
		MsgID: "bad", Kind: model.MsgAck, Key: "k", Version: 3, ReplicaID: rp1.ID,
	})
	res, err := svc.RunReplay(sc.ID)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if res.Status != model.ScenarioMonotonicityViolated {
		t.Fatalf("expected violation, got %s", res.Status)
	}
	viols, _ := svc.ListViolations(sc.ID)
	if len(viols) == 0 {
		t.Fatal("expected violation record")
	}
}

func TestReopenPersistsState(t *testing.T) {
	st, svc, dbPath := newTestService(t)

	ks, _ := svc.Topo.CreateKeyspace("ks", "persist", nil)
	rp1, _ := svc.Topo.RegisterReplica(ks.ID, "a")
	_, _ = svc.Ver.ApplySourceUpdate(ks.ID, "k", 1, "v1")
	sc, _, _ := svc.CreateScenario(ks.ID, "s1", nil)
	_, _ = svc.AppendMessage(sc.ID, model.Message{
		MsgID: "u", Kind: model.MsgUpdate, Key: "k", Version: 1, ReplicaID: rp1.ID,
	})
	if _, err := svc.RunReplay(sc.ID); err != nil {
		t.Fatalf("replay: %v", err)
	}
	sp, err := svc.Spec.FreezeFromScenario(sc.ID, "spec1")
	if err != nil {
		t.Fatalf("freeze: %v", err)
	}
	_ = st.Close()

	st2, err := store.Open(store.Options{Path: dbPath})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	svc2 := service.New(st2, time.Now)
	restored, err := svc2.GetScenario(sc.ID)
	if err != nil {
		t.Fatalf("restore scenario: %v", err)
	}
	if restored.Status != model.ScenarioConverged {
		t.Fatalf("expected restored converged, got %s", restored.Status)
	}
	spec2, err := svc2.GetSpec(sp.ID)
	if err != nil {
		t.Fatalf("restore spec: %v", err)
	}
	if spec2.Status != model.SpecFrozen || spec2.MessageHash == "" {
		t.Fatalf("expected frozen spec with hash, got %+v", spec2)
	}
}

func TestCursorResumeAfterReopen(t *testing.T) {
	st, svc, dbPath := newTestService(t)

	ks, _ := svc.Topo.CreateKeyspace("ks", "resume", nil)
	rp1, _ := svc.Topo.RegisterReplica(ks.ID, "a")
	rp2, _ := svc.Topo.RegisterReplica(ks.ID, "b")
	_, _ = svc.Ver.ApplySourceUpdate(ks.ID, "k", 5, "v5")

	sc, _, _ := svc.CreateScenario(ks.ID, "resume-sc", nil)
	// 只给 rp1 发消息 → 第一次回放应为 timeout（rp2 未收敛）。
	_, _ = svc.AppendMessage(sc.ID, model.Message{
		MsgID: "u1", Kind: model.MsgUpdate, Key: "k", Version: 5, ReplicaID: rp1.ID,
	})
	if res, _ := svc.RunReplay(sc.ID); res.Status != model.ScenarioTimeoutNotConverged {
		t.Fatalf("expected timeout, got %s", res.Status)
	}
	_ = st.Close()

	// 重开：游标与状态必须从稳定检查点恢复，不从头重放。
	st2, err := store.Open(store.Options{Path: dbPath})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	svc2 := service.New(st2, time.Now)
	restored, err := svc2.GetScenario(sc.ID)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if restored.CursorPos != 1 || restored.Status != model.ScenarioTimeoutNotConverged {
		t.Fatalf("expected cursor=1 status=timeout, got cursor=%d status=%s",
			restored.CursorPos, restored.Status)
	}
	// 幂等续跑：已处理的消息不重复计数（processed=0）。
	res, err := svc2.RunReplay(sc.ID)
	if err != nil {
		t.Fatalf("replay after reopen: %v", err)
	}
	if res.Processed != 0 {
		t.Fatalf("expected no reprocessing after resume, got processed=%d", res.Processed)
	}
	// rp1 的观察版本已持久化，重启后仍可见。
	rkv, err := svc2.Ver.GetReplicaKeyVersion(rp1.ID, "k")
	if err != nil {
		t.Fatalf("restored replica version: %v", err)
	}
	if rkv.Version != 5 || rkv.Status != model.KeyValid {
		t.Fatalf("expected rp1 observed v5 valid, got %+v", rkv)
	}
	_ = rp2
}

func TestFingerprintReuse(t *testing.T) {
	st, svc, _ := newTestService(t)
	defer st.Close()

	ks, _ := svc.Topo.CreateKeyspace("ks", "fp", nil)
	rp1, _ := svc.Topo.RegisterReplica(ks.ID, "a")
	_, _ = svc.Ver.ApplySourceUpdate(ks.ID, "k", 1, "v1")
	msgs := []model.Message{
		{MsgID: "u", Kind: model.MsgUpdate, Key: "k", Version: 1, ReplicaID: rp1.ID},
	}
	sc1, _, _ := svc.CreateScenario(ks.ID, "first", msgs)
	_, _ = svc.AppendMessages(sc1.ID, msgs)
	_, _ = svc.RunReplay(sc1.ID)

	// 相同指纹的新场景应标记复用（reused=true）。
	sc2, reused, err := svc.CreateScenario(ks.ID, "second", msgs)
	if err != nil {
		t.Fatalf("create second: %v", err)
	}
	if !reused {
		t.Fatalf("expected fingerprint reuse for identical message set (sc2=%d)", sc2.ID)
	}
}
