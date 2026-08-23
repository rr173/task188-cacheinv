// Package spec 管理回归规格：冻结场景、保存完整消息序列哈希、按规格重放验证。
package spec

import (
	"errors"
	"testing"
	"time"

	"task188-cacheinv/internal/model"
)

// fakeRepo 是用于规格服务测试的内存存储实现。
type fakeRepo struct {
	specs      map[int64]*model.Spec
	specMsgs   map[int64][]model.SpecMessage
	scenarios  map[int64]*model.Scenario
	messages   map[int64][]model.Message
	protocol   map[string]*model.ProtocolParams
	nextSpecID int64
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		specs:     map[int64]*model.Spec{},
		specMsgs:  map[int64][]model.SpecMessage{},
		scenarios: map[int64]*model.Scenario{},
		messages:  map[int64][]model.Message{},
		protocol:  map[string]*model.ProtocolParams{},
	}
}

func (f *fakeRepo) CreateSpec(spec *model.Spec, msgs []model.SpecMessage) (int64, error) {
	f.nextSpecID++
	id := f.nextSpecID
	cp := *spec
	cp.ID = id
	f.specs[id] = &cp
	f.specMsgs[id] = append([]model.SpecMessage(nil), msgs...)
	return id, nil
}

func (f *fakeRepo) GetSpec(id int64) (*model.Spec, error) {
	sp, ok := f.specs[id]
	if !ok {
		return nil, model.ErrNotFound
	}
	cp := *sp
	return &cp, nil
}

func (f *fakeRepo) ListSpecs() ([]model.Spec, error) {
	out := make([]model.Spec, 0, len(f.specs))
	for _, s := range f.specs {
		out = append(out, *s)
	}
	return out, nil
}

func (f *fakeRepo) FreezeSpec(id int64) error {
	sp, ok := f.specs[id]
	if !ok {
		return model.ErrNotFound
	}
	sp.Status = model.SpecFrozen
	return nil
}

func (f *fakeRepo) ListSpecMessages(specID int64) ([]model.SpecMessage, error) {
	return append([]model.SpecMessage(nil), f.specMsgs[specID]...), nil
}

func (f *fakeRepo) ListMessages(scenarioID int64) ([]model.Message, error) {
	return append([]model.Message(nil), f.messages[scenarioID]...), nil
}

func (f *fakeRepo) GetScenario(id int64) (*model.Scenario, error) {
	sc, ok := f.scenarios[id]
	if !ok {
		return nil, model.ErrNotFound
	}
	return sc, nil
}

func (f *fakeRepo) GetProtocol(keyspaceID string) (*model.ProtocolParams, error) {
	p, ok := f.protocol[keyspaceID]
	if !ok {
		return nil, model.ErrNotFound
	}
	return p, nil
}

// setupFrozenSpec 构造一个已收敛场景并冻结为规格，返回规格 ID 与协议指针。
func setupFrozenSpec(t *testing.T, s *Service) (int64, *model.ProtocolParams) {
	t.Helper()
	const ksID = "ks-1"
	const scID = int64(1)
	fake := s.repo.(*fakeRepo)
	fake.scenarios[scID] = &model.Scenario{ID: scID, KeyspaceID: ksID, Status: model.ScenarioConverged}
	fake.messages[scID] = []model.Message{
		{ID: 1, MsgID: "u1", Kind: model.MsgUpdate, Key: "k", Version: 1, ReplicaID: "rp-1"},
	}
	p := &model.ProtocolParams{
		KeyspaceID:           ksID,
		MaxRetries:           3,
		LeaseTTLMs:           5000,
		ConvergenceTimeoutMs: 10000,
		Ordering:             "logical",
	}
	fake.protocol[ksID] = p
	sp, err := s.FreezeFromScenario(scID, "spec")
	if err != nil {
		t.Fatalf("freeze: %v", err)
	}
	return sp.ID, p
}

// TestRecheckPassedWhenProtocolUnchanged 协议未变化时 Recheck 应返回 passed。
func TestRecheckPassedWhenProtocolUnchanged(t *testing.T) {
	s := New(newFakeRepo(), time.Now)
	id, _ := setupFrozenSpec(t, s)

	res, err := s.Recheck(id)
	if err != nil {
		t.Fatalf("recheck: %v", err)
	}
	if res != "passed" {
		t.Fatalf("expected passed, got %s", res)
	}
}

// TestRecheckFailedOnProtocolDrift 协议参数变化后 Recheck 必须返回 failed（规格漂移），
// 而非仍报 passed。这是本修复的核心回归点。
func TestRecheckFailedOnProtocolDrift(t *testing.T) {
	s := New(newFakeRepo(), time.Now)
	id, p := setupFrozenSpec(t, s)

	// 冻结后修改协议参数：旧规格应被发现不再代表当前协议。
	p.MaxRetries = 9

	res, err := s.Recheck(id)
	if err != nil {
		t.Fatalf("recheck: %v", err)
	}
	if res != "failed" {
		t.Fatalf("expected failed on protocol drift, got %s", res)
	}
}

// TestRecheckFailedOnLeaseTTLChange 任一协议参数变化都构成漂移。
func TestRecheckFailedOnLeaseTTLChange(t *testing.T) {
	s := New(newFakeRepo(), time.Now)
	id, p := setupFrozenSpec(t, s)
	p.LeaseTTLMs = 99999

	res, _ := s.Recheck(id)
	if res != "failed" {
		t.Fatalf("expected failed on lease_ttl change, got %s", res)
	}
}

// TestRecheckFailedOnOrderingChange ordering 字段变化也构成漂移。
func TestRecheckFailedOnOrderingChange(t *testing.T) {
	s := New(newFakeRepo(), time.Now)
	id, p := setupFrozenSpec(t, s)
	p.Ordering = "sequential"

	res, _ := s.Recheck(id)
	if res != "failed" {
		t.Fatalf("expected failed on ordering change, got %s", res)
	}
}

// TestRecheckFailedOnMessageDrift 协议未变但消息序列变化也应判为 failed。
func TestRecheckFailedOnMessageDrift(t *testing.T) {
	s := New(newFakeRepo(), time.Now)
	id, _ := setupFrozenSpec(t, s)
	fake := s.repo.(*fakeRepo)

	// 追加一条消息改变消息序列哈希。
	fake.messages[1] = append(fake.messages[1],
		model.Message{ID: 2, MsgID: "u2", Kind: model.MsgUpdate, Key: "k", Version: 2, ReplicaID: "rp-1"})

	res, _ := s.Recheck(id)
	if res != "failed" {
		t.Fatalf("expected failed on message drift, got %s", res)
	}
}

// TestRecheckRejectedForUnfrozenSpec 未冻结规格不可 Recheck。
func TestRecheckRejectedForUnfrozenSpec(t *testing.T) {
	s := New(newFakeRepo(), time.Now)
	id, _ := setupFrozenSpec(t, s)
	fake := s.repo.(*fakeRepo)
	fake.specs[id].Status = model.SpecDraft

	if _, err := s.Recheck(id); !errors.Is(err, model.ErrSpecFrozen) {
		t.Fatalf("expected ErrSpecFrozen, got %v", err)
	}
}

// TestFreezeCapturesProtocolHash 冻结时必须写入非空协议指纹。
func TestFreezeCapturesProtocolHash(t *testing.T) {
	s := New(newFakeRepo(), time.Now)
	id, _ := setupFrozenSpec(t, s)
	sp, err := s.GetSpec(id)
	if err != nil {
		t.Fatalf("get spec: %v", err)
	}
	if sp.ProtocolHash == "" {
		t.Fatal("expected non-empty protocol hash on frozen spec")
	}
}
