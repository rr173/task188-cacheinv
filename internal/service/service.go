// Package service 编排拓扑、版本、投递、回放、证明与规格模块，
// 并实现回放/证明/规格模块所需的存储接口（转发到底层仓储）。
package service

import (
	"time"

	"task188-cacheinv/internal/delivery"
	"task188-cacheinv/internal/model"
	"task188-cacheinv/internal/proof"
	"task188-cacheinv/internal/replay"
	"task188-cacheinv/internal/spec"
	"task188-cacheinv/internal/store"
	"task188-cacheinv/internal/topology"
	"task188-cacheinv/internal/versioning"
)

// Service 是应用编排根对象。
type Service struct {
	st  *store.Store
	ks  *store.KeyspaceStore
	vs  *store.VersionStore
	scs *store.ScenarioStore
	sps *store.SpecStore

	Topo *topology.Manager
	Ver  *versioning.Service
	Dlv  *delivery.Manager
	Replay  *replay.Engine
	Proof   *proof.Generator
	Spec    *spec.Service

	now func() time.Time
}

// New 构造编排服务。
func New(st *store.Store, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	ks := store.NewKeyspaceStore(st.DB())
	vs := store.NewVersionStore(st.DB())
	scs := store.NewScenarioStore(st.DB())
	sps := store.NewSpecStore(st.DB())

	topo := topology.NewManager(ks, now)
	ver := versioning.NewService(vs, now)
	dlv := delivery.NewManager(now)

	s := &Service{
		st: st, ks: ks, vs: vs, scs: scs, sps: sps,
		Topo: topo, Ver: ver, Dlv: dlv, now: now,
	}
	s.Replay = replay.New(s, ver, dlv, now)
	s.Proof = proof.New(s, now)
	s.Spec = spec.New(s, now)
	return s
}

// Now 返回当前时间（供测试注入）。
func (s *Service) Now() time.Time { return s.now() }

// ---------------------------------------------------------------
// 以下方法实现 replay.Repo / proof.Repo / spec.Repo 接口。
// ---------------------------------------------------------------

func (s *Service) GetScenario(id int64) (*model.Scenario, error) { return s.scs.GetScenario(id) }

func (s *Service) UpdateScenarioStatus(id int64, status model.ScenarioStatus, cursorPos int) error {
	return s.scs.UpdateScenarioStatus(id, status, cursorPos)
}

func (s *Service) ListMessages(scenarioID int64) ([]model.Message, error) {
	return s.scs.ListMessages(scenarioID)
}

func (s *Service) UpdateMessageStatus(id int64, status model.MessageStatus) error {
	return s.scs.UpdateMessageStatus(id, status)
}

func (s *Service) UpdateMessageStatusAndRetry(id int64, status model.MessageStatus, retry int) error {
	return s.scs.UpdateMessageStatusAndRetry(id, status, retry)
}

func (s *Service) AppendReplayEvent(e *model.ReplayEvent) error { return s.scs.AppendReplayEvent(e) }

func (s *Service) AppendViolation(v *model.Violation) error { return s.scs.AppendViolation(v) }

func (s *Service) GetProtocol(keyspaceID string) (*model.ProtocolParams, error) {
	return s.ks.GetProtocol(keyspaceID)
}

func (s *Service) ListReplicas(keyspaceID string) ([]model.Replica, error) {
	return s.ks.ListReplicas(keyspaceID)
}

func (s *Service) GetReplica(id string) (*model.Replica, error) { return s.ks.GetReplica(id) }

func (s *Service) ListSourceVersions(keyspaceID string) ([]model.SourceVersion, error) {
	return s.vs.ListSourceVersions(keyspaceID)
}

func (s *Service) GetSourceVersion(keyspaceID, key string) (*model.SourceVersion, error) {
	return s.vs.GetSourceVersion(keyspaceID, key)
}

func (s *Service) ListReplicaKeyVersions(replicaID string) ([]model.ReplicaKeyVersion, error) {
	return s.vs.ListReplicaKeyVersions(replicaID)
}

func (s *Service) ListReplayEvents(scenarioID int64) ([]model.ReplayEvent, error) {
	return s.scs.ListReplayEvents(scenarioID)
}

func (s *Service) ListViolations(scenarioID int64) ([]model.Violation, error) {
	return s.scs.ListViolations(scenarioID)
}

func (s *Service) InsertProof(p *model.Proof) (int64, error) { return s.scs.InsertProof(p) }

func (s *Service) GetProof(scenarioID int64) (*model.Proof, error) {
	return s.scs.GetProof(scenarioID)
}

func (s *Service) CreateSpec(sp *model.Spec, msgs []model.SpecMessage) (int64, error) {
	return s.sps.CreateSpec(sp, msgs)
}

func (s *Service) GetSpec(id int64) (*model.Spec, error) { return s.sps.GetSpec(id) }

func (s *Service) ListSpecs() ([]model.Spec, error) { return s.sps.ListSpecs() }

func (s *Service) FreezeSpec(id int64) error { return s.sps.FreezeSpec(id) }

func (s *Service) ListSpecMessages(specID int64) ([]model.SpecMessage, error) {
	return s.sps.ListSpecMessages(specID)
}
