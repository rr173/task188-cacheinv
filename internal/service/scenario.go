package service

import (
	"fmt"

	"task188-cacheinv/internal/model"
	"task188-cacheinv/internal/replay"
	"task188-cacheinv/internal/versioning"
)

// CreateScenario 创建场景（editing 态），并做同指纹复用检查。
// 返回场景与是否复用了既有结果。
func (s *Service) CreateScenario(keyspaceID, name string, msgs []model.Message) (*model.Scenario, bool, error) {
	if _, err := s.Topo.GetKeyspace(keyspaceID); err != nil {
		return nil, false, err
	}
	sc := replay.BuildScenario(keyspaceID, name, msgs)
	existing, err := s.scs.FindScenarioByFingerprint(keyspaceID, sc.Fingerprint)
	if err != nil && err != model.ErrNotFound {
		return nil, false, err
	}
	reused := false
	if existing != nil && existing.ID > 0 {
		reused = replay.Resolve(sc, existing)
	}
	sc.CreatedAt = s.now().UTC()
	id, err := s.scs.CreateScenario(sc)
	if err != nil {
		return nil, false, fmt.Errorf("create scenario: %w", err)
	}
	sc.ID = id
	return sc, reused, nil
}

// AppendMessage 追加一条消息：逻辑时钟（id）由 SQLite AUTOINCREMENT 原子分配，
// 消除并发追加时的 MAX+1 竞态。每条成功追加都获得唯一的逻辑时钟与记录；
// 同 (scenario_id, msg_id) 幂等重试由 store 透传 ErrDuplicateMessage，不再静默丢弃。
func (s *Service) AppendMessage(scenarioID int64, msg model.Message) (*model.Message, error) {
	sc, err := s.scs.GetScenario(scenarioID)
	if err != nil {
		return nil, err
	}
	if sc.Status != model.ScenarioEditing {
		return nil, fmt.Errorf("%w: scenario %d is %s", model.ErrScenarioNotEditable, scenarioID, sc.Status)
	}
	msg.ScenarioID = scenarioID
	msg.Status = model.MsgPending
	msg.ContentHash = versioning.ContentHash(msg.Kind, msg.Key, msg.Version, msg.ReplicaID)
	msg.CreatedAt = s.now().UTC()
	if err := s.scs.InsertMessage(&msg); err != nil {
		return nil, fmt.Errorf("insert message: %w", err)
	}
	return &msg, nil
}

// AppendMessages 批量追加（按给定顺序分配递增逻辑时钟）。
func (s *Service) AppendMessages(scenarioID int64, msgs []model.Message) ([]model.Message, error) {
	out := make([]model.Message, 0, len(msgs))
	for _, m := range msgs {
		appended, err := s.AppendMessage(scenarioID, m)
		if err != nil {
			return out, err
		}
		out = append(out, *appended)
	}
	return out, nil
}

// ListScenarios 列出全部场景。
func (s *Service) ListScenarios() ([]model.Scenario, error) {
	return s.scs.ListScenarios()
}

// RunReplay 运行回放（从游标续跑）并返回结果。
func (s *Service) RunReplay(scenarioID int64) (*replay.Result, error) {
	return s.Replay.Replay(scenarioID)
}

// Stats 汇总统计。
func (s *Service) Stats() (*model.Stats, error) {
	st := &model.Stats{}
	var err error
	if st.Keyspaces, err = s.ks.KeyspaceCount(); err != nil {
		return nil, err
	}
	if st.Replicas, err = s.ks.ReplicaCount(); err != nil {
		return nil, err
	}
	if st.SourceVersions, err = s.vs.SourceVersionCount(); err != nil {
		return nil, err
	}
	if st.Scenarios, err = s.scs.ScenarioCount(); err != nil {
		return nil, err
	}
	if st.Messages, err = s.scs.MessageCount(); err != nil {
		return nil, err
	}
	if st.Violations, err = s.scs.ViolationCount(); err != nil {
		return nil, err
	}
	if st.Proofs, err = s.scs.ProofCount(); err != nil {
		return nil, err
	}
	if st.Specs, err = s.sps.SpecCount(); err != nil {
		return nil, err
	}
	return st, nil
}
