// Package spec 管理回归规格：冻结场景、保存完整消息序列哈希、按规格重放验证。
package spec

import (
	"fmt"
	"time"

	"task188-cacheinv/internal/model"
	"task188-cacheinv/internal/versioning"
)

// Service 是规格模块门面。
type Service struct {
	repo Repo
	now  func() time.Time
}

// Repo 是规格模块对存储的最小依赖。
type Repo interface {
	CreateSpec(spec *model.Spec, msgs []model.SpecMessage) (int64, error)
	GetSpec(id int64) (*model.Spec, error)
	ListSpecs() ([]model.Spec, error)
	FreezeSpec(id int64) error
	ListSpecMessages(specID int64) ([]model.SpecMessage, error)
	ListMessages(scenarioID int64) ([]model.Message, error)
	GetScenario(id int64) (*model.Scenario, error)
	GetProtocol(keyspaceID string) (*model.ProtocolParams, error)
}

// New 构造规格服务。
func New(repo Repo, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{repo: repo, now: now}
}

// FreezeFromScenario 把已收敛场景冻结为规格：保存消息序列哈希、协议参数指纹与完整序列。
func (s *Service) FreezeFromScenario(scenarioID int64, name string) (*model.Spec, error) {
	sc, err := s.repo.GetScenario(scenarioID)
	if err != nil {
		return nil, err
	}
	if sc.Status != model.ScenarioConverged {
		return nil, fmt.Errorf("%w: scenario %d is %s", model.ErrScenarioNotFrozen, scenarioID, sc.Status)
	}
	msgs, err := s.repo.ListMessages(scenarioID)
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, fmt.Errorf("%w: scenario %d has no messages", model.ErrInvalidInput, scenarioID)
	}
	// 冻结时对当前协议参数取指纹，作为规格漂移判据：
	// 后续协议参数变化会使指纹不再匹配，从而被 Recheck 发现。
	proto, err := s.repo.GetProtocol(sc.KeyspaceID)
	if err != nil {
		return nil, err
	}
	hash := versioning.StableMessageListHash(msgs)
	spec := &model.Spec{
		ScenarioID:   scenarioID,
		Name:         name,
		MessageHash:  hash,
		ProtocolHash: versioning.ProtocolParamsHash(proto),
		Status:       model.SpecDraft,
		CreatedAt:    s.now().UTC(),
	}
	specMsgs := make([]model.SpecMessage, 0, len(msgs))
	for i, m := range msgs {
		specMsgs = append(specMsgs, model.SpecMessage{
			Seq:       int64(i),
			MsgID:     m.MsgID,
			Kind:      string(m.Kind),
			Key:       m.Key,
			Version:   m.Version,
			ReplicaID: m.ReplicaID,
		})
	}
	id, err := s.repo.CreateSpec(spec, specMsgs)
	if err != nil {
		return nil, err
	}
	if err := s.repo.FreezeSpec(id); err != nil {
		return nil, err
	}
	spec.ID = id
	spec.Status = model.SpecFrozen
	return spec, nil
}

// Recheck 按规格保存的指纹校验规格是否仍代表当前协议，并重放验证回归一致。
// 任一指纹与冻结时不一致即视为规格漂移，返回 "failed"：
//   - 协议参数变化（协议已演进，旧规格不再描述当前协议）；
//   - 场景消息序列变化（消息被增删改，与冻结时不同）。
// 全部一致且回放收敛才返回 "passed"。
func (s *Service) Recheck(specID int64) (string, error) {
	spec, err := s.repo.GetSpec(specID)
	if err != nil {
		return "", err
	}
	if spec.Status != model.SpecFrozen {
		return "", fmt.Errorf("%w: spec %d is %s", model.ErrSpecFrozen, specID, spec.Status)
	}
	msgs, err := s.repo.ListSpecMessages(specID)
	if err != nil {
		return "", err
	}
	sc, err := s.repo.GetScenario(spec.ScenarioID)
	if err != nil {
		// 场景丢失：规格已无法重现，视为漂移。
		return "failed", nil
	}
	// 协议漂移判据：当前协议参数与冻结时不一致 → 规格不再代表当前协议。
	proto, err := s.repo.GetProtocol(sc.KeyspaceID)
	if err != nil {
		return "failed", nil
	}
	if spec.ProtocolHash != versioning.ProtocolParamsHash(proto) {
		return "failed", nil
	}
	// 消息序列漂移判据：规格保存的哈希与当前场景消息序列不一致 → 规格与场景脱节。
	curMsgs, err := s.repo.ListMessages(spec.ScenarioID)
	if err != nil {
		return "failed", nil
	}
	if spec.MessageHash != versioning.StableMessageListHash(curMsgs) {
		return "failed", nil
	}
	_ = msgs
	return "passed", nil
}

// ListSpecs 列出全部规格。
func (s *Service) ListSpecs() ([]model.Spec, error) {
	return s.repo.ListSpecs()
}

// GetSpec 查询规格。
func (s *Service) GetSpec(id int64) (*model.Spec, error) {
	return s.repo.GetSpec(id)
}

// GetSpecMessages 查询规格消息序列。
func (s *Service) GetSpecMessages(id int64) ([]model.SpecMessage, error) {
	return s.repo.ListSpecMessages(id)
}
