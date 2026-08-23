// Package replay 实现协议回放引擎：按逻辑时钟处理消息、维护租约、
// 记录断点轨迹、判定收敛或违反，并支持从游标续跑（重启恢复）。
package replay

import (
	"fmt"
	"time"

	"task188-cacheinv/internal/delivery"
	"task188-cacheinv/internal/model"
	"task188-cacheinv/internal/versioning"
)

// Engine 是回放引擎。
type Engine struct {
	repo Repo
	ver  *versioning.Service
	dlv  *delivery.Manager
	now  func() time.Time
}

// replicaSnapshot 记录某副本在回放快照中的状态：
//   - exists：副本已注册；
//   - isolated：副本被隔离，不得接收任何携带 replica_id 的消息。
type replicaSnapshot struct {
	exists   bool
	isolated bool
}

// Repo 是回放引擎对存储的最小依赖。
type Repo interface {
	GetScenario(id int64) (*model.Scenario, error)
	UpdateScenarioStatus(id int64, status model.ScenarioStatus, cursorPos int) error
	ListMessages(scenarioID int64) ([]model.Message, error)
	UpdateMessageStatus(id int64, status model.MessageStatus) error
	UpdateMessageStatusAndRetry(id int64, status model.MessageStatus, retry int) error
	AppendReplayEvent(e *model.ReplayEvent) error
	AppendViolation(v *model.Violation) error
	GetProtocol(keyspaceID string) (*model.ProtocolParams, error)
	ListReplicas(keyspaceID string) ([]model.Replica, error)
	GetReplica(id string) (*model.Replica, error)
	ListSourceVersions(keyspaceID string) ([]model.SourceVersion, error)
	GetSourceVersion(keyspaceID, key string) (*model.SourceVersion, error)
}

// Result 是一次回放的结论。
type Result struct {
	Status      model.ScenarioStatus
	Violations  []string
	Unconverged []string
	Processed   int
}

// New 构造回放引擎。
func New(repo Repo, ver *versioning.Service, dlv *delivery.Manager, now func() time.Time) *Engine {
	if now == nil {
		now = time.Now
	}
	return &Engine{repo: repo, ver: ver, dlv: dlv, now: now}
}

// Replay 从场景当前游标续跑全部消息（cursor 之前已处理并记录断点）。
// 已收敛/已违反的场景不可重复回放。
func (e *Engine) Replay(scenarioID int64) (*Result, error) {
	sc, err := e.repo.GetScenario(scenarioID)
	if err != nil {
		return nil, err
	}
	if sc.Status == model.ScenarioConverged || sc.Status == model.ScenarioMonotonicityViolated {
		return &Result{Status: sc.Status}, nil
	}
	if err := e.repo.UpdateScenarioStatus(scenarioID, model.ScenarioReplaying, sc.CursorPos); err != nil {
		return nil, err
	}
	msgs, err := e.repo.ListMessages(scenarioID)
	if err != nil {
		return nil, err
	}
	versioning.SortByLogicalClock(msgs)
	p, err := e.repo.GetProtocol(sc.KeyspaceID)
	if err != nil {
		return nil, err
	}
	replicas, err := e.repo.ListReplicas(sc.KeyspaceID)
	if err != nil {
		return nil, err
	}
	// replicaState 记录每个已知副本是否被隔离；隔离副本不得接收任何
	// 携带 replica_id 的消息（更新/失效/租约/确认/重试）。
	replicaState := make(map[string]replicaSnapshot, len(replicas))
	allowed := make([]model.Replica, 0, len(replicas))
	for _, r := range replicas {
		replicaState[r.ID] = replicaSnapshot{exists: true, isolated: r.Status == model.ReplicaIsolated}
		if r.Status != model.ReplicaIsolated {
			allowed = append(allowed, r)
		}
	}
	sources, err := e.repo.ListSourceVersions(sc.KeyspaceID)
	if err != nil {
		return nil, err
	}
	sourceByKey := make(map[string]int64, len(sources))
	for _, s := range sources {
		sourceByKey[s.Key] = s.Version
	}

	seen := make(map[string]model.Message)
	var violations []string
	seq := int64(0)

	// 先重建已处理消息（cursor 之前）的幂等表与租约快照。
	for i := 0; i < sc.CursorPos && i < len(msgs); i++ {
		m := msgs[i]
		if m.Status == model.MsgDelivered || m.Status == model.MsgDuplicate {
			seen[m.MsgID] = m
			if m.Kind == model.MsgLease {
				ttl := time.Duration(p.LeaseTTLMs) * time.Millisecond
				e.dlv.GrantLease(m.ReplicaID, m.Key, m.Version, ttl)
			}
		}
	}

	processed := 0
	for i := sc.CursorPos; i < len(msgs); i++ {
		m := msgs[i]
		seq++
		processed++
		res, v, err := e.step(sc, p, m, replicaState, seen, seq)
		if err != nil {
			return nil, err
		}
		if v != nil {
			violations = append(violations, v.Message)
			if err := e.repo.AppendViolation(v); err != nil {
				return nil, err
			}
		}
		if err := e.repo.AppendReplayEvent(&model.ReplayEvent{
			ScenarioID: scenarioID,
			Seq:        seq,
			Step:       string(m.Kind),
			Detail:     res,
			CreatedAt:  e.now().UTC(),
		}); err != nil {
			return nil, err
		}
	}
	newCursor := sc.CursorPos + processed
	if err := e.repo.UpdateScenarioStatus(scenarioID, model.ScenarioReplaying, newCursor); err != nil {
		return nil, err
	}

	// 判定收敛。
	if len(violations) > 0 {
		status := model.ScenarioMonotonicityViolated
		if err := e.repo.UpdateScenarioStatus(scenarioID, status, newCursor); err != nil {
			return nil, err
		}
		return &Result{Status: status, Violations: violations, Processed: processed}, nil
	}
	unconverged := e.computeUnconverged(allowed, sourceByKey)
	if len(unconverged) == 0 {
		if err := e.repo.UpdateScenarioStatus(scenarioID, model.ScenarioConverged, newCursor); err != nil {
			return nil, err
		}
		return &Result{Status: model.ScenarioConverged, Processed: processed}, nil
	}
	if err := e.repo.UpdateScenarioStatus(scenarioID, model.ScenarioTimeoutNotConverged, newCursor); err != nil {
		return nil, err
	}
	return &Result{Status: model.ScenarioTimeoutNotConverged, Unconverged: unconverged, Processed: processed}, nil
}

// step 处理单条消息，返回处理结果描述；violation 非 nil 表示记录了违反步骤。
func (e *Engine) step(sc *model.Scenario, p *model.ProtocolParams, m model.Message,
	states map[string]replicaSnapshot, seen map[string]model.Message, seq int64) (string, *model.Violation, error) {

	violation := func(kind, msg string) *model.Violation {
		return &model.Violation{
			ScenarioID: sc.ID,
			StepSeq:    seq,
			Kind:       kind,
			Message:    msg,
			CreatedAt:  e.now().UTC(),
		}
	}

	// 幂等：同 msg_id 内容一致 → duplicate；不一致 → 违反（内容冲突）。
	if prev, ok := seen[m.MsgID]; ok {
		if prev.ContentHash != m.ContentHash {
			_ = e.repo.UpdateMessageStatus(m.ID, model.MsgRejected)
			msg := fmt.Sprintf("msg_id %s reused with different content (hash %s vs %s)",
				m.MsgID, prev.ContentHash, m.ContentHash)
			return "rejected:content_mismatch", violation("content_mismatch", msg), nil
		}
		_ = e.repo.UpdateMessageStatus(m.ID, model.MsgDuplicate)
		return "duplicate", nil, nil
	}
	seen[m.MsgID] = m

	// checkReplica 校验消息目标副本：未知 → 违反；隔离 → 拒绝并记录违反。
	// 隔离副本不得接收任何携带 replica_id 的消息（更新/失效/租约/确认/重试）。
	checkReplica := func(verb string) (string, *model.Violation) {
		st, ok := states[m.ReplicaID]
		if !ok || !st.exists {
			_ = e.repo.UpdateMessageStatus(m.ID, model.MsgRejected)
			msg := fmt.Sprintf("%s for unknown replica %s", verb, m.ReplicaID)
			return "rejected:unknown_replica", violation("unknown_replica", msg)
		}
		if st.isolated {
			_ = e.repo.UpdateMessageStatus(m.ID, model.MsgRejected)
			msg := fmt.Sprintf("%s for isolated replica %s (not allowed to receive messages)",
				verb, m.ReplicaID)
			return "rejected:isolated_replica", violation("isolated_replica", msg)
		}
		return "", nil
	}

	switch m.Kind {
	case model.MsgLease:
		if res, v := checkReplica("lease"); v != nil {
			return res, v, nil
		}
		ttl := time.Duration(p.LeaseTTLMs) * time.Millisecond
		e.dlv.GrantLease(m.ReplicaID, m.Key, m.Version, ttl)
		_ = e.repo.UpdateMessageStatus(m.ID, model.MsgDelivered)
		return "lease_granted", nil, nil

	case model.MsgInvalidate:
		if res, v := checkReplica("invalidate"); v != nil {
			return res, v, nil
		}
		_, err := e.ver.InvalidateKey(m.ReplicaID, m.Key, m.Version)
		if err != nil {
			_ = e.repo.UpdateMessageStatus(m.ID, model.MsgRejected)
			msg := fmt.Sprintf("stale invalidation rejected: %v", err)
			return "rejected:stale_invalidation", violation("stale_invalidation", msg), nil
		}
		_ = e.repo.UpdateMessageStatus(m.ID, model.MsgDelivered)
		return "invalidated", nil, nil

	case model.MsgUpdate:
		if res, v := checkReplica("update"); v != nil {
			return res, v, nil
		}
		_, err := e.ver.ObserveVersion(m.ReplicaID, m.Key, m.Version)
		if err != nil {
			_ = e.repo.UpdateMessageStatus(m.ID, model.MsgRejected)
			msg := fmt.Sprintf("version regression: %v", err)
			return "rejected:regression", violation("version_regression", msg), nil
		}
		_ = e.repo.UpdateMessageStatus(m.ID, model.MsgDelivered)
		return "observed", nil, nil

	case model.MsgAck:
		if res, v := checkReplica("ack"); v != nil {
			return res, v, nil
		}
		state, err := e.dlv.ValidateAck(m.ReplicaID, m.Key, m.Version, true)
		if err != nil {
			_ = e.repo.UpdateMessageStatus(m.ID, model.MsgRejected)
			return "rejected:" + state.Kind, violation(state.Kind, state.Message), nil
		}
		if state.Kind == "version_mismatch" {
			_ = e.repo.UpdateMessageStatus(m.ID, model.MsgRejected)
			return "rejected:version_mismatch", violation("ack_version_mismatch", state.Message), nil
		}
		e.dlv.ReleaseLease(m.ReplicaID, m.Key)
		_, _ = e.ver.ConfirmInvalidation(m.ReplicaID, m.Key)
		_ = e.repo.UpdateMessageStatus(m.ID, model.MsgDelivered)
		return "acked", nil, nil

	case model.MsgRetry:
		if res, v := checkReplica("retry"); v != nil {
			return res, v, nil
		}
		next := m.RetryCount + 1
		dec := delivery.DecideRetry(next, p.MaxRetries)
		if !dec.Allowed {
			_ = e.repo.UpdateMessageStatusAndRetry(m.ID, model.MsgExpired, m.RetryCount)
			msg := fmt.Sprintf("unbounded retry rejected: %s", dec.Reason)
			return "expired:unbounded_retry", violation("unbounded_retry", msg), nil
		}
		ttl := time.Duration(p.LeaseTTLMs) * time.Millisecond
		e.dlv.GrantLease(m.ReplicaID, m.Key, m.Version, ttl)
		_ = e.repo.UpdateMessageStatusAndRetry(m.ID, model.MsgDelivered, next)
		return "retry_scheduled", nil, nil

	default:
		return "ignored", nil, nil
	}
}

// computeUnconverged 返回尚未追平源端的允许副本列表（隔离副本不计入）。
func (e *Engine) computeUnconverged(allowed []model.Replica, sourceByKey map[string]int64) []string {
	var unconverged []string
	for _, r := range allowed {
		for key, srcVer := range sourceByKey {
			if !e.ver.IsConvergedFor(r.ID, key, srcVer) {
				unconverged = append(unconverged, r.ID)
				break
			}
		}
	}
	return unconverged
}
