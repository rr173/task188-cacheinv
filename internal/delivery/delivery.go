// Package delivery 实现消息投递语义：租约授予、确认校验、重试计数与幂等识别。
package delivery

import (
	"fmt"
	"time"

	"task188-cacheinv/internal/model"
)

// Lease 表示副本对某键的一份租约。
type Lease struct {
	ReplicaID string    `json:"replica_id"`
	Key       string    `json:"key"`
	Version   int64     `json:"version"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Valid 判断租约在给定时刻是否仍有效。
func (l Lease) Valid(at time.Time) bool {
	return at.Before(l.ExpiresAt)
}

// Manager 是投递模块的门面，负责租约簿记与确认校验（内存态+持久化回放重建）。
type Manager struct {
	leases map[string]Lease // key: replicaID+"\x00"+key
	now    func() time.Time
}

// NewManager 构造投递管理器。
func NewManager(now func() time.Time) *Manager {
	if now == nil {
		now = time.Now
	}
	return &Manager{leases: make(map[string]Lease), now: now}
}

func leaseKey(replicaID, key string) string { return replicaID + "\x00" + key }

// GrantLease 授予副本对某键某版本的租约；任何时刻同键仅一份租约（新租约替换旧租约）。
func (m *Manager) GrantLease(replicaID, key string, version int64, ttl time.Duration) Lease {
	l := Lease{
		ReplicaID: replicaID,
		Key:       key,
		Version:   version,
		ExpiresAt: m.now().Add(ttl),
	}
	m.leases[leaseKey(replicaID, key)] = l
	return l
}

// ReleaseLease 释放副本对某键的租约（确认完成后调用）。
func (m *Manager) ReleaseLease(replicaID, key string) {
	delete(m.leases, leaseKey(replicaID, key))
}

// GetLease 查询副本对某键的租约；不存在返回 nil。
func (m *Manager) GetLease(replicaID, key string) *Lease {
	l, ok := m.leases[leaseKey(replicaID, key)]
	if !ok {
		return nil
	}
	return &l
}

// AckState 描述一次确认的结果。
type AckState struct {
	// Kind: ok | no_lease | version_mismatch | unknown_replica | duplicate
	Kind    string
	Message string
}

// ValidateAck 校验副本确认：
//   - 未知副本 → ErrUnknownReplicaAck；
//   - 无租约 → ErrAckWithoutLease；
//   - 确认版本与租约版本不符 → 返回 mismatch（violation 而非硬错误）。
func (m *Manager) ValidateAck(replicaID, key string, ackVersion int64, replicaExists bool) (*AckState, error) {
	if !replicaExists {
		return &AckState{Kind: "unknown_replica", Message: fmt.Sprintf("ack from unknown replica %s", replicaID)}, model.ErrUnknownReplicaAck
	}
	l := m.GetLease(replicaID, key)
	if l == nil {
		return &AckState{Kind: "no_lease", Message: fmt.Sprintf("ack for %s/%s without lease", replicaID, key)}, model.ErrAckWithoutLease
	}
	if !l.Valid(m.now()) {
		return &AckState{Kind: "no_lease", Message: fmt.Sprintf("lease for %s/%s expired", replicaID, key)}, model.ErrAckWithoutLease
	}
	if ackVersion != l.Version {
		return &AckState{Kind: "version_mismatch", Message: fmt.Sprintf(
			"ack version %d != lease version %d for %s/%s", ackVersion, l.Version, replicaID, key)}, nil
	}
	return &AckState{Kind: "ok", Message: "ack accepted"}, nil
}

// RetryDecision 判断重试是否允许。
type RetryDecision struct {
	Allowed bool
	Reason  string
}

// DecideRetry 依据协议 max_retries 判断重试是否允许（拒绝无界重试）。
func DecideRetry(retryCount, maxRetries int) RetryDecision {
	if maxRetries < 0 {
		return RetryDecision{Allowed: false, Reason: "protocol max_retries misconfigured"}
	}
	if retryCount >= maxRetries {
		return RetryDecision{Allowed: false, Reason: fmt.Sprintf("retry %d exceeds max %d", retryCount, maxRetries)}
	}
	return RetryDecision{Allowed: true, Reason: ""}
}

// DuplicateCheck 幂等判断：同 msg_id 再次出现。
func DuplicateCheck(seen map[string]model.Message, m model.Message) bool {
	prev, ok := seen[m.MsgID]
	if !ok {
		return false
	}
	// 内容一致 → duplicate；内容不一致 → ErrMessageContentMismatch（调用方处理）。
	return prev.ContentHash == m.ContentHash
}
