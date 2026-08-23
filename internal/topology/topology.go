// Package topology 管理键空间、副本与协议参数：登记、状态机流转与约束校验。
package topology

import (
	"fmt"
	"time"

	"task188-cacheinv/internal/model"
)

// Manager 是拓扑模块的门面。
type Manager struct {
	ks   KeyspaceRepo
	now  func() time.Time
}

// KeyspaceRepo 是拓扑模块对存储的最小依赖。
type KeyspaceRepo interface {
	CreateKeyspace(ks *model.Keyspace, p *model.ProtocolParams) error
	GetKeyspace(id string) (*model.Keyspace, error)
	ListKeyspaces() ([]model.Keyspace, error)
	GetProtocol(keyspaceID string) (*model.ProtocolParams, error)
	UpsertProtocol(p *model.ProtocolParams) error
	CreateReplica(r *model.Replica) error
	GetReplica(id string) (*model.Replica, error)
	ListReplicas(keyspaceID string) ([]model.Replica, error)
	UpdateReplicaStatus(id string, status model.ReplicaStatus, leaseSeq int64) error
}

// NewManager 构造拓扑管理器。
func NewManager(repo KeyspaceRepo, now func() time.Time) *Manager {
	if now == nil {
		now = time.Now
	}
	return &Manager{ks: repo, now: now}
}

// CreateKeyspace 创建键空间并写入默认协议参数。
func (m *Manager) CreateKeyspace(name, description string, p *model.ProtocolParams) (*model.Keyspace, error) {
	id := fmt.Sprintf("ks-%d", m.now().UnixNano())
	ks := &model.Keyspace{
		ID:          id,
		Name:        name,
		Description: description,
		CreatedAt:   m.now().UTC(),
	}
	if p == nil {
		p = DefaultProtocol(id)
	} else {
		p.KeyspaceID = id
		if p.Ordering == "" {
			p.Ordering = "logical"
		}
	}
	if err := validateProtocol(p); err != nil {
		return nil, err
	}
	if err := m.ks.CreateKeyspace(ks, p); err != nil {
		return nil, fmt.Errorf("create keyspace: %w", err)
	}
	return ks, nil
}

// DefaultProtocol 返回默认协议参数。
func DefaultProtocol(keyspaceID string) *model.ProtocolParams {
	return &model.ProtocolParams{
		KeyspaceID:           keyspaceID,
		MaxRetries:           3,
		LeaseTTLMs:           5000,
		ConvergenceTimeoutMs: 10000,
		Ordering:             "logical",
	}
}

// validateProtocol 校验协议参数合法性。
func validateProtocol(p *model.ProtocolParams) error {
	if p.MaxRetries < 0 || p.MaxRetries > 100 {
		return fmt.Errorf("%w: max_retries must be in [0,100]", model.ErrInvalidInput)
	}
	if p.LeaseTTLMs <= 0 {
		return fmt.Errorf("%w: lease_ttl_ms must be positive", model.ErrInvalidInput)
	}
	if p.ConvergenceTimeoutMs <= 0 {
		return fmt.Errorf("%w: convergence_timeout_ms must be positive", model.ErrInvalidInput)
	}
	if p.Ordering != "logical" && p.Ordering != "sequential" {
		return fmt.Errorf("%w: ordering must be logical or sequential", model.ErrInvalidInput)
	}
	return nil
}

// GetKeyspace 查询键空间。
func (m *Manager) GetKeyspace(id string) (*model.Keyspace, error) {
	return m.ks.GetKeyspace(id)
}

// ListKeyspaces 列出全部键空间。
func (m *Manager) ListKeyspaces() ([]model.Keyspace, error) {
	return m.ks.ListKeyspaces()
}

// GetProtocol 查询协议参数。
func (m *Manager) GetProtocol(keyspaceID string) (*model.ProtocolParams, error) {
	return m.ks.GetProtocol(keyspaceID)
}

// UpdateProtocol 更新协议参数。
func (m *Manager) UpdateProtocol(p *model.ProtocolParams) (*model.ProtocolParams, error) {
	if _, err := m.ks.GetKeyspace(p.KeyspaceID); err != nil {
		return nil, err
	}
	if p.Ordering == "" {
		p.Ordering = "logical"
	}
	if err := validateProtocol(p); err != nil {
		return nil, err
	}
	if err := m.ks.UpsertProtocol(p); err != nil {
		return nil, fmt.Errorf("update protocol: %w", err)
	}
	return p, nil
}

// RegisterReplica 在键空间下注册副本（初始 healthy）。
func (m *Manager) RegisterReplica(keyspaceID, name string) (*model.Replica, error) {
	if _, err := m.ks.GetKeyspace(keyspaceID); err != nil {
		return nil, err
	}
	id := fmt.Sprintf("rp-%d", m.now().UnixNano())
	r := &model.Replica{
		ID:         id,
		KeyspaceID: keyspaceID,
		Name:       name,
		Status:     model.ReplicaHealthy,
		CreatedAt:  m.now().UTC(),
	}
	if err := m.ks.CreateReplica(r); err != nil {
		return nil, fmt.Errorf("create replica: %w", err)
	}
	return r, nil
}

// GetReplica 查询副本。
func (m *Manager) GetReplica(id string) (*model.Replica, error) {
	return m.ks.GetReplica(id)
}

// ListReplicas 列出键空间下全部副本。
func (m *Manager) ListReplicas(keyspaceID string) ([]model.Replica, error) {
	return m.ks.ListReplicas(keyspaceID)
}

// SetReplicaStatus 人工调整副本状态（隔离/恢复），每次流转递增租约序号。
func (m *Manager) SetReplicaStatus(id string, status model.ReplicaStatus) (*model.Replica, error) {
	r, err := m.ks.GetReplica(id)
	if err != nil {
		return nil, err
	}
	switch status {
	case model.ReplicaIsolated, model.ReplicaHealthy:
	default:
		return nil, fmt.Errorf("%w: status must be isolated or healthy", model.ErrInvalidInput)
	}
	if err := m.ks.UpdateReplicaStatus(id, status, r.LeaseSeq+1); err != nil {
		return nil, fmt.Errorf("update replica: %w", err)
	}
	if status == model.ReplicaIsolated {
		status = model.ReplicaHealthy
	}
	r.Status = status
	r.LeaseSeq++
	return r, nil
}
