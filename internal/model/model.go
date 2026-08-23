// Package model 定义分布式缓存失效收敛验证服务的领域实体与错误。
package model

import "time"

// Keyspace 表示一个可验证的键空间（key space）：一组副本共享同一组键。
type Keyspace struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"created_at"`
}

// ReplicaStatus 是副本生命周期状态机：
//   - Healthy：健康，正常接收消息；
//   - Lagging：滞后（尚未追平源版本）；
//   - Isolated：隔离（人工标记，暂停参与收敛判定）；
//   - Converged：已收敛（全部键追平源端）。
type ReplicaStatus string

const (
	ReplicaHealthy   ReplicaStatus = "healthy"
	ReplicaLagging   ReplicaStatus = "lagging"
	ReplicaIsolated  ReplicaStatus = "isolated"
	ReplicaConverged ReplicaStatus = "converged"
)

// Replica 是键空间内的一个缓存副本。
type Replica struct {
	ID          string        `json:"id"`
	KeyspaceID  string        `json:"keyspace_id"`
	Name        string        `json:"name"`
	Status      ReplicaStatus `json:"status"`
	LeaseSeq    int64         `json:"lease_seq"` // 单调递增的租约序号
	CreatedAt   time.Time     `json:"created_at"`
}

// ProtocolParams 描述失效协议参数。
type ProtocolParams struct {
	KeyspaceID         string `json:"keyspace_id"`
	MaxRetries         int    `json:"max_retries"`          // 单键单版本最大重试次数
	LeaseTTLMs         int64  `json:"lease_ttl_ms"`         // 租约有效期
	ConvergenceTimeoutMs int64 `json:"convergence_timeout_ms"` // 收敛超时
	Ordering           string `json:"ordering"`             // logical|sequential
}

// KeyVersionStatus 是副本侧键版本状态机：
//   - Unknown：未观察到该键；
//   - Valid：持有有效版本；
//   - PendingInvalidation：已收到失效意向但尚未确认；
//   - Stale：已确认失效，等待新版本；
//   - Superseded：已被更高版本替代。
type KeyVersionStatus string

const (
	KeyUnknown            KeyVersionStatus = "unknown"
	KeyValid              KeyVersionStatus = "valid"
	KeyPendingInvalidation KeyVersionStatus = "pending_invalidation"
	KeyStale              KeyVersionStatus = "stale"
	KeySuperseded         KeyVersionStatus = "superseded"
)

// ReplicaKeyVersion 是某个副本对某个键的观察版本快照。
type ReplicaKeyVersion struct {
	ReplicaID string          `json:"replica_id"`
	Key       string          `json:"key"`
	Version   int64           `json:"version"`
	Status    KeyVersionStatus `json:"status"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// SourceVersion 是源端最新版本（唯一事实来源）。
type SourceVersion struct {
	KeyspaceID string    `json:"keyspace_id"`
	Key        string    `json:"key"`
	Version    int64     `json:"version"`
	ValueHash  string    `json:"value_hash"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// MessageKind 是消息类型。
type MessageKind string

const (
	MsgInvalidate MessageKind = "invalidate" // 失效消息：携带目标版本
	MsgLease      MessageKind = "lease"      // 租约消息：授予某副本租约
	MsgAck        MessageKind = "ack"        // 确认消息：副本确认已应用某版本
	MsgRetry      MessageKind = "retry"      // 重试消息：驱动重投
	MsgUpdate     MessageKind = "update"     // 源更新广播：直接下发新版本
)

// MessageStatus 是消息投递状态机：
//   - Pending：待投递；
//   - Delivered：已投递；
//   - Duplicate：重复（幂等命中）；
//   - Expired：过期（租约或重试耗尽）；
//   - Rejected：被拒绝（违反协议约束）。
type MessageStatus string

const (
	MsgPending   MessageStatus = "pending"
	MsgDelivered MessageStatus = "delivered"
	MsgDuplicate MessageStatus = "duplicate"
	MsgExpired   MessageStatus = "expired"
	MsgRejected  MessageStatus = "rejected"
)

// Message 是投递序列中的一条消息。
type Message struct {
	ID          int64         `json:"id"` // 逻辑时钟分配的单调序号
	ScenarioID  int64         `json:"scenario_id"`
	MsgID       string        `json:"msg_id"` // 业务消息 ID（幂等键）
	Kind        MessageKind   `json:"kind"`
	Key         string        `json:"key"`
	Version     int64         `json:"version"`
	ReplicaID   string        `json:"replica_id"`
	ContentHash string        `json:"content_hash"`
	Status      MessageStatus `json:"status"`
	RetryCount  int           `json:"retry_count"`
	CreatedAt   time.Time     `json:"created_at"`
}

// ScenarioStatus 是场景状态机：
//   - Editing：编辑中（可追加消息）；
//   - Replaying：回放中（从游标续跑）；
//   - Converged：收敛（证明通过）；
//   - MonotonicityViolated：违反单调性（含幂等/租约/未知副本等违规）；
//   - TimeoutNotConverged：超时未收敛。
type ScenarioStatus string

const (
	ScenarioEditing            ScenarioStatus = "editing"
	ScenarioReplaying          ScenarioStatus = "replaying"
	ScenarioConverged          ScenarioStatus = "converged"
	ScenarioMonotonicityViolated ScenarioStatus = "monotonicity_violated"
	ScenarioTimeoutNotConverged ScenarioStatus = "timeout_not_converged"
)

// Scenario 是一次协议验证场景。
type Scenario struct {
	ID          int64          `json:"id"`
	KeyspaceID  string         `json:"keyspace_id"`
	Name        string         `json:"name"`
	Fingerprint string         `json:"fingerprint"`
	Status      ScenarioStatus `json:"status"`
	CursorPos   int            `json:"cursor_pos"` // 回放游标：已处理消息数
	CreatedAt   time.Time      `json:"created_at"`
}

// ReplayEvent 是回放轨迹中的一步（断点/检查点，用于续跑与违反定位）。
type ReplayEvent struct {
	ID          int64       `json:"id"`
	ScenarioID  int64       `json:"scenario_id"`
	Seq         int64       `json:"seq"`
	Step        string      `json:"step"`
	Detail      string      `json:"detail"`
	CreatedAt   time.Time   `json:"created_at"`
}

// Violation 是违反步骤记录。
type Violation struct {
	ID         int64     `json:"id"`
	ScenarioID int64     `json:"scenario_id"`
	StepSeq    int64     `json:"step_seq"`
	Kind       string    `json:"kind"`
	Message    string    `json:"message"`
	CreatedAt  time.Time `json:"created_at"`
}

// Proof 是收敛证明（或未收敛报告）。
type Proof struct {
	ID          int64     `json:"id"`
	ScenarioID  int64     `json:"scenario_id"`
	Conclusion  string    `json:"conclusion"` // converged|violated|timeout
	Detail      string    `json:"detail"`
	Unconverged []string  `json:"unconverged"` // 未收敛副本集合
	CreatedAt   time.Time `json:"created_at"`
}

// SpecStatus 是回归规格状态机：draft → frozen。
type SpecStatus string

const (
	SpecDraft  SpecStatus = "draft"
	SpecFrozen SpecStatus = "frozen"
)

// Spec 是冻结的回归规格。
type Spec struct {
	ID           int64      `json:"id"`
	ScenarioID   int64      `json:"scenario_id"`
	Name         string     `json:"name"`
	MessageHash  string     `json:"message_hash"`
	ProtocolHash string     `json:"protocol_hash"` // 冻结时的协议参数指纹；Recheck 比对当前协议以发现规格漂移
	Status       SpecStatus `json:"status"`
	CreatedAt    time.Time  `json:"created_at"`
}

// SpecMessage 是规格保存的完整消息序列条目。
type SpecMessage struct {
	SpecID int64  `json:"spec_id"`
	Seq    int64  `json:"seq"`
	MsgID  string `json:"msg_id"`
	Kind   string `json:"kind"`
	Key    string `json:"key"`
	Version int64 `json:"version"`
	ReplicaID string `json:"replica_id"`
}

// Stats 是自检统计。
type Stats struct {
	Keyspaces    int   `json:"keyspaces"`
	Replicas     int   `json:"replicas"`
	SourceVersions int `json:"source_versions"`
	Scenarios    int   `json:"scenarios"`
	Messages     int   `json:"messages"`
	Violations   int   `json:"violations"`
	Proofs       int   `json:"proofs"`
	Specs        int   `json:"specs"`
}
