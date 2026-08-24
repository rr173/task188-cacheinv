package model

import "errors"

// 领域错误集合：HTTP 层与 service 层按这些哨兵错误映射 4xx/5xx。
var (
	// ErrNotFound 表示资源不存在（副本/键空间/场景/规格）。
	ErrNotFound = errors.New("resource not found")

	// ErrConflict 表示状态冲突（如重复创建、规格已冻结）。
	ErrConflict = errors.New("resource conflict")

	// ErrVersionRegression 表示源更新版本倒退（新版本 <= 当前源版本）。
	ErrVersionRegression = errors.New("version regression rejected")

	// ErrMessageContentMismatch 表示同一消息 ID 携带不同内容（幂等键冲突）。
	ErrMessageContentMismatch = errors.New("message id reused with different content")

	// ErrDuplicateMessage 表示同 (scenario_id, msg_id) 的消息已存在（幂等重试）。
	// 与 ErrMessageContentMismatch 区分：内容一致的真重复，而非内容冲突。
	ErrDuplicateMessage = errors.New("duplicate message")

	// ErrAckWithoutLease 表示副本在未持有租约的情况下确认版本。
	ErrAckWithoutLease = errors.New("ack without lease rejected")

	// ErrUnknownReplicaAck 表示确认来自未知副本。
	ErrUnknownReplicaAck = errors.New("ack from unknown replica rejected")

	// ErrUnboundedRetry 表示重试次数超过协议上限。
	ErrUnboundedRetry = errors.New("unbounded retry rejected")

	// ErrInvalidInput 表示请求体校验失败。
	ErrInvalidInput = errors.New("invalid input")

	// ErrSpecFrozen 表示规格已冻结不可修改。
	ErrSpecFrozen = errors.New("spec is frozen")

	// ErrScenarioNotEditable 表示场景已离开编辑状态，不能再追加消息。
	ErrScenarioNotEditable = errors.New("scenario not editable")

	// ErrScenarioNotFrozen 表示场景尚未收敛，不能冻结规格。
	ErrScenarioNotFrozen = errors.New("scenario not converged")

	// ErrReplayInProgress 表示回放正在进行中。
	ErrReplayInProgress = errors.New("replay in progress")
)

// ErrorKind 用于日志与审计分类。
func ErrorKind(err error) string {
	switch {
	case errors.Is(err, ErrNotFound):
		return "not_found"
	case errors.Is(err, ErrConflict):
		return "conflict"
	case errors.Is(err, ErrVersionRegression):
		return "version_regression"
	case errors.Is(err, ErrMessageContentMismatch):
		return "message_content_mismatch"
	case errors.Is(err, ErrDuplicateMessage):
		return "duplicate"
	case errors.Is(err, ErrAckWithoutLease):
		return "ack_without_lease"
	case errors.Is(err, ErrUnknownReplicaAck):
		return "unknown_replica_ack"
	case errors.Is(err, ErrUnboundedRetry):
		return "unbounded_retry"
	case errors.Is(err, ErrInvalidInput):
		return "invalid_input"
	default:
		return "internal"
	}
}
