package topology

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"task188-cacheinv/internal/model"
)

// FingerprintKeyspace 计算键空间拓扑指纹（副本列表 + 协议参数）。
// 相同指纹意味着相同的拓扑与协议，可用于场景指纹复用的前置判定。
func FingerprintKeyspace(replicas []model.Replica, p *model.ProtocolParams) string {
	ids := make([]string, 0, len(replicas))
	for _, r := range replicas {
		ids = append(ids, r.ID)
	}
	sort.Strings(ids)
	parts := []string{
		fmt.Sprintf("retries=%d", p.MaxRetries),
		fmt.Sprintf("lease=%d", p.LeaseTTLMs),
		fmt.Sprintf("timeout=%d", p.ConvergenceTimeoutMs),
		fmt.Sprintf("ordering=%s", p.Ordering),
		"replicas=" + strings.Join(ids, ","),
	}
	h := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(h[:])
}

// ReplicaAllowed 判断副本是否允许参与收敛判定（isolated 除外）。
func ReplicaAllowed(status model.ReplicaStatus) bool {
	return status != model.ReplicaIsolated
}

// ReplicaActive 判断副本是否处于可接收消息的状态。
func ReplicaActive(status model.ReplicaStatus) bool {
	switch status {
	case model.ReplicaHealthy, model.ReplicaLagging, model.ReplicaConverged:
		return true
	default:
		return false
	}
}

// NormalizeKey 归一化键名（非空且不超长）。
func NormalizeKey(key string) (string, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", fmt.Errorf("%w: key must not be empty", model.ErrInvalidInput)
	}
	if len(key) > 256 {
		return "", fmt.Errorf("%w: key too long", model.ErrInvalidInput)
	}
	return key, nil
}

// NormalizeReplicaID 归一化副本 ID（非空）。
func NormalizeReplicaID(id string) (string, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return "", fmt.Errorf("%w: replica_id must not be empty", model.ErrInvalidInput)
	}
	return id, nil
}
