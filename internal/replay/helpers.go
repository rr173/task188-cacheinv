package replay

import (
	"fmt"
	"sort"

	"task188-cacheinv/internal/model"
	"task188-cacheinv/internal/versioning"
)

// Resolve 基于场景指纹复用已收敛的结果：若存在同指纹且已收敛/已违反的场景，
// 返回其状态与证明，避免重复回放。
func Resolve(sc *model.Scenario, sameFingerprint *model.Scenario) bool {
	if sameFingerprint == nil {
		return false
	}
	if sc.ID == sameFingerprint.ID {
		return false
	}
	switch sameFingerprint.Status {
	case model.ScenarioConverged, model.ScenarioMonotonicityViolated:
		return true
	default:
		return false
	}
}

// BuildScenario 由键空间、协议与消息列表组装场景（编辑态），
// 并计算指纹（拓扑无关部分复用 versioning.ScenarioFingerprint）。
func BuildScenario(keyspaceID, name string, msgs []model.Message) *model.Scenario {
	fp := versioning.ScenarioFingerprint(msgs)
	return &model.Scenario{
		KeyspaceID:  keyspaceID,
		Name:        name,
		Fingerprint: fp,
		Status:      model.ScenarioEditing,
		CursorPos:   0,
	}
}

// DescribeResult 将回放结果格式化为人类可读摘要。
func DescribeResult(r *Result) string {
	switch r.Status {
	case model.ScenarioConverged:
		return fmt.Sprintf("converged after processing %d messages", r.Processed)
	case model.ScenarioMonotonicityViolated:
		return fmt.Sprintf("monotonicity violated (%d violations)", len(r.Violations))
	case model.ScenarioTimeoutNotConverged:
		ids := r.Unconverged
		sort.Strings(ids)
		return fmt.Sprintf("timeout, unconverged replicas: %v", ids)
	default:
		return string(r.Status)
	}
}

// MergeUnconverged 去重合并未收敛副本集合。
func MergeUnconverged(a, b []string) []string {
	set := make(map[string]bool)
	for _, id := range append(append([]string{}, a...), b...) {
		set[id] = true
	}
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
