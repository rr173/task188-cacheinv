package proof

import (
	"fmt"
	"sort"

	"task188-cacheinv/internal/model"
)

// LocateViolationSteps 定位违反步骤：返回按 step_seq 升序的违反清单。
// 与 ListViolations 等价但返回字符串形式，供 HTTP 层直接展示。
func LocateViolationSteps(viols []model.Violation) []string {
	sort.Slice(viols, func(i, j int) bool { return viols[i].StepSeq < viols[j].StepSeq })
	out := make([]string, 0, len(viols))
	for _, v := range viols {
		out = append(out, fmt.Sprintf("step %d [%s] %s", v.StepSeq, v.Kind, v.Message))
	}
	return out
}

// SummarizeViolation 汇总违反类型计数。
func SummarizeViolation(viols []model.Violation) map[string]int {
	counts := make(map[string]int)
	for _, v := range viols {
		counts[v.Kind]++
	}
	return counts
}

// CheckMonotonicObservations 静态审计消息序列：模拟"仅观察"的版本推进，
// 检查是否存在会回滚的乱序失效（纯静态预检，不依赖回放）。
// 返回违反描述列表。
func CheckMonotonicObservations(msgs []model.Message) []string {
	type state struct {
		version int64
		status  model.KeyVersionStatus
	}
	byReplicaKey := make(map[string]*state)
	var problems []string
	for _, m := range msgs {
		key := m.ReplicaID + "\x00" + m.Key
		st, ok := byReplicaKey[key]
		if !ok {
			st = &state{version: 0, status: model.KeyUnknown}
			byReplicaKey[key] = st
		}
		switch m.Kind {
		case model.MsgUpdate, model.MsgInvalidate:
			if m.Version < st.version {
				problems = append(problems, fmt.Sprintf(
					"msg %s (%s key=%s): version %d would regress observed %d",
					m.MsgID, m.Kind, m.Key, m.Version, st.version))
				continue
			}
			st.version = m.Version
			if m.Kind == model.MsgUpdate {
				st.status = model.KeyValid
			} else {
				st.status = model.KeyStale
			}
		case model.MsgAck:
			// ACK 不推进版本。
		}
	}
	return problems
}
