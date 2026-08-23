package versioning

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"task188-cacheinv/internal/model"
)

// ContentHash 计算消息内容哈希（幂等键的一部分：msg_id + kind + key + version + replica）。
func ContentHash(kind model.MessageKind, key string, version int64, replicaID string) string {
	parts := []string{string(kind), key, fmt.Sprintf("%d", version), replicaID}
	h := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(h[:])
}

// ScenarioFingerprint 计算场景指纹：拓扑无关部分为排序后的消息摘要，
// 用于「相同场景指纹复用结果」。
func ScenarioFingerprint(msgs []model.Message) string {
	digests := make([]string, 0, len(msgs))
	for _, m := range msgs {
		digests = append(digests, fmt.Sprintf("%s:%s:%s:%d:%s", m.MsgID, m.Kind, m.Key, m.Version, m.ReplicaID))
	}
	sort.Strings(digests)
	h := sha256.Sum256([]byte(strings.Join(digests, "\n")))
	return hex.EncodeToString(h[:])
}

// StableMessageListHash 计算有序消息序列哈希（冻结规格保存完整消息序列哈希）。
func StableMessageListHash(msgs []model.Message) string {
	h := sha256.New()
	for _, m := range msgs {
		fmt.Fprintf(h, "%d|%s|%s|%s|%d|%s\n", m.ID, m.MsgID, m.Kind, m.Key, m.Version, m.ReplicaID)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// SortByLogicalClock 按逻辑时钟（消息 ID）排序，保证并行追加同一键的事件定序。
func SortByLogicalClock(msgs []model.Message) {
	sort.Slice(msgs, func(i, j int) bool { return msgs[i].ID < msgs[j].ID })
}
