// Package selfcheck 实现 --smoke-test 离线自检：
// 真实建库、登记拓扑与协议、提交源更新与消息序列、回放验证收敛、
// 关闭并重新打开同一数据库验证持久化恢复，最后以 0/非 0 退出。
package selfcheck

import (
	"fmt"
	"time"

	"task188-cacheinv/internal/model"
	"task188-cacheinv/internal/service"
	"task188-cacheinv/internal/store"
)

// Run 执行端到端自检。dbPath 用于重开验证。
func Run(st *store.Store, svc *service.Service, dbPath string) error {
	now := time.Now
	_ = now

	// 1. 创建键空间与副本。
	ks, err := svc.Topo.CreateKeyspace("smoke-keyspace", "smoke test keyspace", nil)
	if err != nil {
		return fmt.Errorf("create keyspace: %w", err)
	}
	rp1, err := svc.Topo.RegisterReplica(ks.ID, "replica-a")
	if err != nil {
		return fmt.Errorf("register replica a: %w", err)
	}
	rp2, err := svc.Topo.RegisterReplica(ks.ID, "replica-b")
	if err != nil {
		return fmt.Errorf("register replica b: %w", err)
	}

	// 2. 提交源更新：key=user:42 到版本 3。
	if _, err := svc.Ver.ApplySourceUpdate(ks.ID, "user:42", 3, "hash-v3"); err != nil {
		return fmt.Errorf("apply source update: %w", err)
	}
	// 版本倒退必须被拒绝。
	if _, err := svc.Ver.ApplySourceUpdate(ks.ID, "user:42", 2, "hash-v2"); err == nil {
		return fmt.Errorf("expected version regression to be rejected")
	}

	// 3. 创建场景并追加消息：乱序失效先到，再补租约+确认+更新。
	sc, reused, err := svc.CreateScenario(ks.ID, "smoke-scenario", nil)
	if err != nil {
		return fmt.Errorf("create scenario: %w", err)
	}
	if reused {
		return fmt.Errorf("unexpected fingerprint reuse for fresh scenario")
	}
	msgs := []model.Message{
		{MsgID: "inv-1", Kind: model.MsgInvalidate, Key: "user:42", Version: 1, ReplicaID: rp1.ID},
		{MsgID: "lease-1", Kind: model.MsgLease, Key: "user:42", Version: 1, ReplicaID: rp1.ID},
		{MsgID: "ack-1", Kind: model.MsgAck, Key: "user:42", Version: 1, ReplicaID: rp1.ID},
		{MsgID: "upd-1", Kind: model.MsgUpdate, Key: "user:42", Version: 3, ReplicaID: rp1.ID},
		{MsgID: "inv-2", Kind: model.MsgInvalidate, Key: "user:42", Version: 2, ReplicaID: rp2.ID},
		{MsgID: "lease-2", Kind: model.MsgLease, Key: "user:42", Version: 2, ReplicaID: rp2.ID},
		{MsgID: "ack-2", Kind: model.MsgAck, Key: "user:42", Version: 2, ReplicaID: rp2.ID},
		{MsgID: "upd-2", Kind: model.MsgUpdate, Key: "user:42", Version: 3, ReplicaID: rp2.ID},
	}
	if _, err := svc.AppendMessages(sc.ID, msgs); err != nil {
		return fmt.Errorf("append messages: %w", err)
	}

	// 4. 回放：乱序失效 inv-1 (v1) 先到 rp1，随后补齐到 v3 → 应收敛。
	res, err := svc.RunReplay(sc.ID)
	if err != nil {
		return fmt.Errorf("replay: %w", err)
	}
	if res.Status != model.ScenarioConverged {
		return fmt.Errorf("expected convergence, got %s (violations=%v unconverged=%v)",
			res.Status, res.Violations, res.Unconverged)
	}

	// 5. 生成证明并冻结规格。
	p, err := svc.Proof.Build(sc.ID)
	if err != nil {
		return fmt.Errorf("build proof: %w", err)
	}
	if p.Conclusion != "converged" {
		return fmt.Errorf("expected converged proof, got %s", p.Conclusion)
	}
	spec, err := svc.Spec.FreezeFromScenario(sc.ID, "smoke-spec")
	if err != nil {
		return fmt.Errorf("freeze spec: %w", err)
	}
	if spec.Status != model.SpecFrozen {
		return fmt.Errorf("expected frozen spec, got %s", spec.Status)
	}

	// 6. 违反场景：无租约 ACK 必须被定位。
	sc2, _, err := svc.CreateScenario(ks.ID, "smoke-violation", nil)
	if err != nil {
		return fmt.Errorf("create violation scenario: %w", err)
	}
	bad := []model.Message{
		{MsgID: "bad-ack", Kind: model.MsgAck, Key: "user:42", Version: 3, ReplicaID: rp1.ID},
	}
	if _, err := svc.AppendMessages(sc2.ID, bad); err != nil {
		return fmt.Errorf("append bad message: %w", err)
	}
	res2, err := svc.RunReplay(sc2.ID)
	if err != nil {
		return fmt.Errorf("replay violation: %w", err)
	}
	if res2.Status != model.ScenarioMonotonicityViolated {
		return fmt.Errorf("expected violation, got %s", res2.Status)
	}

	// 7. 关闭并重开数据库：验证持久化与恢复。
	if err := st.Close(); err != nil {
		return fmt.Errorf("close store: %w", err)
	}
	st2, err := store.Open(store.Options{Path: dbPath})
	if err != nil {
		return fmt.Errorf("reopen store: %w", err)
	}
	defer st2.Close()
	svc2 := service.New(st2, time.Now)
	restored, err := svc2.GetScenario(sc.ID)
	if err != nil {
		return fmt.Errorf("restore scenario: %w", err)
	}
	if restored.Status != model.ScenarioConverged {
		return fmt.Errorf("expected restored converged, got %s", restored.Status)
	}
	restoredSpec, err := svc2.GetSpec(spec.ID)
	if err != nil {
		return fmt.Errorf("restore spec: %w", err)
	}
	if restoredSpec.Status != model.SpecFrozen {
		return fmt.Errorf("expected restored frozen spec, got %s", restoredSpec.Status)
	}
	if restoredSpec.MessageHash == "" {
		return fmt.Errorf("restored spec missing message hash")
	}

	// 8. 续跑语义：用新键 user:99 构造只覆盖 rp1 的消息集，
	// 回放后 rp2 未收敛 → timeout；重开后再次回放保持 timeout 且不重复处理（断点续传）。
	if _, err := svc2.Ver.ApplySourceUpdate(ks.ID, "user:99", 1, "v99"); err != nil {
		return fmt.Errorf("apply source update user:99: %w", err)
	}
	sc3, _, err := svc2.CreateScenario(ks.ID, "smoke-resume", nil)
	if err != nil {
		return fmt.Errorf("create resume scenario: %w", err)
	}
	half := []model.Message{
		{MsgID: "r1", Kind: model.MsgUpdate, Key: "user:99", Version: 1, ReplicaID: rp1.ID},
	}
	if _, err := svc2.AppendMessages(sc3.ID, half); err != nil {
		return fmt.Errorf("append half: %w", err)
	}
	res3, err := svc2.RunReplay(sc3.ID)
	if err != nil {
		return fmt.Errorf("partial replay: %w", err)
	}
	if res3.Status != model.ScenarioTimeoutNotConverged {
		return fmt.Errorf("expected timeout not converged after partial replay, got %s (unconv=%v)",
			res3.Status, res3.Unconverged)
	}
	// 重开已在第 7 步完成（svc2 运行于重开后的库）；再次回放应幂等、不重放已处理消息。
	res3b, err := svc2.RunReplay(sc3.ID)
	if err != nil {
		return fmt.Errorf("replay resume: %w", err)
	}
	if res3b.Processed != 0 {
		return fmt.Errorf("expected zero reprocessing on resume, got %d", res3b.Processed)
	}
	if res3b.Status != model.ScenarioTimeoutNotConverged {
		return fmt.Errorf("expected stable timeout on resumed replay, got %s", res3b.Status)
	}
	return nil
}
