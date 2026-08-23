// Package proof 生成收敛证明、单调性审计与违反步骤定位。
package proof

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"task188-cacheinv/internal/model"
)

// Generator 生成证明。
type Generator struct {
	repo Repo
	now  func() time.Time
}

// Repo 是证明模块对存储的最小依赖。
type Repo interface {
	GetScenario(id int64) (*model.Scenario, error)
	GetProtocol(keyspaceID string) (*model.ProtocolParams, error)
	ListReplicas(keyspaceID string) ([]model.Replica, error)
	ListSourceVersions(keyspaceID string) ([]model.SourceVersion, error)
	ListReplicaKeyVersions(replicaID string) ([]model.ReplicaKeyVersion, error)
	ListReplayEvents(scenarioID int64) ([]model.ReplayEvent, error)
	ListViolations(scenarioID int64) ([]model.Violation, error)
	InsertProof(p *model.Proof) (int64, error)
}

// New 构造证明生成器。
func New(repo Repo, now func() time.Time) *Generator {
	if now == nil {
		now = time.Now
	}
	return &Generator{repo: repo, now: now}
}

// Build 依据场景当前状态生成证明并持久化。
func (g *Generator) Build(scenarioID int64) (*model.Proof, error) {
	sc, err := g.repo.GetScenario(scenarioID)
	if err != nil {
		return nil, err
	}
	switch sc.Status {
	case model.ScenarioConverged:
		return g.buildConverged(sc)
	case model.ScenarioMonotonicityViolated:
		return g.buildViolated(sc)
	case model.ScenarioTimeoutNotConverged:
		return g.buildTimeout(sc)
	default:
		return g.buildPending(sc)
	}
}

// buildConverged 生成收敛证明：逐副本逐键比对源版本。
func (g *Generator) buildConverged(sc *model.Scenario) (*model.Proof, error) {
	sources, err := g.repo.ListSourceVersions(sc.KeyspaceID)
	if err != nil {
		return nil, err
	}
	replicas, err := g.repo.ListReplicas(sc.KeyspaceID)
	if err != nil {
		return nil, err
	}
	srcByKey := make(map[string]int64, len(sources))
	for _, s := range sources {
		srcByKey[s.Key] = s.Version
	}
	var lines []string
	for _, r := range replicas {
		vers, err := g.repo.ListReplicaKeyVersions(r.ID)
		if err != nil {
			return nil, err
		}
		byKey := make(map[string]model.ReplicaKeyVersion, len(vers))
		for _, v := range vers {
			byKey[v.Key] = v
		}
		for _, key := range sortedKeys(srcByKey) {
			obs, ok := byKey[key]
			status := "missing"
			if ok && obs.Version == srcByKey[key] && (obs.Status == model.KeyValid || obs.Status == model.KeySuperseded) {
				status = "converged"
			}
			lines = append(lines, fmt.Sprintf("replica %s key %s: source=%d observed=%d %s",
				r.ID, key, srcByKey[key], obs.Version, status))
		}
	}
	p := &model.Proof{
		ScenarioID: sc.ID,
		Conclusion: "converged",
		Detail:     "all allowed replicas match source versions\n" + strings.Join(lines, "\n"),
		CreatedAt:  g.now().UTC(),
	}
	id, err := g.repo.InsertProof(p)
	if err != nil {
		return nil, err
	}
	p.ID = id
	return p, nil
}

// buildViolated 生成违反审计：列出全部违反步骤与涉及的轨迹。
func (g *Generator) buildViolated(sc *model.Scenario) (*model.Proof, error) {
	viols, err := g.repo.ListViolations(sc.ID)
	if err != nil {
		return nil, err
	}
	events, err := g.repo.ListReplayEvents(sc.ID)
	if err != nil {
		return nil, err
	}
	var lines []string
	for _, v := range viols {
		lines = append(lines, fmt.Sprintf("step %d [%s] %s", v.StepSeq, v.Kind, v.Message))
	}
	for _, e := range events {
		lines = append(lines, fmt.Sprintf("trace step %d: %s %s", e.Seq, e.Step, e.Detail))
	}
	p := &model.Proof{
		ScenarioID: sc.ID,
		Conclusion: "violated",
		Detail:     fmt.Sprintf("%d violation(s)\n%s", len(viols), strings.Join(lines, "\n")),
		CreatedAt:  g.now().UTC(),
	}
	id, err := g.repo.InsertProof(p)
	if err != nil {
		return nil, err
	}
	p.ID = id
	return p, nil
}

// buildTimeout 生成超时未收敛报告：返回未收敛副本集合。
func (g *Generator) buildTimeout(sc *model.Scenario) (*model.Proof, error) {
	sources, err := g.repo.ListSourceVersions(sc.KeyspaceID)
	if err != nil {
		return nil, err
	}
	replicas, err := g.repo.ListReplicas(sc.KeyspaceID)
	if err != nil {
		return nil, err
	}
	srcByKey := make(map[string]int64, len(sources))
	for _, s := range sources {
		srcByKey[s.Key] = s.Version
	}
	var unconverged []string
	var lines []string
	for _, r := range replicas {
		if r.Status == model.ReplicaIsolated {
			continue
		}
		vers, err := g.repo.ListReplicaKeyVersions(r.ID)
		if err != nil {
			return nil, err
		}
		byKey := make(map[string]model.ReplicaKeyVersion, len(vers))
		for _, v := range vers {
			byKey[v.Key] = v
		}
		lagging := false
		for _, key := range sortedKeys(srcByKey) {
			obs, ok := byKey[key]
			if !ok || obs.Version != srcByKey[key] {
				lagging = true
				lines = append(lines, fmt.Sprintf("replica %s key %s: source=%d observed=%d",
					r.ID, key, srcByKey[key], obs.Version))
			}
		}
		if lagging {
			unconverged = append(unconverged, r.ID)
		}
	}
	sort.Strings(unconverged)
	p := &model.Proof{
		ScenarioID:  sc.ID,
		Conclusion:  "timeout",
		Detail:      "replicas not converged within timeout\n" + strings.Join(lines, "\n"),
		Unconverged: unconverged,
		CreatedAt:   g.now().UTC(),
	}
	id, err := g.repo.InsertProof(p)
	if err != nil {
		return nil, err
	}
	p.ID = id
	return p, nil
}

// buildPending 生成未完成说明。
func (g *Generator) buildPending(sc *model.Scenario) (*model.Proof, error) {
	p := &model.Proof{
		ScenarioID: sc.ID,
		Conclusion: "pending",
		Detail:     fmt.Sprintf("scenario %d status %s: run replay first", sc.ID, sc.Status),
		CreatedAt:  g.now().UTC(),
	}
	id, err := g.repo.InsertProof(p)
	if err != nil {
		return nil, err
	}
	p.ID = id
	return p, nil
}

func sortedKeys(m map[string]int64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
