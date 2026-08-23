package spec

import (
	"errors"
	"testing"
	"time"

	"task188-cacheinv/internal/model"
)

// fakeRepo 是 spec.Service 的最小内存依赖实现，便于冻结路径的单测。
type fakeRepo struct {
	sc      *model.Scenario
	msgs    []model.Message
	created []*model.Spec
}

func (f *fakeRepo) GetScenario(id int64) (*model.Scenario, error) { return f.sc, nil }
func (f *fakeRepo) ListMessages(scenarioID int64) ([]model.Message, error) {
	return f.msgs, nil
}
func (f *fakeRepo) CreateSpec(sp *model.Spec, msgs []model.SpecMessage) (int64, error) {
	cp := *sp
	cp.ID = int64(len(f.created) + 1)
	f.created = append(f.created, &cp)
	return cp.ID, nil
}
func (f *fakeRepo) GetSpec(id int64) (*model.Spec, error)            { return nil, model.ErrNotFound }
func (f *fakeRepo) ListSpecs() ([]model.Spec, error)                { return nil, nil }
func (f *fakeRepo) FreezeSpec(id int64) error                      { return nil }
func (f *fakeRepo) ListSpecMessages(specID int64) ([]model.SpecMessage, error) { return nil, nil }

// TestFreezeFromScenarioEmptyMessagesNotCrash 回归保护：
// 一个已收敛但无消息的场景不应触发切片越界 panic，
// 而应返回明确的输入错误，并且不向存储写入空规格。
func TestFreezeFromScenarioEmptyMessagesNotCrash(t *testing.T) {
	repo := &fakeRepo{
		sc:   &model.Scenario{ID: 1, Status: model.ScenarioConverged},
		msgs: nil, // 无消息
	}
	svc := New(repo, time.Now)

	spec, err := svc.FreezeFromScenario(1, "empty")
	if err == nil {
		t.Fatalf("expected input error, got spec %+v", spec)
	}
	if !errors.Is(err, model.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput, got %v", err)
	}
	if len(repo.created) != 0 {
		t.Fatalf("expected no spec persisted, got %d", len(repo.created))
	}
}
