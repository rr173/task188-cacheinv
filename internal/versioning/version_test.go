package versioning

import (
	"testing"
	"time"

	"task188-cacheinv/internal/model"
)

// fakeRepo 是内存版 VersionRepo，用于单测。
type fakeRepo struct {
	sources map[string]*model.SourceVersion
	obs     map[string]*model.ReplicaKeyVersion
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		sources: make(map[string]*model.SourceVersion),
		obs:     make(map[string]*model.ReplicaKeyVersion),
	}
}

func (f *fakeRepo) UpsertSourceVersion(sv *model.SourceVersion) error {
	f.sources[sv.KeyspaceID+"/"+sv.Key] = sv
	return nil
}
func (f *fakeRepo) GetSourceVersion(ks, key string) (*model.SourceVersion, error) {
	sv, ok := f.sources[ks+"/"+key]
	if !ok {
		return nil, model.ErrNotFound
	}
	return sv, nil
}
func (f *fakeRepo) ListSourceVersions(string) ([]model.SourceVersion, error) { return nil, nil }
func (f *fakeRepo) UpsertReplicaKeyVersion(rkv *model.ReplicaKeyVersion) error {
	f.obs[rkv.ReplicaID+"/"+rkv.Key] = rkv
	return nil
}
func (f *fakeRepo) GetReplicaKeyVersion(rid, key string) (*model.ReplicaKeyVersion, error) {
	rkv, ok := f.obs[rid+"/"+key]
	if !ok {
		return nil, model.ErrNotFound
	}
	return rkv, nil
}
func (f *fakeRepo) ListReplicaKeyVersions(string) ([]model.ReplicaKeyVersion, error) { return nil, nil }

func TestApplySourceUpdateRejectsRegression(t *testing.T) {
	svc := NewService(newFakeRepo(), time.Now)
	if _, err := svc.ApplySourceUpdate("ks", "k", 5, "h5"); err != nil {
		t.Fatalf("first update: %v", err)
	}
	if _, err := svc.ApplySourceUpdate("ks", "k", 4, "h4"); err == nil {
		t.Fatal("expected regression error")
	}
	if _, err := svc.ApplySourceUpdate("ks", "k", 6, "h6"); err != nil {
		t.Fatalf("forward update: %v", err)
	}
}

func TestObserveVersionOutOfOrderRejected(t *testing.T) {
	svc := NewService(newFakeRepo(), time.Now)
	if _, err := svc.ObserveVersion("r1", "k", 10); err != nil {
		t.Fatalf("observe v10: %v", err)
	}
	if _, err := svc.ObserveVersion("r1", "k", 9); err == nil {
		t.Fatal("expected regression on observing older version")
	}
	// 幂等重复观察 v10 应成功。
	if _, err := svc.ObserveVersion("r1", "k", 10); err != nil {
		t.Fatalf("idempotent observe v10: %v", err)
	}
}

func TestInvalidateStaleTargetRejected(t *testing.T) {
	svc := NewService(newFakeRepo(), time.Now)
	_, _ = svc.ObserveVersion("r1", "k", 10)
	// 乱序失效：目标版本低于当前观察版本，不得回滚。
	if _, err := svc.InvalidateKey("r1", "k", 7); err == nil {
		t.Fatal("expected stale invalidation to be rejected")
	}
	// 目标版本等于当前版本 → 进入待失效。
	rkv, err := svc.InvalidateKey("r1", "k", 10)
	if err != nil {
		t.Fatalf("invalidate same version: %v", err)
	}
	if rkv.Status != model.KeyPendingInvalidation {
		t.Fatalf("expected pending_invalidation, got %s", rkv.Status)
	}
}
