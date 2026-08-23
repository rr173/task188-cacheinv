package versioning_test

import (
	"testing"
	"time"

	"task188-cacheinv/internal/model"
	"task188-cacheinv/internal/proof"
	"task188-cacheinv/internal/versioning"
)

func TestTask188Bug03_EqualVersionInvalidationRemainsPending(t *testing.T) {
	repo := newVersionRepoForTask188Bug03()
	ver := versioning.NewService(repo, time.Now)
	if _, err := ver.ObserveVersion("r1", "k", 10); err != nil {
		t.Fatalf("observe version: %v", err)
	}
	rkv, err := ver.InvalidateKey("r1", "k", 10)
	if err != nil {
		t.Fatalf("equal-version invalidation: %v", err)
	}
	if rkv.Status != model.KeyPendingInvalidation {
		t.Fatalf("status = %s, want pending_invalidation", rkv.Status)
	}
	problems := proof.CheckMonotonicObservations([]model.Message{
		{MsgID: "u", Kind: model.MsgUpdate, Key: "k", Version: 10, ReplicaID: "r1"},
		{MsgID: "i", Kind: model.MsgInvalidate, Key: "k", Version: 10, ReplicaID: "r1"},
	})
	if len(problems) != 0 {
		t.Fatalf("equal version generated false regression: %v", problems)
	}
}

type task188Bug03VersionRepo struct {
	obs map[string]*model.ReplicaKeyVersion
}

func newVersionRepoForTask188Bug03() *task188Bug03VersionRepo {
	return &task188Bug03VersionRepo{obs: map[string]*model.ReplicaKeyVersion{}}
}

func (r *task188Bug03VersionRepo) UpsertSourceVersion(*model.SourceVersion) error { return nil }
func (r *task188Bug03VersionRepo) GetSourceVersion(string, string) (*model.SourceVersion, error) {
	return nil, model.ErrNotFound
}
func (r *task188Bug03VersionRepo) ListSourceVersions(string) ([]model.SourceVersion, error) {
	return nil, nil
}
func (r *task188Bug03VersionRepo) UpsertReplicaKeyVersion(v *model.ReplicaKeyVersion) error {
	copy := *v
	r.obs[v.ReplicaID+"/"+v.Key] = &copy
	return nil
}
func (r *task188Bug03VersionRepo) GetReplicaKeyVersion(replicaID, key string) (*model.ReplicaKeyVersion, error) {
	v, ok := r.obs[replicaID+"/"+key]
	if !ok {
		return nil, model.ErrNotFound
	}
	copy := *v
	return &copy, nil
}
func (r *task188Bug03VersionRepo) ListReplicaKeyVersions(string) ([]model.ReplicaKeyVersion, error) {
	return nil, nil
}
