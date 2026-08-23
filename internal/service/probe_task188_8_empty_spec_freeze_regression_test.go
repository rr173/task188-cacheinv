package service_test

import (
	"path/filepath"
	"testing"
	"time"

	"task188-cacheinv/internal/model"
	"task188-cacheinv/internal/service"
	"task188-cacheinv/internal/store"
)

func TestTask188Bug08_EmptyScenarioFreezeReturnsInputError(t *testing.T) {
	st, err := store.Open(store.Options{Path: filepath.Join(t.TempDir(), "empty-spec.db")})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	svc := service.New(st, time.Now)
	ks, err := svc.Topo.CreateKeyspace("ks", "empty spec", nil)
	if err != nil {
		t.Fatalf("create keyspace: %v", err)
	}
	sc, _, err := svc.CreateScenario(ks.ID, "empty", nil)
	if err != nil {
		t.Fatalf("create scenario: %v", err)
	}
	if err := svc.UpdateScenarioStatus(sc.ID, model.ScenarioConverged, 0); err != nil {
		t.Fatalf("mark converged: %v", err)
	}
	if _, err := svc.Spec.FreezeFromScenario(sc.ID, "empty-spec"); err == nil {
		t.Fatal("empty scenario freeze unexpectedly succeeded")
	}
}
