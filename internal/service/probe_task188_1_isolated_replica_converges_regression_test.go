package service_test

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"task188-cacheinv/internal/model"
	"task188-cacheinv/internal/service"
	"task188-cacheinv/internal/store"
)

func TestTask188Bug01_IsolatedReplicaExcludedFromConvergence(t *testing.T) {
	st, err := store.Open(store.Options{Path: filepath.Join(t.TempDir(), "isolated.db")})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	svc := service.New(st, time.Now)
	ks, err := svc.Topo.CreateKeyspace("ks", "isolated", nil)
	if err != nil {
		t.Fatalf("create keyspace: %v", err)
	}
	rp1, err := svc.Topo.RegisterReplica(ks.ID, "active")
	if err != nil {
		t.Fatalf("register active: %v", err)
	}
	rp2, err := svc.Topo.RegisterReplica(ks.ID, "isolated")
	if err != nil {
		t.Fatalf("register isolated: %v", err)
	}
	if _, err := svc.Topo.SetReplicaStatus(rp2.ID, model.ReplicaIsolated); err != nil {
		t.Fatalf("isolate replica: %v", err)
	}
	if _, err := svc.Ver.ApplySourceUpdate(ks.ID, "k", 1, "v1"); err != nil {
		t.Fatalf("source update: %v", err)
	}
	sc, _, err := svc.CreateScenario(ks.ID, "isolated-convergence", nil)
	if err != nil {
		t.Fatalf("create scenario: %v", err)
	}
	if _, err := svc.AppendMessage(sc.ID, model.Message{MsgID: "u1", Kind: model.MsgUpdate, Key: "k", Version: 1, ReplicaID: rp1.ID}); err != nil {
		t.Fatalf("append update: %v", err)
	}
	res, err := svc.RunReplay(sc.ID)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if res.Status != model.ScenarioConverged {
		t.Fatalf("status = %s, want converged; unconverged=%v", res.Status, res.Unconverged)
	}
	p, err := svc.Proof.Build(sc.ID)
	if err != nil {
		t.Fatalf("proof: %v", err)
	}
	if !strings.Contains(p.Detail, "isolated (excluded)") {
		t.Fatalf("proof did not record excluded replica: %s", p.Detail)
	}
}
