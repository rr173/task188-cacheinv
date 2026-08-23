package service_test

import (
	"path/filepath"
	"testing"
	"time"

	"task188-cacheinv/internal/model"
	"task188-cacheinv/internal/service"
	"task188-cacheinv/internal/store"
)

func TestTask188Bug09_ProtocolDriftInvalidatesFrozenSpec(t *testing.T) {
	st, err := store.Open(store.Options{Path: filepath.Join(t.TempDir(), "spec-drift.db")})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	svc := service.New(st, time.Now)
	ks, err := svc.Topo.CreateKeyspace("ks", "spec drift", nil)
	if err != nil {
		t.Fatalf("create keyspace: %v", err)
	}
	rp, err := svc.Topo.RegisterReplica(ks.ID, "r1")
	if err != nil {
		t.Fatalf("register replica: %v", err)
	}
	if _, err := svc.Ver.ApplySourceUpdate(ks.ID, "k", 1, "v1"); err != nil {
		t.Fatalf("source update: %v", err)
	}
	sc, _, err := svc.CreateScenario(ks.ID, "scenario", nil)
	if err != nil {
		t.Fatalf("create scenario: %v", err)
	}
	if _, err := svc.AppendMessage(sc.ID, model.Message{MsgID: "u1", Kind: model.MsgUpdate, Key: "k", Version: 1, ReplicaID: rp.ID}); err != nil {
		t.Fatalf("append update: %v", err)
	}
	if res, err := svc.RunReplay(sc.ID); err != nil || res.Status != model.ScenarioConverged {
		t.Fatalf("replay = %+v err=%v", res, err)
	}
	sp, err := svc.Spec.FreezeFromScenario(sc.ID, "frozen")
	if err != nil {
		t.Fatalf("freeze spec: %v", err)
	}
	p, err := svc.Topo.GetProtocol(ks.ID)
	if err != nil {
		t.Fatalf("get protocol: %v", err)
	}
	p.MaxRetries++
	if _, err := svc.Topo.UpdateProtocol(p); err != nil {
		t.Fatalf("update protocol: %v", err)
	}
	result, err := svc.Spec.Recheck(sp.ID)
	if err != nil {
		t.Fatalf("recheck: %v", err)
	}
	if result != "failed" {
		t.Fatalf("recheck result = %q, want failed after protocol drift", result)
	}
}
