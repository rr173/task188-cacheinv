package service_test

import (
	"path/filepath"
	"testing"
	"time"

	"task188-cacheinv/internal/model"
	"task188-cacheinv/internal/service"
	"task188-cacheinv/internal/store"
)

func TestTask188Bug05_FingerprintReuseStaysWithinKeyspace(t *testing.T) {
	st, err := store.Open(store.Options{Path: filepath.Join(t.TempDir(), "fingerprint.db")})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	svc := service.New(st, time.Now)
	ks1, err := svc.Topo.CreateKeyspace("ks1", "first", nil)
	if err != nil {
		t.Fatalf("create ks1: %v", err)
	}
	rp1, err := svc.Topo.RegisterReplica(ks1.ID, "r1")
	if err != nil {
		t.Fatalf("register r1: %v", err)
	}
	if _, err := svc.Ver.ApplySourceUpdate(ks1.ID, "k", 1, "v1"); err != nil {
		t.Fatalf("source update: %v", err)
	}
	msgs := []model.Message{{MsgID: "u1", Kind: model.MsgUpdate, Key: "k", Version: 1, ReplicaID: rp1.ID}}
	sc1, _, err := svc.CreateScenario(ks1.ID, "first-scenario", msgs)
	if err != nil {
		t.Fatalf("create first scenario: %v", err)
	}
	if _, err := svc.AppendMessages(sc1.ID, msgs); err != nil {
		t.Fatalf("append first scenario: %v", err)
	}
	if res, err := svc.RunReplay(sc1.ID); err != nil || res.Status != model.ScenarioConverged {
		t.Fatalf("first replay = %+v err=%v", res, err)
	}
	ks2, err := svc.Topo.CreateKeyspace("ks2", "second", nil)
	if err != nil {
		t.Fatalf("create ks2: %v", err)
	}
	_, reused, err := svc.CreateScenario(ks2.ID, "second-scenario", msgs)
	if err != nil {
		t.Fatalf("create second scenario: %v", err)
	}
	if reused {
		t.Fatal("scenario result was reused across different keyspaces")
	}
}
