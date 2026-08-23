package service_test

import (
	"path/filepath"
	"testing"
	"time"

	"task188-cacheinv/internal/model"
	"task188-cacheinv/internal/service"
	"task188-cacheinv/internal/store"
)

func TestTask188Bug06_IsolatedReplicaRejectsUpdate(t *testing.T) {
	st, err := store.Open(store.Options{Path: filepath.Join(t.TempDir(), "isolated-update.db")})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	svc := service.New(st, time.Now)
	ks, err := svc.Topo.CreateKeyspace("ks", "isolated update", nil)
	if err != nil {
		t.Fatalf("create keyspace: %v", err)
	}
	rp, err := svc.Topo.RegisterReplica(ks.ID, "isolated")
	if err != nil {
		t.Fatalf("register replica: %v", err)
	}
	if _, err := svc.Topo.SetReplicaStatus(rp.ID, model.ReplicaIsolated); err != nil {
		t.Fatalf("isolate replica: %v", err)
	}
	if _, err := svc.Ver.ApplySourceUpdate(ks.ID, "k", 1, "v1"); err != nil {
		t.Fatalf("source update: %v", err)
	}
	sc, _, err := svc.CreateScenario(ks.ID, "isolated-update", nil)
	if err != nil {
		t.Fatalf("create scenario: %v", err)
	}
	if _, err := svc.AppendMessage(sc.ID, model.Message{MsgID: "u1", Kind: model.MsgUpdate, Key: "k", Version: 1, ReplicaID: rp.ID}); err != nil {
		t.Fatalf("append update: %v", err)
	}
	res, err := svc.RunReplay(sc.ID)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if res.Status != model.ScenarioMonotonicityViolated {
		t.Fatalf("status = %s, want monotonicity_violated", res.Status)
	}
	msgs, err := svc.ListMessages(sc.ID)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(msgs) != 1 || msgs[0].Status != model.MsgRejected {
		t.Fatalf("message = %+v, want rejected", msgs)
	}
}
