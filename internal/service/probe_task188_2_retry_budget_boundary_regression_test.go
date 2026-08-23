package service_test

import (
	"path/filepath"
	"testing"
	"time"

	"task188-cacheinv/internal/model"
	"task188-cacheinv/internal/service"
	"task188-cacheinv/internal/store"
)

func TestTask188Bug02_RetryAtConfiguredBoundaryIsDelivered(t *testing.T) {
	st, err := store.Open(store.Options{Path: filepath.Join(t.TempDir(), "retry.db")})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	svc := service.New(st, time.Now)
	ks, err := svc.Topo.CreateKeyspace("ks", "retry", &model.ProtocolParams{MaxRetries: 2, LeaseTTLMs: 5000, ConvergenceTimeoutMs: 1000, Ordering: "logical"})
	if err != nil {
		t.Fatalf("create keyspace: %v", err)
	}
	rp, err := svc.Topo.RegisterReplica(ks.ID, "r1")
	if err != nil {
		t.Fatalf("register replica: %v", err)
	}
	sc, _, err := svc.CreateScenario(ks.ID, "retry-boundary", nil)
	if err != nil {
		t.Fatalf("create scenario: %v", err)
	}
	if _, err := svc.AppendMessage(sc.ID, model.Message{MsgID: "retry-1", Kind: model.MsgRetry, Key: "k", Version: 1, ReplicaID: rp.ID}); err != nil {
		t.Fatalf("append retry: %v", err)
	}
	if _, err := svc.RunReplay(sc.ID); err != nil {
		t.Fatalf("replay: %v", err)
	}
	msgs, err := svc.ListMessages(sc.ID)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(msgs) != 1 || msgs[0].Status != model.MsgDelivered || msgs[0].RetryCount != 1 {
		t.Fatalf("retry message = %+v, want delivered with retry_count=1", msgs)
	}
}
