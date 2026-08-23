package service_test

import (
	"path/filepath"
	"testing"
	"time"

	"task188-cacheinv/internal/service"
	"task188-cacheinv/internal/store"
)

func TestTask188Bug07_NilProtocolUsesDefaults(t *testing.T) {
	st, err := store.Open(store.Options{Path: filepath.Join(t.TempDir(), "defaults.db")})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	svc := service.New(st, time.Now)
	ks, err := svc.Topo.CreateKeyspace("defaults", "nil protocol", nil)
	if err != nil {
		t.Fatalf("create keyspace with nil protocol: %v", err)
	}
	if ks.ID == "" {
		t.Fatal("created keyspace has no id")
	}
	p, err := svc.Topo.GetProtocol(ks.ID)
	if err != nil {
		t.Fatalf("get default protocol: %v", err)
	}
	if p.MaxRetries != 3 || p.LeaseTTLMs != 5000 || p.ConvergenceTimeoutMs != 10000 || p.Ordering != "logical" {
		t.Fatalf("protocol defaults = %+v", p)
	}
}
