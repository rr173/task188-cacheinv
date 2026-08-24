package service_test

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"task188-cacheinv/internal/model"
	"task188-cacheinv/internal/service"
	"task188-cacheinv/internal/store"
)

func TestTask188Bug04_ConcurrentAppendAllocatesUniqueMessages(t *testing.T) {
	st, err := store.Open(store.Options{Path: filepath.Join(t.TempDir(), "concurrent.db")})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	svc := service.New(st, time.Now)
	ks, err := svc.Topo.CreateKeyspace("ks", "concurrent", nil)
	if err != nil {
		t.Fatalf("create keyspace: %v", err)
	}
	sc, _, err := svc.CreateScenario(ks.ID, "append", nil)
	if err != nil {
		t.Fatalf("create scenario: %v", err)
	}
	const workers = 40
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := svc.AppendMessage(sc.ID, model.Message{
				MsgID: "m-" + string(rune('a'+i)), Kind: model.MsgUpdate,
				Key: "k", Version: int64(i + 1), ReplicaID: "r1",
			})
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent append error: %v", err)
		}
	}
	msgs, err := svc.ListMessages(sc.ID)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(msgs) != workers {
		t.Fatalf("stored %d messages, want %d", len(msgs), workers)
	}
}
