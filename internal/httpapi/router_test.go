package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"task188-cacheinv/internal/service"
	"task188-cacheinv/internal/store"
)

func TestRoutesCreateAndListKeyspace(t *testing.T) {
	st, err := store.Open(store.Options{Path: filepath.Join(t.TempDir(), "api.db")})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	h := New(service.New(st, time.Now)).Routes()
	req := httptest.NewRequest(http.MethodPost, "/api/keyspaces", bytes.NewBufferString(`{"name":"api-ks","description":"http","max_retries":3,"lease_ttl_ms":1000,"convergence_timeout_ms":1000,"ordering":"logical"}`))
	req.Header.Set("Content-Type", "application/json")
	create := httptest.NewRecorder()
	h.ServeHTTP(create, req)
	if create.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", create.Code, create.Body.String())
	}

	var created struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(create.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if created.ID == "" {
		t.Fatal("create response did not contain an id")
	}

	list := httptest.NewRecorder()
	h.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/api/keyspaces", nil))
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", list.Code, list.Body.String())
	}
	var payload struct {
		Keyspaces []struct {
			ID string `json:"id"`
		} `json:"keyspaces"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(payload.Keyspaces) != 1 || payload.Keyspaces[0].ID != created.ID {
		t.Fatalf("list response = %+v, want created keyspace", payload.Keyspaces)
	}
}
