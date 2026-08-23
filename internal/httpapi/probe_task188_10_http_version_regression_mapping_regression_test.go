package httpapi_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"task188-cacheinv/internal/httpapi"
	"task188-cacheinv/internal/service"
	"task188-cacheinv/internal/store"
)

func TestTask188Bug10_HTTPVersionRegressionMapsToConflict(t *testing.T) {
	st, err := store.Open(store.Options{Path: filepath.Join(t.TempDir(), "http.db")})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	h := httpapi.New(service.New(st, time.Now)).Routes()
	create := httptest.NewRequest(http.MethodPost, "/api/keyspaces", bytes.NewBufferString(`{"name":"ks","description":"http","max_retries":3,"lease_ttl_ms":1000,"convergence_timeout_ms":1000,"ordering":"logical"}`))
	created := httptest.NewRecorder()
	h.ServeHTTP(created, create)
	if created.Code != http.StatusCreated {
		t.Fatalf("create keyspace status = %d, body = %s", created.Code, created.Body.String())
	}
	var keyspace struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &keyspace); err != nil {
		t.Fatalf("decode keyspace: %v", err)
	}
	post := func(version int) *httptest.ResponseRecorder {
		body := fmt.Sprintf(`{"key":"k","version":%d,"value_hash":"v%d"}`, version, version)
		req := httptest.NewRequest(http.MethodPost, "/api/keyspaces/"+keyspace.ID+"/versions", bytes.NewBufferString(body))
		out := httptest.NewRecorder()
		h.ServeHTTP(out, req)
		return out
	}
	if out := post(2); out.Code != http.StatusCreated {
		t.Fatalf("first version status = %d, body = %s", out.Code, out.Body.String())
	}
	if out := post(1); out.Code != http.StatusConflict {
		t.Fatalf("regression status = %d, want 409, body = %s", out.Code, out.Body.String())
	}
}
