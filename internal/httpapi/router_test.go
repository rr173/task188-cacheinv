package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"task188-cacheinv/internal/model"
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

// TestApplySourceUpdateRegressionReturns409 锁定源版本回退的 HTTP 契约：
// 提交比当前源版本更旧的版本时，接口必须稳定返回 409 Conflict，
// 调用方据此停止继续传播旧版本。该路径此前因 statusFor 用子串匹配而把
// ErrVersionRegression（"version conflict rejected"）误判为 conflict → 200，属核心缺陷。
func TestApplySourceUpdateRegressionReturns409(t *testing.T) {
	st, err := store.Open(store.Options{Path: filepath.Join(t.TempDir(), "api-regression.db")})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	h := New(service.New(st, time.Now)).Routes()

	// 建键空间。
	create := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/api/keyspaces",
		bytes.NewBufferString(`{"name":"reg-ks","description":"regression","max_retries":3,"lease_ttl_ms":1000,"convergence_timeout_ms":1000,"ordering":"logical"}`))
	createReq.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(create, createReq)
	if create.Code != http.StatusCreated {
		t.Fatalf("create keyspace status = %d, body = %s", create.Code, create.Body.String())
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(create.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	post := func(version int64) int {
		body := fmt.Sprintf(`{"key":"user:7","version":%d,"value_hash":"h%d"}`, version, version)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/keyspaces/"+created.ID+"/versions", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(rec, req)
		return rec.Code
	}

	// 首次提交 v3 → 201。
	if code := post(3); code != http.StatusCreated {
		t.Fatalf("first source update status = %d, want %d", code, http.StatusCreated)
	}
	// 源版本倒退 v2 → 必须 409，而非 200。
	if code := post(2); code != http.StatusConflict {
		t.Fatalf("regression status = %d, want %d (callers must see conflict to stop propagation)", code, http.StatusConflict)
	}
	// 同版本回退 v3 → 同样属于倒退（<= 当前源版本），必须 409。
	if code := post(3); code != http.StatusConflict {
		t.Fatalf("equal-version status = %d, want %d", code, http.StatusConflict)
	}
	// 前进 v4 → 201，证明仅倒退被拒绝。
	if code := post(4); code != http.StatusCreated {
		t.Fatalf("forward status = %d, want %d", code, http.StatusCreated)
	}
}

// TestStatusForRegressionCoversWrappedError 直接验证错误映射层的稳定性：
// 即便领域错误被 fmt.Errorf("%w", ...) 包装并拼入业务字段，errors.Is 仍能命中
// ErrVersionRegression → 409，杜绝子串歧义导致的 200 误判。
func TestStatusForRegressionCoversWrappedError(t *testing.T) {
	wrapped := fmt.Errorf("source update failed: %w: key=user:7 proposed=2 current=3",
		model.ErrVersionRegression)
	if code := statusFor(wrapped); code != http.StatusConflict {
		t.Fatalf("statusFor(wrapped regression) = %d, want %d", code, http.StatusConflict)
	}
	if !strings.Contains(wrapped.Error(), "conflict") {
		t.Fatal("precondition: wrapped regression message must contain 'conflict' for the substring-trap test")
	}
	// 同一信息同时含 "rejected" 与 "conflict"：旧子串实现会先命中 conflict→200。
	if code := statusFor(model.ErrVersionRegression); code != http.StatusConflict {
		t.Fatalf("statusFor(ErrVersionRegression) = %d, want %d", code, http.StatusConflict)
	}
}
