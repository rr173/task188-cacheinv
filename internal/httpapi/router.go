// Package httpapi 提供 HTTP+JSON 接口（路由前缀 /api）。
package httpapi

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"

	"task188-cacheinv/internal/service"
)

// API 持有路由所需的编排服务。
type API struct {
	svc *service.Service
}

// New 构造 HTTP API。
func New(svc *service.Service) *API { return &API{svc: svc} }

// Routes 返回路由 mux（前缀 /api）。
func (a *API) Routes() *http.ServeMux {
	mux := http.NewServeMux()

	// 键空间与副本。
	mux.HandleFunc("POST /api/keyspaces", a.handleCreateKeyspace)
	mux.HandleFunc("GET /api/keyspaces", a.handleListKeyspaces)
	mux.HandleFunc("GET /api/keyspaces/{id}", a.handleGetKeyspace)
	mux.HandleFunc("POST /api/keyspaces/{id}/replicas", a.handleRegisterReplica)
	mux.HandleFunc("GET /api/keyspaces/{id}/replicas", a.handleListReplicas)
	mux.HandleFunc("GET /api/keyspaces/{id}/protocol", a.handleGetProtocol)
	mux.HandleFunc("PUT /api/keyspaces/{id}/protocol", a.handleUpdateProtocol)
	mux.HandleFunc("POST /api/keyspaces/{id}/versions", a.handleApplySourceUpdate)
	mux.HandleFunc("GET /api/keyspaces/{id}/versions", a.handleListSourceVersions)
	mux.HandleFunc("POST /api/replicas/{id}/status", a.handleSetReplicaStatus)

	// 场景与回放。
	mux.HandleFunc("POST /api/scenarios", a.handleCreateScenario)
	mux.HandleFunc("GET /api/scenarios", a.handleListScenarios)
	mux.HandleFunc("GET /api/scenarios/{id}", a.handleGetScenario)
	mux.HandleFunc("POST /api/scenarios/{id}/messages", a.handleAppendMessage)
	mux.HandleFunc("GET /api/scenarios/{id}/messages", a.handleListMessages)
	mux.HandleFunc("POST /api/scenarios/{id}/replay", a.handleRunReplay)
	mux.HandleFunc("GET /api/scenarios/{id}/replay", a.handleReplayStatus)
	mux.HandleFunc("GET /api/scenarios/{id}/violations", a.handleListViolations)
	mux.HandleFunc("GET /api/scenarios/{id}/proof", a.handleBuildProof)
	mux.HandleFunc("POST /api/scenarios/{id}/specs", a.handleFreezeSpec)

	// 规格。
	mux.HandleFunc("GET /api/specs", a.handleListSpecs)
	mux.HandleFunc("GET /api/specs/{id}", a.handleGetSpec)
	mux.HandleFunc("POST /api/specs/{id}/recheck", a.handleRecheckSpec)

	// 统计与健康。
	mux.HandleFunc("GET /api/stats", a.handleStats)
	mux.HandleFunc("GET /api/health", a.handleHealth)

	return mux
}

func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleStats 返回聚合统计。
func (a *API) handleStats(w http.ResponseWriter, r *http.Request) {
	st, err := a.svc.Stats()
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

// writeJSON 写 JSON 响应。
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("write json: %v", err)
	}
}

// writeErr 按错误种类映射 HTTP 状态码。
func writeErr(w http.ResponseWriter, err error) {
	code := statusFor(err)
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

// statusFor 错误 → HTTP 状态码。
func statusFor(err error) int {
	switch {
	case strings.Contains(err.Error(), "not found"):
		return http.StatusNotFound
	case strings.Contains(err.Error(), "duplicate"):
		return http.StatusConflict
	case strings.Contains(err.Error(), "conflict"):
		return http.StatusConflict
	case strings.Contains(err.Error(), "regression"):
		return http.StatusConflict
	case strings.Contains(err.Error(), "rejected"):
		return http.StatusBadRequest
	case strings.Contains(err.Error(), "invalid"):
		return http.StatusBadRequest
	case strings.Contains(err.Error(), "frozen"):
		return http.StatusConflict
	case strings.Contains(err.Error(), "not editable"):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

// idParam 解析路径参数为 int64。
func idParam(r *http.Request, name string) (int64, error) {
	return strconv.ParseInt(r.PathValue(name), 10, 64)
}
