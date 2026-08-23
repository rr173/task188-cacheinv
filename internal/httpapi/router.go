// Package httpapi 提供 HTTP+JSON 接口（路由前缀 /api）。
package httpapi

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"

	"task188-cacheinv/internal/model"
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
//
// 必须基于 errors.Is 哨兵匹配而非字符串子串匹配：领域错误在向上传播时会被
// fmt.Errorf("%w", ...) 反复包装，错误信息也会拼入业务字段（如 key、版本号）。
// 用子串匹配会因顺序歧义把一个错误映射到错误的状态——典型地，
// model.ErrVersionRegression 的信息 "version conflict rejected" 同时含有 "conflict"
// 与 "rejected"，按子串它会先命中 conflict 分支而返回 200，使源版本倒退被当作成功，
// 调用方据此继续传播旧版本。因此这里用 errors.Is 稳定区分各类哨兵错误。
func statusFor(err error) int {
	switch {
	case errors.Is(err, model.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, model.ErrVersionRegression):
		// 源版本倒退（或副本观察版本回滚）：必须返回 409，调用方据此停止传播旧版本。
		return http.StatusConflict
	case errors.Is(err, model.ErrConflict):
		return http.StatusConflict
	case errors.Is(err, model.ErrSpecFrozen):
		return http.StatusConflict
	case errors.Is(err, model.ErrScenarioNotEditable):
		return http.StatusConflict
	case errors.Is(err, model.ErrScenarioNotFrozen):
		return http.StatusConflict
	case errors.Is(err, model.ErrMessageContentMismatch):
		return http.StatusConflict
	case errors.Is(err, model.ErrAckWithoutLease):
		return http.StatusConflict
	case errors.Is(err, model.ErrUnknownReplicaAck):
		return http.StatusConflict
	case errors.Is(err, model.ErrUnboundedRetry):
		return http.StatusConflict
	case errors.Is(err, model.ErrReplayInProgress):
		return http.StatusConflict
	case errors.Is(err, model.ErrInvalidInput):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

// idParam 解析路径参数为 int64。
func idParam(r *http.Request, name string) (int64, error) {
	return strconv.ParseInt(r.PathValue(name), 10, 64)
}
