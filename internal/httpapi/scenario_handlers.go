package httpapi

import (
	"encoding/json"
	"net/http"

	"task188-cacheinv/internal/model"
	"task188-cacheinv/internal/replay"
)

type createScenarioReq struct {
	Name       string          `json:"name"`
	KeyspaceID string          `json:"keyspace_id"`
	Messages   []model.Message `json:"messages"`
}

// handleCreateScenario 创建场景（可携带初始消息）。
func (a *API) handleCreateScenario(w http.ResponseWriter, r *http.Request) {
	var req createScenarioReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, model.ErrInvalidInput)
		return
	}
	sc, reused, err := a.svc.CreateScenario(req.KeyspaceID, req.Name, req.Messages)
	if err != nil {
		writeErr(w, err)
		return
	}
	if len(req.Messages) > 0 {
		if _, err := a.svc.AppendMessages(sc.ID, req.Messages); err != nil {
			writeErr(w, err)
			return
		}
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"scenario": sc,
		"reused":   reused,
	})
}

// handleListScenarios 列出全部场景。
func (a *API) handleListScenarios(w http.ResponseWriter, r *http.Request) {
	scs, err := a.svc.ListScenarios()
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"scenarios": scs})
}

// handleGetScenario 查询单个场景。
func (a *API) handleGetScenario(w http.ResponseWriter, r *http.Request) {
	id, err := idParam(r, "id")
	if err != nil {
		writeErr(w, model.ErrInvalidInput)
		return
	}
	sc, err := a.svc.GetScenario(id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sc)
}

// handleAppendMessage 追加一条消息。
func (a *API) handleAppendMessage(w http.ResponseWriter, r *http.Request) {
	id, err := idParam(r, "id")
	if err != nil {
		writeErr(w, model.ErrInvalidInput)
		return
	}
	var msg model.Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		writeErr(w, model.ErrInvalidInput)
		return
	}
	appended, err := a.svc.AppendMessage(id, msg)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, appended)
}

// handleListMessages 列出场景消息（逻辑时钟序）。
func (a *API) handleListMessages(w http.ResponseWriter, r *http.Request) {
	id, err := idParam(r, "id")
	if err != nil {
		writeErr(w, model.ErrInvalidInput)
		return
	}
	msgs, err := a.svc.ListMessages(id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": msgs})
}

// handleRunReplay 运行回放。
func (a *API) handleRunReplay(w http.ResponseWriter, r *http.Request) {
	id, err := idParam(r, "id")
	if err != nil {
		writeErr(w, model.ErrInvalidInput)
		return
	}
	result, err := a.svc.RunReplay(id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":      result.Status,
		"violations":  result.Violations,
		"unconverged": result.Unconverged,
		"processed":   result.Processed,
		"summary":     replay.DescribeResult(result),
	})
}

// handleReplayStatus 查询回放状态（不触发回放）。
func (a *API) handleReplayStatus(w http.ResponseWriter, r *http.Request) {
	id, err := idParam(r, "id")
	if err != nil {
		writeErr(w, model.ErrInvalidInput)
		return
	}
	sc, err := a.svc.GetScenario(id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"scenario_id": sc.ID,
		"status":      sc.Status,
		"cursor_pos":  sc.CursorPos,
	})
}

// handleListViolations 列出违反步骤。
func (a *API) handleListViolations(w http.ResponseWriter, r *http.Request) {
	id, err := idParam(r, "id")
	if err != nil {
		writeErr(w, model.ErrInvalidInput)
		return
	}
	viols, err := a.svc.ListViolations(id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"violations": viols})
}

// handleBuildProof 生成收敛证明。
func (a *API) handleBuildProof(w http.ResponseWriter, r *http.Request) {
	id, err := idParam(r, "id")
	if err != nil {
		writeErr(w, model.ErrInvalidInput)
		return
	}
	p, err := a.svc.Proof.Build(id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

// handleFreezeSpec 冻结规格。
func (a *API) handleFreezeSpec(w http.ResponseWriter, r *http.Request) {
	id, err := idParam(r, "id")
	if err != nil {
		writeErr(w, model.ErrInvalidInput)
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Name == "" {
		req.Name = "spec"
	}
	sp, err := a.svc.Spec.FreezeFromScenario(id, req.Name)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, sp)
}
