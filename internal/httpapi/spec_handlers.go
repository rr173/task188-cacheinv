package httpapi

import (
	"net/http"

	"task188-cacheinv/internal/model"
)

// handleListSpecs 列出全部规格。
func (a *API) handleListSpecs(w http.ResponseWriter, r *http.Request) {
	specs, err := a.svc.ListSpecs()
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"specs": specs})
}

// handleGetSpec 查询规格（含消息序列哈希）。
func (a *API) handleGetSpec(w http.ResponseWriter, r *http.Request) {
	id, err := idParam(r, "id")
	if err != nil {
		writeErr(w, model.ErrInvalidInput)
		return
	}
	sp, err := a.svc.GetSpec(id)
	if err != nil {
		writeErr(w, err)
		return
	}
	msgs, err := a.svc.ListSpecMessages(id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"spec":     sp,
		"messages": msgs,
	})
}

// handleRecheckSpec 规格回归验证。
func (a *API) handleRecheckSpec(w http.ResponseWriter, r *http.Request) {
	id, err := idParam(r, "id")
	if err != nil {
		writeErr(w, model.ErrInvalidInput)
		return
	}
	result, err := a.svc.Spec.Recheck(id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"result": result})
}
