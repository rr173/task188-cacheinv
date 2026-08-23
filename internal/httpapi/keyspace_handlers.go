package httpapi

import (
	"encoding/json"
	"net/http"

	"task188-cacheinv/internal/model"
)

type createKeyspaceReq struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	MaxRetries  *int   `json:"max_retries"`
	LeaseTTLMs  *int64 `json:"lease_ttl_ms"`
	TimeoutMs   *int64 `json:"convergence_timeout_ms"`
	Ordering    string `json:"ordering"`
}

// handleCreateKeyspace 创建键空间（含协议参数）。
func (a *API) handleCreateKeyspace(w http.ResponseWriter, r *http.Request) {
	var req createKeyspaceReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, model.ErrInvalidInput)
		return
	}
	p := model.ProtocolParams{Ordering: req.Ordering}
	if req.MaxRetries != nil {
		p.MaxRetries = *req.MaxRetries
	}
	if req.LeaseTTLMs != nil {
		p.LeaseTTLMs = *req.LeaseTTLMs
	}
	if req.TimeoutMs != nil {
		p.ConvergenceTimeoutMs = *req.TimeoutMs
	}
	ks, err := a.svc.Topo.CreateKeyspace(req.Name, req.Description, &p)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, ks)
}

// handleListKeyspaces 列出全部键空间。
func (a *API) handleListKeyspaces(w http.ResponseWriter, r *http.Request) {
	kss, err := a.svc.Topo.ListKeyspaces()
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"keyspaces": kss})
}

// handleGetKeyspace 查询单个键空间。
func (a *API) handleGetKeyspace(w http.ResponseWriter, r *http.Request) {
	ks, err := a.svc.Topo.GetKeyspace(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ks)
}

type registerReplicaReq struct {
	Name string `json:"name"`
}

// handleRegisterReplica 注册副本。
func (a *API) handleRegisterReplica(w http.ResponseWriter, r *http.Request) {
	var req registerReplicaReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, model.ErrInvalidInput)
		return
	}
	rp, err := a.svc.Topo.RegisterReplica(r.PathValue("id"), req.Name)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, rp)
}

// handleListReplicas 列出键空间下副本。
func (a *API) handleListReplicas(w http.ResponseWriter, r *http.Request) {
	rps, err := a.svc.Topo.ListReplicas(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"replicas": rps})
}

// handleGetProtocol 查询协议参数。
func (a *API) handleGetProtocol(w http.ResponseWriter, r *http.Request) {
	p, err := a.svc.Topo.GetProtocol(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

// handleUpdateProtocol 更新协议参数。
func (a *API) handleUpdateProtocol(w http.ResponseWriter, r *http.Request) {
	var p model.ProtocolParams
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		writeErr(w, model.ErrInvalidInput)
		return
	}
	p.KeyspaceID = r.PathValue("id")
	updated, err := a.svc.Topo.UpdateProtocol(&p)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

type sourceUpdateReq struct {
	Key       string `json:"key"`
	Version   int64  `json:"version"`
	ValueHash string `json:"value_hash"`
}

// handleApplySourceUpdate 提交源更新（拒绝版本倒退）。
func (a *API) handleApplySourceUpdate(w http.ResponseWriter, r *http.Request) {
	var req sourceUpdateReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, model.ErrInvalidInput)
		return
	}
	sv, err := a.svc.Ver.ApplySourceUpdate(r.PathValue("id"), req.Key, req.Version, req.ValueHash)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, sv)
}

// handleListSourceVersions 列出源版本。
func (a *API) handleListSourceVersions(w http.ResponseWriter, r *http.Request) {
	svs, err := a.svc.Ver.ListSourceVersions(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"versions": svs})
}

type setStatusReq struct {
	Status string `json:"status"`
}

// handleSetReplicaStatus 隔离/恢复副本。
func (a *API) handleSetReplicaStatus(w http.ResponseWriter, r *http.Request) {
	var req setStatusReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, model.ErrInvalidInput)
		return
	}
	rp, err := a.svc.Topo.SetReplicaStatus(r.PathValue("id"), model.ReplicaStatus(req.Status))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rp)
}
