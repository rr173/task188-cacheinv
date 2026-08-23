package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"task188-cacheinv/internal/model"
)

// KeyspaceStore 负责 keyspaces / replicas / protocol_params 的持久化。
type KeyspaceStore struct{ db *sql.DB }

func NewKeyspaceStore(db *sql.DB) *KeyspaceStore { return &KeyspaceStore{db: db} }

// CreateKeyspace 写入键空间与默认协议参数。
func (k *KeyspaceStore) CreateKeyspace(ks *model.Keyspace, p *model.ProtocolParams) error {
	tx, err := k.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(
		`INSERT INTO keyspaces (id, name, description, created_at) VALUES (?, ?, ?, ?)`,
		ks.ID, ks.Name, ks.Description, ks.CreatedAt.Format(time.RFC3339Nano),
	); err != nil {
		return fmt.Errorf("insert keyspace: %w", err)
	}
	if _, err := tx.Exec(
		`INSERT INTO protocol_params (keyspace_id, max_retries, lease_ttl_ms, convergence_timeout_ms, ordering)
		 VALUES (?, ?, ?, ?, ?)`,
		p.KeyspaceID, p.MaxRetries, p.LeaseTTLMs, p.ConvergenceTimeoutMs, p.Ordering,
	); err != nil {
		return fmt.Errorf("insert protocol: %w", err)
	}
	return tx.Commit()
}

// GetKeyspace 按 ID 查询键空间。
func (k *KeyspaceStore) GetKeyspace(id string) (*model.Keyspace, error) {
	row := k.db.QueryRow(`SELECT id, name, description, created_at FROM keyspaces WHERE id = ?`, id)
	var ks model.Keyspace
	var ts string
	if err := row.Scan(&ks.ID, &ks.Name, &ks.Description, &ts); err != nil {
		if err == sql.ErrNoRows {
			return nil, model.ErrNotFound
		}
		return nil, err
	}
	ks.CreatedAt, _ = time.Parse(time.RFC3339Nano, ts)
	return &ks, nil
}

// ListKeyspaces 返回全部键空间。
func (k *KeyspaceStore) ListKeyspaces() ([]model.Keyspace, error) {
	rows, err := k.db.Query(`SELECT id, name, description, created_at FROM keyspaces ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Keyspace
	for rows.Next() {
		var ks model.Keyspace
		var ts string
		if err := rows.Scan(&ks.ID, &ks.Name, &ks.Description, &ts); err != nil {
			return nil, err
		}
		ks.CreatedAt, _ = time.Parse(time.RFC3339Nano, ts)
		out = append(out, ks)
	}
	return out, rows.Err()
}

// GetProtocol 按键空间 ID 查询协议参数。
func (k *KeyspaceStore) GetProtocol(keyspaceID string) (*model.ProtocolParams, error) {
	row := k.db.QueryRow(
		`SELECT keyspace_id, max_retries, lease_ttl_ms, convergence_timeout_ms, ordering
		 FROM protocol_params WHERE keyspace_id = ?`, keyspaceID)
	var p model.ProtocolParams
	if err := row.Scan(&p.KeyspaceID, &p.MaxRetries, &p.LeaseTTLMs, &p.ConvergenceTimeoutMs, &p.Ordering); err != nil {
		if err == sql.ErrNoRows {
			return nil, model.ErrNotFound
		}
		return nil, err
	}
	return &p, nil
}

// UpsertProtocol 更新协议参数（不存在则插入）。
func (k *KeyspaceStore) UpsertProtocol(p *model.ProtocolParams) error {
	_, err := k.db.Exec(
		`INSERT INTO protocol_params (keyspace_id, max_retries, lease_ttl_ms, convergence_timeout_ms, ordering)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(keyspace_id) DO UPDATE SET
		   max_retries=excluded.max_retries,
		   lease_ttl_ms=excluded.lease_ttl_ms,
		   convergence_timeout_ms=excluded.convergence_timeout_ms,
		   ordering=excluded.ordering`,
		p.KeyspaceID, p.MaxRetries, p.LeaseTTLMs, p.ConvergenceTimeoutMs, p.Ordering,
	)
	return err
}

// CreateReplica 注册副本。
func (k *KeyspaceStore) CreateReplica(r *model.Replica) error {
	_, err := k.db.Exec(
		`INSERT INTO replicas (id, keyspace_id, name, status, lease_seq, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		r.ID, r.KeyspaceID, r.Name, string(r.Status), r.LeaseSeq, r.CreatedAt.Format(time.RFC3339Nano),
	)
	return err
}

// GetReplica 按 ID 查询副本。
func (k *KeyspaceStore) GetReplica(id string) (*model.Replica, error) {
	row := k.db.QueryRow(`SELECT id, keyspace_id, name, status, lease_seq, created_at FROM replicas WHERE id = ?`, id)
	var r model.Replica
	var ts string
	if err := row.Scan(&r.ID, &r.KeyspaceID, &r.Name, &r.Status, &r.LeaseSeq, &ts); err != nil {
		if err == sql.ErrNoRows {
			return nil, model.ErrNotFound
		}
		return nil, err
	}
	r.CreatedAt, _ = time.Parse(time.RFC3339Nano, ts)
	return &r, nil
}

// ListReplicas 列出键空间下全部副本。
func (k *KeyspaceStore) ListReplicas(keyspaceID string) ([]model.Replica, error) {
	rows, err := k.db.Query(`SELECT id, keyspace_id, name, status, lease_seq, created_at FROM replicas WHERE keyspace_id = ? ORDER BY created_at`, keyspaceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Replica
	for rows.Next() {
		var r model.Replica
		var ts string
		if err := rows.Scan(&r.ID, &r.KeyspaceID, &r.Name, &r.Status, &r.LeaseSeq, &ts); err != nil {
			return nil, err
		}
		r.CreatedAt, _ = time.Parse(time.RFC3339Nano, ts)
		out = append(out, r)
	}
	return out, rows.Err()
}

// UpdateReplicaStatus 更新副本状态并递增租约序号。
func (k *KeyspaceStore) UpdateReplicaStatus(id string, status model.ReplicaStatus, leaseSeq int64) error {
	res, err := k.db.Exec(`UPDATE replicas SET status = ?, lease_seq = ? WHERE id = ?`, string(status), leaseSeq, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return model.ErrNotFound
	}
	return nil
}

// ReplicaCount 统计副本总数。
func (k *KeyspaceStore) ReplicaCount() (int, error) {
	return countRows(k.db, `SELECT COUNT(*) FROM replicas`)
}

// KeyspaceCount 统计键空间总数。
func (k *KeyspaceStore) KeyspaceCount() (int, error) {
	return countRows(k.db, `SELECT COUNT(*) FROM keyspaces`)
}

func countRows(db *sql.DB, q string) (int, error) {
	var n int
	if err := db.QueryRow(q).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// MarshalUnconverged 序列化未收敛副本集合（存储为 JSON 数组）。
func MarshalUnconverged(ids []string) string {
	b, _ := json.Marshal(ids)
	return string(b)
}

// UnmarshalUnconverged 反序列化未收敛副本集合。
func UnmarshalUnconverged(s string) []string {
	var out []string
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil
	}
	return out
}
