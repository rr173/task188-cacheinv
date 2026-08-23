package store

import (
	"database/sql"
	"fmt"
	"time"

	"task188-cacheinv/internal/model"
)

// VersionStore 负责 source_versions 与 replica_key_versions 的持久化。
type VersionStore struct{ db *sql.DB }

func NewVersionStore(db *sql.DB) *VersionStore { return &VersionStore{db: db} }

// UpsertSourceVersion 写入源端最新版本；版本必须单调递增（调用方校验）。
func (v *VersionStore) UpsertSourceVersion(sv *model.SourceVersion) error {
	_, err := v.db.Exec(
		`INSERT INTO source_versions (keyspace_id, key, version, value_hash, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(keyspace_id, key) DO UPDATE SET
		   version=excluded.version, value_hash=excluded.value_hash, updated_at=excluded.updated_at`,
		sv.KeyspaceID, sv.Key, sv.Version, sv.ValueHash, sv.UpdatedAt.Format(time.RFC3339Nano),
	)
	return err
}

// GetSourceVersion 查询某键的源端版本。
func (v *VersionStore) GetSourceVersion(keyspaceID, key string) (*model.SourceVersion, error) {
	row := v.db.QueryRow(
		`SELECT keyspace_id, key, version, value_hash, updated_at FROM source_versions WHERE keyspace_id = ? AND key = ?`,
		keyspaceID, key)
	var sv model.SourceVersion
	var ts string
	if err := row.Scan(&sv.KeyspaceID, &sv.Key, &sv.Version, &sv.ValueHash, &ts); err != nil {
		if err == sql.ErrNoRows {
			return nil, model.ErrNotFound
		}
		return nil, err
	}
	sv.UpdatedAt, _ = time.Parse(time.RFC3339Nano, ts)
	return &sv, nil
}

// ListSourceVersions 列出键空间全部源版本。
func (v *VersionStore) ListSourceVersions(keyspaceID string) ([]model.SourceVersion, error) {
	rows, err := v.db.Query(
		`SELECT keyspace_id, key, version, value_hash, updated_at FROM source_versions WHERE keyspace_id = ? ORDER BY key`,
		keyspaceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.SourceVersion
	for rows.Next() {
		var sv model.SourceVersion
		var ts string
		if err := rows.Scan(&sv.KeyspaceID, &sv.Key, &sv.Version, &sv.ValueHash, &ts); err != nil {
			return nil, err
		}
		sv.UpdatedAt, _ = time.Parse(time.RFC3339Nano, ts)
		out = append(out, sv)
	}
	return out, rows.Err()
}

// SourceVersionCount 统计源版本总数。
func (v *VersionStore) SourceVersionCount() (int, error) {
	var n int
	if err := v.db.QueryRow(`SELECT COUNT(*) FROM source_versions`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// UpsertReplicaKeyVersion 写入副本对某键的观察版本。
func (v *VersionStore) UpsertReplicaKeyVersion(rkv *model.ReplicaKeyVersion) error {
	_, err := v.db.Exec(
		`INSERT INTO replica_key_versions (replica_id, key, version, status, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(replica_id, key) DO UPDATE SET
		   version=excluded.version, status=excluded.status, updated_at=excluded.updated_at`,
		rkv.ReplicaID, rkv.Key, rkv.Version, string(rkv.Status), rkv.UpdatedAt.Format(time.RFC3339Nano),
	)
	return err
}

// GetReplicaKeyVersion 查询副本对某键的观察版本。
func (v *VersionStore) GetReplicaKeyVersion(replicaID, key string) (*model.ReplicaKeyVersion, error) {
	row := v.db.QueryRow(
		`SELECT replica_id, key, version, status, updated_at FROM replica_key_versions WHERE replica_id = ? AND key = ?`,
		replicaID, key)
	var rkv model.ReplicaKeyVersion
	var ts string
	if err := row.Scan(&rkv.ReplicaID, &rkv.Key, &rkv.Version, &rkv.Status, &ts); err != nil {
		if err == sql.ErrNoRows {
			return nil, model.ErrNotFound
		}
		return nil, err
	}
	rkv.UpdatedAt, _ = time.Parse(time.RFC3339Nano, ts)
	return &rkv, nil
}

// ListReplicaKeyVersions 列出副本的全部观察版本。
func (v *VersionStore) ListReplicaKeyVersions(replicaID string) ([]model.ReplicaKeyVersion, error) {
	rows, err := v.db.Query(
		`SELECT replica_id, key, version, status, updated_at FROM replica_key_versions WHERE replica_id = ? ORDER BY key`,
		replicaID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.ReplicaKeyVersion
	for rows.Next() {
		var rkv model.ReplicaKeyVersion
		var ts string
		if err := rows.Scan(&rkv.ReplicaID, &rkv.Key, &rkv.Version, &rkv.Status, &ts); err != nil {
			return nil, err
		}
		rkv.UpdatedAt, _ = time.Parse(time.RFC3339Nano, ts)
		out = append(out, rkv)
	}
	return out, rows.Err()
}

// ListAllReplicaKeyVersions 列出某键空间内全部副本观察版本（用于收敛判定）。
func (v *VersionStore) ListAllReplicaKeyVersions(keyspaceID string) ([]model.ReplicaKeyVersion, error) {
	rows, err := v.db.Query(
		`SELECT rkv.replica_id, rkv.key, rkv.version, rkv.status, rkv.updated_at
		 FROM replica_key_versions rkv
		 JOIN replicas r ON r.id = rkv.replica_id
		 WHERE r.keyspace_id = ? ORDER BY rkv.replica_id, rkv.key`,
		keyspaceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.ReplicaKeyVersion
	for rows.Next() {
		var rkv model.ReplicaKeyVersion
		var ts string
		if err := rows.Scan(&rkv.ReplicaID, &rkv.Key, &rkv.Version, &rkv.Status, &ts); err != nil {
			return nil, err
		}
		rkv.UpdatedAt, _ = time.Parse(time.RFC3339Nano, ts)
		out = append(out, rkv)
	}
	return out, rows.Err()
}

// touchKeyVersion 在事务内写入副本观察版本（供回放引擎复用）。
func touchKeyVersion(tx *sql.Tx, rkv *model.ReplicaKeyVersion) error {
	_, err := tx.Exec(
		`INSERT INTO replica_key_versions (replica_id, key, version, status, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(replica_id, key) DO UPDATE SET
		   version=excluded.version, status=excluded.status, updated_at=excluded.updated_at`,
		rkv.ReplicaID, rkv.Key, rkv.Version, string(rkv.Status), rkv.UpdatedAt.Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("touch replica key version: %w", err)
	}
	return nil
}
