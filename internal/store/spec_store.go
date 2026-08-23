package store

import (
	"database/sql"
	"time"

	"task188-cacheinv/internal/model"
)

// SpecStore 负责 specs 与 spec_messages 的持久化。
type SpecStore struct{ db *sql.DB }

func NewSpecStore(db *sql.DB) *SpecStore { return &SpecStore{db: db} }

// CreateSpec 创建规格（draft 状态）并保存完整消息序列。
func (s *SpecStore) CreateSpec(spec *model.Spec, msgs []model.SpecMessage) (int64, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	res, err := tx.Exec(
		`INSERT INTO specs (scenario_id, name, message_hash, status, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		spec.ScenarioID, spec.Name, spec.MessageHash, string(spec.Status), spec.CreatedAt.Format(time.RFC3339Nano),
	)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	for _, m := range msgs {
		if _, err := tx.Exec(
			`INSERT INTO spec_messages (spec_id, seq, msg_id, kind, key, version, replica_id)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			id, m.Seq, m.MsgID, m.Kind, m.Key, m.Version, m.ReplicaID,
		); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return id, nil
}

// GetSpec 按 ID 查询规格。
func (s *SpecStore) GetSpec(id int64) (*model.Spec, error) {
	row := s.db.QueryRow(
		`SELECT id, scenario_id, name, message_hash, status, created_at FROM specs WHERE id = ?`, id)
	var spec model.Spec
	var ts string
	if err := row.Scan(&spec.ID, &spec.ScenarioID, &spec.Name, &spec.MessageHash, &spec.Status, &ts); err != nil {
		if err == sql.ErrNoRows {
			return nil, model.ErrNotFound
		}
		return nil, err
	}
	spec.CreatedAt, _ = time.Parse(time.RFC3339Nano, ts)
	return &spec, nil
}

// ListSpecs 返回全部规格。
func (s *SpecStore) ListSpecs() ([]model.Spec, error) {
	rows, err := s.db.Query(`SELECT id, scenario_id, name, message_hash, status, created_at FROM specs ORDER BY id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Spec
	for rows.Next() {
		var spec model.Spec
		var ts string
		if err := rows.Scan(&spec.ID, &spec.ScenarioID, &spec.Name, &spec.MessageHash, &spec.Status, &ts); err != nil {
			return nil, err
		}
		spec.CreatedAt, _ = time.Parse(time.RFC3339Nano, ts)
		out = append(out, spec)
	}
	return out, rows.Err()
}

// FreezeSpec 将规格置为 frozen。
func (s *SpecStore) FreezeSpec(id int64) error {
	res, err := s.db.Exec(`UPDATE specs SET status = ? WHERE id = ?`, string(model.SpecFrozen), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return model.ErrNotFound
	}
	return nil
}

// ListSpecMessages 列出规格保存的完整消息序列。
func (s *SpecStore) ListSpecMessages(specID int64) ([]model.SpecMessage, error) {
	rows, err := s.db.Query(
		`SELECT spec_id, seq, msg_id, kind, key, version, replica_id FROM spec_messages WHERE spec_id = ? ORDER BY seq`,
		specID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.SpecMessage
	for rows.Next() {
		var m model.SpecMessage
		if err := rows.Scan(&m.SpecID, &m.Seq, &m.MsgID, &m.Kind, &m.Key, &m.Version, &m.ReplicaID); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// SpecCount 统计规格总数。
func (s *SpecStore) SpecCount() (int, error) {
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM specs`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}
