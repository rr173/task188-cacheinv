package store

import (
	"database/sql"
	"fmt"
	"time"

	"task188-cacheinv/internal/model"
)

// ScenarioStore 负责 scenarios / messages / replay_events / violations / proofs 的持久化。
type ScenarioStore struct{ db *sql.DB }

func NewScenarioStore(db *sql.DB) *ScenarioStore { return &ScenarioStore{db: db} }

// CreateScenario 创建场景（editing 状态）。
func (s *ScenarioStore) CreateScenario(sc *model.Scenario) (int64, error) {
	res, err := s.db.Exec(
		`INSERT INTO scenarios (keyspace_id, name, fingerprint, status, cursor_pos, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		sc.KeyspaceID, sc.Name, sc.Fingerprint, string(sc.Status), sc.CursorPos,
		sc.CreatedAt.Format(time.RFC3339Nano),
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// GetScenario 按 ID 查询场景。
func (s *ScenarioStore) GetScenario(id int64) (*model.Scenario, error) {
	row := s.db.QueryRow(
		`SELECT id, keyspace_id, name, fingerprint, status, cursor_pos, created_at FROM scenarios WHERE id = ?`, id)
	var sc model.Scenario
	var ts string
	if err := row.Scan(&sc.ID, &sc.KeyspaceID, &sc.Name, &sc.Fingerprint, &sc.Status, &sc.CursorPos, &ts); err != nil {
		if err == sql.ErrNoRows {
			return nil, model.ErrNotFound
		}
		return nil, err
	}
	sc.CreatedAt, _ = time.Parse(time.RFC3339Nano, ts)
	return &sc, nil
}

// ListScenarios 返回全部场景。
func (s *ScenarioStore) ListScenarios() ([]model.Scenario, error) {
	rows, err := s.db.Query(`SELECT id, keyspace_id, name, fingerprint, status, cursor_pos, created_at FROM scenarios ORDER BY id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Scenario
	for rows.Next() {
		var sc model.Scenario
		var ts string
		if err := rows.Scan(&sc.ID, &sc.KeyspaceID, &sc.Name, &sc.Fingerprint, &sc.Status, &sc.CursorPos, &ts); err != nil {
			return nil, err
		}
		sc.CreatedAt, _ = time.Parse(time.RFC3339Nano, ts)
		out = append(out, sc)
	}
	return out, rows.Err()
}

// FindScenarioByFingerprint 按指纹查场景（相同场景指纹复用结果）。
func (s *ScenarioStore) FindScenarioByFingerprint(keyspaceID, fingerprint string) (*model.Scenario, error) {
	row := s.db.QueryRow(
		`SELECT id, keyspace_id, name, fingerprint, status, cursor_pos, created_at FROM scenarios
		 WHERE keyspace_id = ? AND fingerprint = ? ORDER BY id LIMIT 1`, keyspaceID, fingerprint)
	var sc model.Scenario
	var ts string
	if err := row.Scan(&sc.ID, &sc.KeyspaceID, &sc.Name, &sc.Fingerprint, &sc.Status, &sc.CursorPos, &ts); err != nil {
		if err == sql.ErrNoRows {
			return nil, model.ErrNotFound
		}
		return nil, err
	}
	sc.CreatedAt, _ = time.Parse(time.RFC3339Nano, ts)
	return &sc, nil
}

// UpdateScenarioStatus 更新场景状态与游标。
func (s *ScenarioStore) UpdateScenarioStatus(id int64, status model.ScenarioStatus, cursorPos int) error {
	_, err := s.db.Exec(`UPDATE scenarios SET status = ?, cursor_pos = ? WHERE id = ?`, string(status), cursorPos, id)
	return err
}

// ScenarioCount 统计场景总数。
func (s *ScenarioStore) ScenarioCount() (int, error) {
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM scenarios`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// InsertMessage 追加一条消息（逻辑时钟由调用方分配 id）。
func (s *ScenarioStore) InsertMessage(m *model.Message) error {
	_, err := s.db.Exec(
		`INSERT INTO messages (id, scenario_id, msg_id, kind, key, version, replica_id, content_hash, status, retry_count, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.ID, m.ScenarioID, m.MsgID, string(m.Kind), m.Key, m.Version, m.ReplicaID,
		m.ContentHash, string(m.Status), m.RetryCount, m.CreatedAt.Format(time.RFC3339Nano),
	)
	return err
}

// UpdateMessageStatus 更新消息状态。
func (s *ScenarioStore) UpdateMessageStatus(id int64, status model.MessageStatus) error {
	_, err := s.db.Exec(`UPDATE messages SET status = ? WHERE id = ?`, string(status), id)
	return err
}

// UpdateMessageStatusAndRetry 更新消息状态与重试次数。
// retry 即本次应持久化的重试投递计数，按原值写入，不额外偏移，确保与实际投递次数一致。
func (s *ScenarioStore) UpdateMessageStatusAndRetry(id int64, status model.MessageStatus, retry int) error {
	_, err := s.db.Exec(`UPDATE messages SET status = ?, retry_count = ? WHERE id = ?`, string(status), retry, id)
	return err
}

// ListMessages 列出场景全部消息（按 id 即逻辑时钟排序）。
func (s *ScenarioStore) ListMessages(scenarioID int64) ([]model.Message, error) {
	rows, err := s.db.Query(
		`SELECT id, scenario_id, msg_id, kind, key, version, replica_id, content_hash, status, retry_count, created_at
		 FROM messages WHERE scenario_id = ? ORDER BY id`, scenarioID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Message
	for rows.Next() {
		var m model.Message
		var ts string
		if err := rows.Scan(&m.ID, &m.ScenarioID, &m.MsgID, &m.Kind, &m.Key, &m.Version,
			&m.ReplicaID, &m.ContentHash, &m.Status, &m.RetryCount, &ts); err != nil {
			return nil, err
		}
		m.CreatedAt, _ = time.Parse(time.RFC3339Nano, ts)
		out = append(out, m)
	}
	return out, rows.Err()
}

// NextMessageID 分配下一个逻辑时钟序号（场景内串行调用）。
func (s *ScenarioStore) NextMessageID() (int64, error) {
	var max sql.NullInt64
	if err := s.db.QueryRow(`SELECT COALESCE(MAX(id), 0) FROM messages`).Scan(&max); err != nil {
		return 0, err
	}
	return max.Int64 + 1, nil
}

// AppendReplayEvent 记录回放轨迹一步。
func (s *ScenarioStore) AppendReplayEvent(e *model.ReplayEvent) error {
	_, err := s.db.Exec(
		`INSERT INTO replay_events (scenario_id, seq, step, detail, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		e.ScenarioID, e.Seq, e.Step, e.Detail, e.CreatedAt.Format(time.RFC3339Nano),
	)
	return err
}

// ListReplayEvents 列出场景回放轨迹。
func (s *ScenarioStore) ListReplayEvents(scenarioID int64) ([]model.ReplayEvent, error) {
	rows, err := s.db.Query(`SELECT id, scenario_id, seq, step, detail, created_at FROM replay_events WHERE scenario_id = ? ORDER BY seq`, scenarioID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.ReplayEvent
	for rows.Next() {
		var e model.ReplayEvent
		var ts string
		if err := rows.Scan(&e.ID, &e.ScenarioID, &e.Seq, &e.Step, &e.Detail, &ts); err != nil {
			return nil, err
		}
		e.CreatedAt, _ = time.Parse(time.RFC3339Nano, ts)
		out = append(out, e)
	}
	return out, rows.Err()
}

// AppendViolation 记录违反步骤。
func (s *ScenarioStore) AppendViolation(v *model.Violation) error {
	_, err := s.db.Exec(
		`INSERT INTO violations (scenario_id, step_seq, kind, message, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		v.ScenarioID, v.StepSeq, v.Kind, v.Message, v.CreatedAt.Format(time.RFC3339Nano),
	)
	return err
}

// ListViolations 列出场景违反步骤。
func (s *ScenarioStore) ListViolations(scenarioID int64) ([]model.Violation, error) {
	rows, err := s.db.Query(`SELECT id, scenario_id, step_seq, kind, message, created_at FROM violations WHERE scenario_id = ? ORDER BY step_seq`, scenarioID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Violation
	for rows.Next() {
		var v model.Violation
		var ts string
		if err := rows.Scan(&v.ID, &v.ScenarioID, &v.StepSeq, &v.Kind, &v.Message, &ts); err != nil {
			return nil, err
		}
		v.CreatedAt, _ = time.Parse(time.RFC3339Nano, ts)
		out = append(out, v)
	}
	return out, rows.Err()
}

// ViolationCount 统计违反总数。
func (s *ScenarioStore) ViolationCount() (int, error) {
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM violations`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// InsertProof 写入收敛证明。
func (s *ScenarioStore) InsertProof(p *model.Proof) (int64, error) {
	res, err := s.db.Exec(
		`INSERT INTO proofs (scenario_id, conclusion, detail, unconverged, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		p.ScenarioID, p.Conclusion, p.Detail, MarshalUnconverged(p.Unconverged), p.CreatedAt.Format(time.RFC3339Nano),
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// GetProof 查询场景最新证明。
func (s *ScenarioStore) GetProof(scenarioID int64) (*model.Proof, error) {
	row := s.db.QueryRow(
		`SELECT id, scenario_id, conclusion, detail, unconverged, created_at FROM proofs WHERE scenario_id = ? ORDER BY id DESC LIMIT 1`,
		scenarioID)
	var p model.Proof
	var ts, unconv string
	if err := row.Scan(&p.ID, &p.ScenarioID, &p.Conclusion, &p.Detail, &unconv, &ts); err != nil {
		if err == sql.ErrNoRows {
			return nil, model.ErrNotFound
		}
		return nil, err
	}
	p.Unconverged = UnmarshalUnconverged(unconv)
	p.CreatedAt, _ = time.Parse(time.RFC3339Nano, ts)
	return &p, nil
}

// ProofCount 统计证明总数。
func (s *ScenarioStore) ProofCount() (int, error) {
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM proofs`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// MessageCount 统计消息总数。
func (s *ScenarioStore) MessageCount() (int, error) {
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// WithTx 在事务内执行 fn。
func (s *ScenarioStore) WithTx(fn func(tx *sql.Tx) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return fmt.Errorf("tx: %w", err)
	}
	return tx.Commit()
}
