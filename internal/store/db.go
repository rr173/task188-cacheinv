// Package store 提供 SQLite 持久化：建表迁移与各领域仓储。
package store

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

// Options 是打开数据库的选项。
type Options struct {
	Path string
}

// Store 封装 SQLite 连接与迁移后的表结构。
// 逻辑时钟（消息 id）由 messages.id 的 AUTOINCREMENT 在 INSERT 时原子分配，
// 无需应用层互斥；并发写入的串行化交给 SQLite 单写者语义 + busy_timeout。
type Store struct {
	db *sql.DB
}

// Open 打开（或创建）SQLite 数据库并执行幂等迁移。
func Open(opts Options) (*Store, error) {
	if opts.Path == "" {
		opts.Path = "cacheinv.db"
	}
	db, err := sql.Open("sqlite", opts.Path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, fmt.Errorf("sql open: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("sql ping: %w", err)
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

// Close 关闭数据库连接。
func (s *Store) Close() error { return s.db.Close() }

// DB 暴露底层句柄供仓储与事务使用。
func (s *Store) DB() *sql.DB { return s.db }

// migrate 幂等建表。
func (s *Store) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS keyspaces (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			description TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS replicas (
			id TEXT PRIMARY KEY,
			keyspace_id TEXT NOT NULL REFERENCES keyspaces(id),
			name TEXT NOT NULL,
			status TEXT NOT NULL,
			lease_seq INTEGER NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS protocol_params (
			keyspace_id TEXT PRIMARY KEY REFERENCES keyspaces(id),
			max_retries INTEGER NOT NULL,
			lease_ttl_ms INTEGER NOT NULL,
			convergence_timeout_ms INTEGER NOT NULL,
			ordering TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS source_versions (
			keyspace_id TEXT NOT NULL,
			key TEXT NOT NULL,
			version INTEGER NOT NULL,
			value_hash TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			PRIMARY KEY (keyspace_id, key)
		)`,
		`CREATE TABLE IF NOT EXISTS replica_key_versions (
			replica_id TEXT NOT NULL,
			key TEXT NOT NULL,
			version INTEGER NOT NULL,
			status TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			PRIMARY KEY (replica_id, key)
		)`,
		`CREATE TABLE IF NOT EXISTS scenarios (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			keyspace_id TEXT NOT NULL REFERENCES keyspaces(id),
			name TEXT NOT NULL,
			fingerprint TEXT NOT NULL,
			status TEXT NOT NULL,
			cursor_pos INTEGER NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS messages (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			scenario_id INTEGER NOT NULL REFERENCES scenarios(id),
			msg_id TEXT NOT NULL,
			kind TEXT NOT NULL,
			key TEXT NOT NULL,
			version INTEGER NOT NULL,
			replica_id TEXT NOT NULL,
			content_hash TEXT NOT NULL,
			status TEXT NOT NULL,
			retry_count INTEGER NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL,
			UNIQUE (scenario_id, msg_id)
		)`,
		`CREATE TABLE IF NOT EXISTS replay_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			scenario_id INTEGER NOT NULL REFERENCES scenarios(id),
			seq INTEGER NOT NULL,
			step TEXT NOT NULL,
			detail TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS violations (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			scenario_id INTEGER NOT NULL REFERENCES scenarios(id),
			step_seq INTEGER NOT NULL,
			kind TEXT NOT NULL,
			message TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS proofs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			scenario_id INTEGER NOT NULL REFERENCES scenarios(id),
			conclusion TEXT NOT NULL,
			detail TEXT NOT NULL,
			unconverged TEXT NOT NULL DEFAULT '[]',
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS specs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			scenario_id INTEGER NOT NULL REFERENCES scenarios(id),
			name TEXT NOT NULL,
			message_hash TEXT NOT NULL,
			status TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS spec_messages (
			spec_id INTEGER NOT NULL REFERENCES specs(id),
			seq INTEGER NOT NULL,
			msg_id TEXT NOT NULL,
			kind TEXT NOT NULL,
			key TEXT NOT NULL,
			version INTEGER NOT NULL,
			replica_id TEXT NOT NULL,
			PRIMARY KEY (spec_id, seq)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_scenario ON messages(scenario_id)`,
		`CREATE INDEX IF NOT EXISTS idx_scenarios_keyspace ON scenarios(keyspace_id)`,
		`CREATE INDEX IF NOT EXISTS idx_replica_versions_key ON replica_key_versions(replica_id)`,
		`CREATE INDEX IF NOT EXISTS idx_violations_scenario ON violations(scenario_id)`,
		`CREATE INDEX IF NOT EXISTS idx_events_scenario ON replay_events(scenario_id)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("exec %q: %w", stmt, err)
		}
	}
	return nil
}
