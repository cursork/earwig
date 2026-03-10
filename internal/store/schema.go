package store

const schemaSQL = `
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS blobs (
    hash     TEXT PRIMARY KEY,
    size     INTEGER NOT NULL,
    data     BLOB NOT NULL,
    encoding TEXT NOT NULL DEFAULT 'raw'
);

CREATE TABLE IF NOT EXISTS snapshots (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    hash       TEXT NOT NULL UNIQUE,
    parent_id  INTEGER REFERENCES snapshots(id),
    created_at TEXT NOT NULL,
    message    TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS snapshot_files (
    snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),
    path        TEXT NOT NULL,
    blob_hash   TEXT NOT NULL REFERENCES blobs(hash),
    mode        INTEGER NOT NULL,
    mod_time    TEXT NOT NULL,
    size        INTEGER NOT NULL,
    type        TEXT NOT NULL DEFAULT 'file',
    PRIMARY KEY (snapshot_id, path)
);

CREATE TABLE IF NOT EXISTS checkpoints (
    name        TEXT PRIMARY KEY,
    snapshot_id INTEGER NOT NULL REFERENCES snapshots(id)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_parent ON snapshots(parent_id);
CREATE INDEX IF NOT EXISTS idx_snapshot_files_blob ON snapshot_files(blob_hash);
CREATE INDEX IF NOT EXISTS idx_checkpoints_snapshot ON checkpoints(snapshot_id);
`

func (s *Store) migrate() error {
	_, err := s.db.Exec(schemaSQL)
	if err != nil {
		return err
	}

	// Check schema version and apply migrations
	var version string
	err = s.db.QueryRow(`SELECT value FROM meta WHERE key = 'schema_version'`).Scan(&version)
	if err != nil {
		// First run — set version
		_, err = s.db.Exec(`INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', '4')`)
		return err
	}

	if version == "1" {
		// v1 -> v2: add type column to snapshot_files
		var count int
		if err := s.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('snapshot_files') WHERE name='type'`).Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			if _, err = s.db.Exec(`ALTER TABLE snapshot_files ADD COLUMN type TEXT NOT NULL DEFAULT 'file'`); err != nil {
				return err
			}
		}
		version = "2"
		if _, err = s.db.Exec(`UPDATE meta SET value = '2' WHERE key = 'schema_version'`); err != nil {
			return err
		}
	}

	if version == "2" {
		// v2 -> v3: add encoding column to blobs
		var count int
		if err := s.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('blobs') WHERE name='encoding'`).Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			if _, err = s.db.Exec(`ALTER TABLE blobs ADD COLUMN encoding TEXT NOT NULL DEFAULT 'raw'`); err != nil {
				return err
			}
		}
		version = "3"
		if _, err = s.db.Exec(`UPDATE meta SET value = '3' WHERE key = 'schema_version'`); err != nil {
			return err
		}
	}

	if version == "3" {
		// v3 -> v4: add checkpoints table
		var count int
		if err := s.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='checkpoints'`).Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			if _, err = s.db.Exec(`CREATE TABLE checkpoints (name TEXT PRIMARY KEY, snapshot_id INTEGER NOT NULL REFERENCES snapshots(id))`); err != nil {
				return err
			}
			if _, err = s.db.Exec(`CREATE INDEX idx_checkpoints_snapshot ON checkpoints(snapshot_id)`); err != nil {
				return err
			}
		}
		if _, err = s.db.Exec(`UPDATE meta SET value = '4' WHERE key = 'schema_version'`); err != nil {
			return err
		}
	}

	return nil
}
