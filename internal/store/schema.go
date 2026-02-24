package store

const schemaSQL = `
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS blobs (
    hash TEXT PRIMARY KEY,
    size INTEGER NOT NULL,
    data BLOB NOT NULL
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
    PRIMARY KEY (snapshot_id, path)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_parent ON snapshots(parent_id);
CREATE INDEX IF NOT EXISTS idx_snapshot_files_blob ON snapshot_files(blob_hash);
`

func (s *Store) migrate() error {
	_, err := s.db.Exec(schemaSQL)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', '1')`)
	return err
}
