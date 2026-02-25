package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	_ "modernc.org/sqlite"
)


type Store struct {
	db      *sql.DB
	zstdEnc *zstd.Encoder
	zstdDec *zstd.Decoder
}

func Open(dbPath string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("creating zstd encoder: %w", err)
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		db.Close()
		enc.Close()
		return nil, fmt.Errorf("creating zstd decoder: %w", err)
	}

	s := &Store{db: db, zstdEnc: enc, zstdDec: dec}
	if err := s.migrate(); err != nil {
		db.Close()
		enc.Close()
		dec.Close()
		return nil, fmt.Errorf("migrating database: %w", err)
	}
	return s, nil
}

func (s *Store) Close() error {
	var errs []error
	if err := s.zstdEnc.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing zstd encoder: %w", err))
	}
	s.zstdDec.Close()
	if err := s.db.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing database: %w", err))
	}
	return errors.Join(errs...)
}

func (s *Store) PutBlob(data []byte) (string, error) {
	h := sha256.Sum256(data)
	hash := hex.EncodeToString(h[:])

	stored := data
	encoding := "raw"
	if len(data) >= 128*1024 {
		compressed := s.zstdEnc.EncodeAll(data, make([]byte, 0, len(data)/2))
		if len(compressed) < len(data) {
			stored = compressed
			encoding = "zstd"
		}
	}

	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO blobs (hash, size, data, encoding) VALUES (?, ?, ?, ?)`,
		hash, len(data), stored, encoding,
	)
	if err != nil {
		return "", fmt.Errorf("storing blob: %w", err)
	}
	return hash, nil
}

func (s *Store) GetBlob(hash string) ([]byte, error) {
	var data []byte
	var encoding string
	err := s.db.QueryRow(`SELECT data, encoding FROM blobs WHERE hash = ?`, hash).Scan(&data, &encoding)
	if err != nil {
		return nil, fmt.Errorf("getting blob %s: %w", hash, err)
	}
	if encoding == "zstd" {
		data, err = s.zstdDec.DecodeAll(data, nil)
		if err != nil {
			return nil, fmt.Errorf("decompressing blob %s: %w", hash, err)
		}
	}
	return data, nil
}

func (s *Store) CreateSnapshot(parentID *int64, files []SnapshotFile, message string) (*Snapshot, error) {
	// Compute snapshot hash from sorted file manifest
	sorted := make([]SnapshotFile, len(files))
	copy(sorted, files)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Path < sorted[j].Path })

	now := time.Now().UTC()

	var b strings.Builder
	// Include parent and timestamp so every snapshot is unique,
	// even if the same state is reached from the same parent twice.
	b.WriteString("time:")
	b.WriteString(now.Format(time.RFC3339Nano))
	b.WriteByte('\n')
	if parentID != nil {
		var parentHash string
		if err := s.db.QueryRow(`SELECT hash FROM snapshots WHERE id = ?`, *parentID).Scan(&parentHash); err != nil {
			return nil, fmt.Errorf("getting parent hash: %w", err)
		}
		b.WriteString("parent:")
		b.WriteString(parentHash)
		b.WriteByte('\n')
	}
	for _, f := range sorted {
		b.WriteString(f.Path)
		b.WriteByte(':')
		b.WriteString(f.BlobHash)
		b.WriteByte('\n')
	}
	h := sha256.Sum256([]byte(b.String()))
	hash := hex.EncodeToString(h[:])

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	res, err := tx.Exec(
		`INSERT INTO snapshots (hash, parent_id, created_at, message) VALUES (?, ?, ?, ?)`,
		hash, parentID, now.Format(time.RFC3339), message,
	)
	if err != nil {
		return nil, fmt.Errorf("inserting snapshot: %w", err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}

	stmt, err := tx.Prepare(
		`INSERT INTO snapshot_files (snapshot_id, path, blob_hash, mode, mod_time, size, type) VALUES (?, ?, ?, ?, ?, ?, ?)`,
	)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	for _, f := range files {
		fileType := f.Type
		if fileType == "" {
			fileType = "file"
		}
		_, err := stmt.Exec(id, f.Path, f.BlobHash, f.Mode, f.ModTime.Format(time.RFC3339), f.Size, fileType)
		if err != nil {
			return nil, fmt.Errorf("inserting snapshot file %s: %w", f.Path, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &Snapshot{
		ID:        id,
		Hash:      hash,
		ParentID:  parentID,
		CreatedAt: now,
		Message:   message,
	}, nil
}

func (s *Store) GetSnapshot(hashPrefix string) (*Snapshot, error) {
	// Escape LIKE wildcards so user-provided prefixes are treated literally
	escaped := strings.NewReplacer("%", "\\%", "_", "\\_").Replace(hashPrefix)
	rows, err := s.db.Query(
		`SELECT id, hash, parent_id, created_at, message FROM snapshots WHERE hash LIKE ? ESCAPE '\' LIMIT 2`,
		escaped+"%",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []Snapshot
	for rows.Next() {
		var snap Snapshot
		var parentID sql.NullInt64
		var createdAt string
		if err := rows.Scan(&snap.ID, &snap.Hash, &parentID, &createdAt, &snap.Message); err != nil {
			return nil, err
		}
		if parentID.Valid {
			snap.ParentID = &parentID.Int64
		}
		snap.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
		results = append(results, snap)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("snapshot not found: %s", hashPrefix)
	}
	if len(results) > 1 {
		return nil, fmt.Errorf("ambiguous snapshot prefix: %s", hashPrefix)
	}
	return &results[0], nil
}

func (s *Store) GetSnapshotFiles(snapshotID int64) ([]SnapshotFile, error) {
	rows, err := s.db.Query(
		`SELECT path, blob_hash, mode, mod_time, size, type FROM snapshot_files WHERE snapshot_id = ? ORDER BY path`,
		snapshotID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []SnapshotFile
	for rows.Next() {
		var f SnapshotFile
		var modTime string
		f.SnapshotID = snapshotID
		if err := rows.Scan(&f.Path, &f.BlobHash, &f.Mode, &modTime, &f.Size, &f.Type); err != nil {
			return nil, err
		}
		f.ModTime, _ = time.Parse(time.RFC3339, modTime)
		files = append(files, f)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return files, nil
}

func (s *Store) ListSnapshots() ([]Snapshot, error) {
	rows, err := s.db.Query(
		`SELECT id, hash, parent_id, created_at, message FROM snapshots ORDER BY id`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var snapshots []Snapshot
	for rows.Next() {
		var snap Snapshot
		var parentID sql.NullInt64
		var createdAt string
		if err := rows.Scan(&snap.ID, &snap.Hash, &parentID, &createdAt, &snap.Message); err != nil {
			return nil, err
		}
		if parentID.Valid {
			snap.ParentID = &parentID.Int64
		}
		snap.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
		snapshots = append(snapshots, snap)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return snapshots, nil
}

func (s *Store) GetLatestSnapshot() (*Snapshot, error) {
	var snap Snapshot
	var parentID sql.NullInt64
	var createdAt string
	err := s.db.QueryRow(
		`SELECT id, hash, parent_id, created_at, message FROM snapshots ORDER BY id DESC LIMIT 1`,
	).Scan(&snap.ID, &snap.Hash, &parentID, &createdAt, &snap.Message)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if parentID.Valid {
		snap.ParentID = &parentID.Int64
	}
	snap.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
	return &snap, nil
}

func (s *Store) DiffSnapshots(oldID, newID int64) ([]FileChange, error) {
	oldFiles, err := s.GetSnapshotFiles(oldID)
	if err != nil {
		return nil, err
	}
	newFiles, err := s.GetSnapshotFiles(newID)
	if err != nil {
		return nil, err
	}

	oldMap := make(map[string]SnapshotFile, len(oldFiles))
	for _, f := range oldFiles {
		oldMap[f.Path] = f
	}
	newMap := make(map[string]SnapshotFile, len(newFiles))
	for _, f := range newFiles {
		newMap[f.Path] = f
	}

	var changes []FileChange

	// Added or modified
	for path, nf := range newMap {
		of, exists := oldMap[path]
		if !exists {
			changes = append(changes, FileChange{Path: path, Type: ChangeAdded, NewHash: nf.BlobHash})
		} else if of.BlobHash != nf.BlobHash || of.Type != nf.Type || of.Mode != nf.Mode {
			changes = append(changes, FileChange{Path: path, Type: ChangeModified, OldHash: of.BlobHash, NewHash: nf.BlobHash})
		}
	}

	// Deleted
	for path, of := range oldMap {
		if _, exists := newMap[path]; !exists {
			changes = append(changes, FileChange{Path: path, Type: ChangeDeleted, OldHash: of.BlobHash})
		}
	}

	sort.Slice(changes, func(i, j int) bool { return changes[i].Path < changes[j].Path })
	return changes, nil
}
