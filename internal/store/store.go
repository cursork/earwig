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
	dec, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(512*1024*1024))
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

// maxBlobSize is the maximum uncompressed blob size we'll accept from the DB.
// This prevents decompression bombs (a small compressed blob expanding to
// gigabytes) and rejects obviously corrupt size values.
const maxBlobSize = 512 * 1024 * 1024 // 512MB

func (s *Store) GetBlob(hash string) ([]byte, error) {
	var data []byte
	var encoding string
	var size int64
	err := s.db.QueryRow(`SELECT data, encoding, size FROM blobs WHERE hash = ?`, hash).Scan(&data, &encoding, &size)
	if err != nil {
		return nil, fmt.Errorf("getting blob %s: %w", hash, err)
	}

	if size > maxBlobSize {
		return nil, fmt.Errorf("blob %s: stored size %d exceeds maximum %d", hash, size, maxBlobSize)
	}

	if encoding == "zstd" {
		decoded, err := s.zstdDec.DecodeAll(data, make([]byte, 0, size))
		if err != nil {
			return nil, fmt.Errorf("decompressing blob %s: %w", hash, err)
		}
		if int64(len(decoded)) != size {
			return nil, fmt.Errorf("blob %s: decompressed size %d != stored size %d", hash, len(decoded), size)
		}
		data = decoded
	}

	// Verify content hash to detect DB corruption or tampering.
	actual := sha256.Sum256(data)
	if hex.EncodeToString(actual[:]) != hash {
		return nil, fmt.Errorf("blob %s: integrity check failed (actual hash %s)", hash, hex.EncodeToString(actual[:]))
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
		b.WriteByte(':')
		fileType := f.Type
		if fileType == "" {
			fileType = "file"
		}
		b.WriteString(fileType)
		b.WriteByte(':')
		fmt.Fprintf(&b, "%o", f.Mode)
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
		if fileType != "file" && fileType != "symlink" {
			return nil, fmt.Errorf("invalid file type %q for %s", fileType, f.Path)
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
		var parseErr error
		snap.CreatedAt, parseErr = time.Parse(time.RFC3339, createdAt)
		if parseErr != nil {
			return nil, fmt.Errorf("corrupt timestamp %q in snapshot %s: %w", createdAt, snap.Hash, parseErr)
		}
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
		var parseErr error
		f.ModTime, parseErr = time.Parse(time.RFC3339, modTime)
		if parseErr != nil {
			return nil, fmt.Errorf("corrupt mod_time %q for %s: %w", modTime, f.Path, parseErr)
		}
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
		var parseErr error
		snap.CreatedAt, parseErr = time.Parse(time.RFC3339, createdAt)
		if parseErr != nil {
			return nil, fmt.Errorf("corrupt timestamp %q in snapshot %d: %w", createdAt, snap.ID, parseErr)
		}
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
	var parseErr error
	snap.CreatedAt, parseErr = time.Parse(time.RFC3339, createdAt)
	if parseErr != nil {
		return nil, fmt.Errorf("corrupt timestamp %q in snapshot %d: %w", createdAt, snap.ID, parseErr)
	}
	return &snap, nil
}

// GarbageCollect removes blobs not referenced by any snapshot_files row.
// Returns the number of orphaned blobs deleted.
func (s *Store) GarbageCollect() (int64, error) {
	result, err := s.db.Exec(`DELETE FROM blobs WHERE hash NOT IN (SELECT DISTINCT blob_hash FROM snapshot_files)`)
	if err != nil {
		return 0, fmt.Errorf("garbage collecting blobs: %w", err)
	}
	return result.RowsAffected()
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
