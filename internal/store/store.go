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
		return nil, errors.Join(fmt.Errorf("pinging database: %w", err), db.Close())
	}

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, errors.Join(fmt.Errorf("creating zstd encoder: %w", err), db.Close())
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(512*1024*1024))
	if err != nil {
		return nil, errors.Join(fmt.Errorf("creating zstd decoder: %w", err), enc.Close(), db.Close())
	}

	s := &Store{db: db, zstdEnc: enc, zstdDec: dec}
	if err := s.migrate(); err != nil {
		dec.Close()
		return nil, errors.Join(fmt.Errorf("migrating database: %w", err), enc.Close(), db.Close())
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
	hash := computeBlobHash(data)

	stored := data
	compressed := data // default: no compression attempted
	if len(data) >= 128*1024 {
		compressed = s.zstdEnc.EncodeAll(data, make([]byte, 0, len(data)/2))
	}
	encoding, useCompressed := chooseEncoding(len(data), len(compressed))
	if useCompressed {
		stored = compressed
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

	// Reject unknown encodings (a crafted DB with encoding="garbage" would
	// skip both the "raw" and "zstd" branches below).
	if err := validateEncoding(encoding); err != nil {
		return nil, fmt.Errorf("blob %s: %w", hash, err)
	}

	if size > maxBlobSize {
		return nil, fmt.Errorf("blob %s: stored size %d exceeds maximum %d", hash, size, maxBlobSize)
	}

	if encoding == "raw" {
		if err := checkBlobSize(len(data), size, maxBlobSize); err != nil {
			return nil, fmt.Errorf("blob %s: %w", hash, err)
		}
	}

	if encoding == "zstd" {
		decoded, err := s.zstdDec.DecodeAll(data, make([]byte, 0, size))
		if err != nil {
			return nil, fmt.Errorf("decompressing blob %s: %w", hash, err)
		}
		if err := checkBlobSize(len(decoded), size, maxBlobSize); err != nil {
			return nil, fmt.Errorf("blob %s: %w", hash, err)
		}
		data = decoded
	}

	// Verify content hash to detect DB corruption or tampering.
	actualHash := computeBlobHash(data)
	if actualHash != hash {
		return nil, fmt.Errorf("blob %s: integrity check failed (actual hash %s)", hash, actualHash)
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

	// Validate path consistency: no path should be a prefix of another
	// (e.g. "foo" and "foo/bar.txt" can't coexist — "foo" can't be both
	// a file and a directory).
	paths := make([]string, len(sorted))
	for i, f := range sorted {
		paths[i] = f.Path
	}
	if err := validatePathConflicts(paths); err != nil {
		return nil, err
	}

	// Validate and collect file types.
	types := make([]string, len(files))
	for i, f := range files {
		t := f.Type
		if t == "" {
			t = "file"
		}
		types[i] = t
	}
	if err := validateFileTypes(types); err != nil {
		return nil, err
	}

	for i, f := range files {
		_, err := stmt.Exec(id, f.Path, f.BlobHash, f.Mode, f.ModTime.Format(time.RFC3339), f.Size, types[i])
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

func (s *Store) GetSnapshotByID(id int64) (*Snapshot, error) {
	var snap Snapshot
	var parentID sql.NullInt64
	var createdAt string
	err := s.db.QueryRow(
		`SELECT id, hash, parent_id, created_at, message FROM snapshots WHERE id = ?`, id,
	).Scan(&snap.ID, &snap.Hash, &parentID, &createdAt, &snap.Message)
	if err != nil {
		return nil, fmt.Errorf("snapshot %d not found: %w", id, err)
	}
	if parentID.Valid {
		snap.ParentID = &parentID.Int64
	}
	var parseErr error
	snap.CreatedAt, parseErr = time.Parse(time.RFC3339, createdAt)
	if parseErr != nil {
		return nil, fmt.Errorf("corrupt timestamp %q in snapshot %d: %w", createdAt, id, parseErr)
	}
	return &snap, nil
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

// DeleteSnapshot removes a snapshot and its file entries.
// Child snapshots that reference this one as parent are re-parented to this
// snapshot's parent (or NULL if this was a root snapshot).
func (s *Store) DeleteSnapshot(id int64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Get this snapshot's parent so we can re-parent children.
	var parentID sql.NullInt64
	if err := tx.QueryRow(`SELECT parent_id FROM snapshots WHERE id = ?`, id).Scan(&parentID); err != nil {
		return fmt.Errorf("snapshot %d not found: %w", id, err)
	}

	// Re-parent children to this snapshot's parent.
	if parentID.Valid {
		_, err = tx.Exec(`UPDATE snapshots SET parent_id = ? WHERE parent_id = ?`, parentID.Int64, id)
	} else {
		_, err = tx.Exec(`UPDATE snapshots SET parent_id = NULL WHERE parent_id = ?`, id)
	}
	if err != nil {
		return fmt.Errorf("re-parenting children: %w", err)
	}

	if _, err := tx.Exec(`DELETE FROM checkpoints WHERE snapshot_id = ?`, id); err != nil {
		return fmt.Errorf("deleting checkpoints: %w", err)
	}
	if _, err := tx.Exec(`DELETE FROM snapshot_files WHERE snapshot_id = ?`, id); err != nil {
		return fmt.Errorf("deleting snapshot files: %w", err)
	}
	if _, err := tx.Exec(`DELETE FROM snapshots WHERE id = ?`, id); err != nil {
		return fmt.Errorf("deleting snapshot: %w", err)
	}

	return tx.Commit()
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

// ResolveRef looks up a ref string: first as a checkpoint name, then as a hash prefix.
func (s *Store) ResolveRef(ref string) (*Snapshot, error) {
	snap, err := s.GetCheckpoint(ref)
	if err == nil {
		return snap, nil
	}
	// Fall through to hash prefix lookup
	snap, err = s.GetSnapshot(ref)
	if err != nil {
		return nil, fmt.Errorf("no checkpoint or snapshot matching: %s", ref)
	}
	return snap, nil
}

// SetCheckpoint creates a new checkpoint. Returns error if name already exists.
func (s *Store) SetCheckpoint(name string, snapshotID int64) error {
	_, err := s.db.Exec(`INSERT INTO checkpoints (name, snapshot_id) VALUES (?, ?)`, name, snapshotID)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint") || strings.Contains(err.Error(), "PRIMARY KEY") {
			return fmt.Errorf("checkpoint %q already exists (use -u to move it)", name)
		}
		return fmt.Errorf("creating checkpoint: %w", err)
	}
	return nil
}

// UpdateCheckpoint moves an existing checkpoint to a new snapshot.
func (s *Store) UpdateCheckpoint(name string, snapshotID int64) error {
	result, err := s.db.Exec(`UPDATE checkpoints SET snapshot_id = ? WHERE name = ?`, snapshotID, name)
	if err != nil {
		return fmt.Errorf("updating checkpoint: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("checkpoint not found: %s", name)
	}
	return nil
}

// DeleteCheckpoint removes a checkpoint by name.
func (s *Store) DeleteCheckpoint(name string) error {
	result, err := s.db.Exec(`DELETE FROM checkpoints WHERE name = ?`, name)
	if err != nil {
		return fmt.Errorf("deleting checkpoint: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("checkpoint not found: %s", name)
	}
	return nil
}

// GetCheckpoint looks up a checkpoint by name and returns the referenced snapshot.
func (s *Store) GetCheckpoint(name string) (*Snapshot, error) {
	var snap Snapshot
	var parentID sql.NullInt64
	var createdAt string
	err := s.db.QueryRow(
		`SELECT s.id, s.hash, s.parent_id, s.created_at, s.message
		 FROM checkpoints c JOIN snapshots s ON c.snapshot_id = s.id
		 WHERE c.name = ?`, name,
	).Scan(&snap.ID, &snap.Hash, &parentID, &createdAt, &snap.Message)
	if err != nil {
		return nil, fmt.Errorf("checkpoint not found: %s", name)
	}
	if parentID.Valid {
		snap.ParentID = &parentID.Int64
	}
	var parseErr error
	snap.CreatedAt, parseErr = time.Parse(time.RFC3339, createdAt)
	if parseErr != nil {
		return nil, fmt.Errorf("corrupt timestamp in checkpoint %s: %w", name, parseErr)
	}
	return &snap, nil
}

// ListCheckpoints returns all checkpoints ordered by name.
func (s *Store) ListCheckpoints() ([]Checkpoint, error) {
	rows, err := s.db.Query(
		`SELECT c.name, c.snapshot_id, s.hash, s.parent_id, s.created_at, s.message
		 FROM checkpoints c JOIN snapshots s ON c.snapshot_id = s.id
		 ORDER BY c.name`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var checkpoints []Checkpoint
	for rows.Next() {
		var cp Checkpoint
		var parentID sql.NullInt64
		var createdAt string
		if err := rows.Scan(&cp.Name, &cp.SnapshotID, &cp.Snapshot.Hash, &parentID, &createdAt, &cp.Snapshot.Message); err != nil {
			return nil, err
		}
		cp.Snapshot.ID = cp.SnapshotID
		if parentID.Valid {
			cp.Snapshot.ParentID = &parentID.Int64
		}
		var parseErr error
		cp.Snapshot.CreatedAt, parseErr = time.Parse(time.RFC3339, createdAt)
		if parseErr != nil {
			return nil, fmt.Errorf("corrupt timestamp in checkpoint %s: %w", cp.Name, parseErr)
		}
		checkpoints = append(checkpoints, cp)
	}
	return checkpoints, rows.Err()
}

// GetCheckpointsForSnapshot returns checkpoint names for a given snapshot ID.
func (s *Store) GetCheckpointsForSnapshot(snapshotID int64) ([]string, error) {
	rows, err := s.db.Query(`SELECT name FROM checkpoints WHERE snapshot_id = ? ORDER BY name`, snapshotID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// CheckpointsBySnapshot returns a map of snapshot ID to checkpoint names.
func (s *Store) CheckpointsBySnapshot() (map[int64][]string, error) {
	rows, err := s.db.Query(`SELECT snapshot_id, name FROM checkpoints ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	m := make(map[int64][]string)
	for rows.Next() {
		var snapID int64
		var name string
		if err := rows.Scan(&snapID, &name); err != nil {
			return nil, err
		}
		m[snapID] = append(m[snapID], name)
	}
	return m, rows.Err()
}

// BlobRefs returns all (blob_hash -> []BlobRef) mappings for files (not symlinks).
// If snapshotIDs is non-empty, only those snapshots are included.
// If maxSize > 0, blobs larger than maxSize bytes are excluded.
// Results are grouped by blob_hash for dedup searching.
func (s *Store) BlobRefs(snapshotIDs []int64, maxSize int64) (map[string][]BlobRef, error) {
	var conditions []string
	var args []interface{}

	conditions = append(conditions, "type = 'file'")

	if maxSize > 0 {
		conditions = append(conditions, "size <= ?")
		args = append(args, maxSize)
	}

	if len(snapshotIDs) > 0 {
		placeholders := make([]string, len(snapshotIDs))
		for i, id := range snapshotIDs {
			placeholders[i] = "?"
			args = append(args, id)
		}
		conditions = append(conditions, "snapshot_id IN ("+strings.Join(placeholders, ",")+")")
	}

	query := "SELECT blob_hash, snapshot_id, path FROM snapshot_files WHERE " + strings.Join(conditions, " AND ") + " ORDER BY blob_hash"
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	m := make(map[string][]BlobRef)
	for rows.Next() {
		var hash string
		var ref BlobRef
		if err := rows.Scan(&hash, &ref.SnapshotID, &ref.Path); err != nil {
			return nil, err
		}
		m[hash] = append(m[hash], ref)
	}
	return m, rows.Err()
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

	// Classify every path that appears in either snapshot
	seen := make(map[string]bool, len(oldMap)+len(newMap))
	for path := range oldMap {
		seen[path] = true
	}
	for path := range newMap {
		seen[path] = true
	}

	for path := range seen {
		of, inOld := oldMap[path]
		nf, inNew := newMap[path]
		contentDiffers := inOld && inNew && (of.BlobHash != nf.BlobHash || of.Type != nf.Type || of.Mode != nf.Mode)

		switch classifyFileChange(inOld, inNew, contentDiffers) {
		case "added":
			changes = append(changes, FileChange{Path: path, Type: ChangeAdded, NewHash: nf.BlobHash})
		case "deleted":
			changes = append(changes, FileChange{Path: path, Type: ChangeDeleted, OldHash: of.BlobHash})
		case "modified":
			changes = append(changes, FileChange{Path: path, Type: ChangeModified, OldHash: of.BlobHash, NewHash: nf.BlobHash})
		// "unchanged" — no entry needed
		}
	}

	sort.Slice(changes, func(i, j int) bool { return changes[i].Path < changes[j].Path })
	return changes, nil
}
