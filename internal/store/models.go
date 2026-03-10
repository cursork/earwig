package store

import "time"

type Snapshot struct {
	ID        int64
	Hash      string
	ParentID  *int64
	CreatedAt time.Time
	Message   string
}

type SnapshotFile struct {
	SnapshotID int64
	Path       string
	BlobHash   string
	Mode       uint32
	ModTime    time.Time
	Size       int64
	Type       string // "file" or "symlink"
}

type ChangeType int

const (
	ChangeAdded ChangeType = iota
	ChangeModified
	ChangeDeleted
)

type FileChange struct {
	Path    string
	Type    ChangeType
	OldHash string
	NewHash string
}

type Checkpoint struct {
	Name       string
	SnapshotID int64
	Snapshot   Snapshot
}
