package store

import "time"

type Blob struct {
	Hash string
	Size int64
	Data []byte
}

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
