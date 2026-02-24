package snapshot

import (
	"os"
	"path/filepath"

	"github.com/nk/earwig/internal/ignore"
	"github.com/nk/earwig/internal/store"
)

type Creator struct {
	store   *store.Store
	rootDir string
	ignore  *ignore.Matcher
}

func NewCreator(s *store.Store, rootDir string, ig *ignore.Matcher) *Creator {
	return &Creator{store: s, rootDir: rootDir, ignore: ig}
}

// TakeSnapshot walks the directory, hashes and stores all non-ignored files,
// and creates a snapshot. Returns nil, nil if nothing changed vs parent.
func (c *Creator) TakeSnapshot(parentID *int64, message string) (*store.Snapshot, error) {
	var files []store.SnapshotFile

	err := filepath.WalkDir(c.rootDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // Skip inaccessible paths
		}

		relPath, _ := filepath.Rel(c.rootDir, path)
		relPath = filepath.ToSlash(relPath)

		if relPath == "." {
			return nil
		}

		if c.ignore.Match(relPath) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		if !d.Type().IsRegular() {
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return nil // Skip unreadable files
		}

		blobHash, err := c.store.PutBlob(data)
		if err != nil {
			return err
		}

		info, err := d.Info()
		if err != nil {
			return nil
		}

		files = append(files, store.SnapshotFile{
			Path:     relPath,
			BlobHash: blobHash,
			Mode:     uint32(info.Mode().Perm()),
			ModTime:  info.ModTime(),
			Size:     info.Size(),
		})

		return nil
	})
	if err != nil {
		return nil, err
	}

	// Check if anything changed vs parent
	if parentID != nil {
		parentFiles, err := c.store.GetSnapshotFiles(*parentID)
		if err != nil {
			return nil, err
		}
		if filesEqual(files, parentFiles) {
			return nil, nil
		}
	}

	return c.store.CreateSnapshot(parentID, files, message)
}

func filesEqual(a []store.SnapshotFile, b []store.SnapshotFile) bool {
	if len(a) != len(b) {
		return false
	}
	aMap := make(map[string]string, len(a))
	for _, f := range a {
		aMap[f.Path] = f.BlobHash
	}
	for _, f := range b {
		if aMap[f.Path] != f.BlobHash {
			return false
		}
	}
	return true
}
