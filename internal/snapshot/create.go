package snapshot

import (
	"os"
	"path/filepath"
	"strings"

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

		// Defense in depth: reject paths that escape root
		if strings.Contains(relPath, "..") {
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

// TakeIncrementalSnapshot creates a snapshot by only re-reading files that changed,
// copying unchanged entries from the parent snapshot.
// Returns nil, nil if nothing actually changed.
func (c *Creator) TakeIncrementalSnapshot(parentID int64, changedPaths map[string]bool, message string) (*store.Snapshot, error) {
	parentFiles, err := c.store.GetSnapshotFiles(parentID)
	if err != nil {
		return nil, err
	}

	// Start with parent's file map
	fileMap := make(map[string]store.SnapshotFile, len(parentFiles))
	for _, f := range parentFiles {
		fileMap[f.Path] = f
	}

	for path := range changedPaths {
		absPath, err := safePath(c.rootDir, path)
		if err != nil {
			continue // skip paths that escape root
		}
		info, statErr := os.Stat(absPath)

		if statErr != nil {
			// Path doesn't exist — deleted file or directory
			delete(fileMap, path)
			// Also remove children in case it was a directory
			prefix := path + "/"
			for p := range fileMap {
				if strings.HasPrefix(p, prefix) {
					delete(fileMap, p)
				}
			}
			continue
		}

		if info.IsDir() {
			// New directory — walk it to discover new files
			filepath.WalkDir(absPath, func(p string, d os.DirEntry, err error) error {
				if err != nil {
					return nil
				}
				relPath, _ := filepath.Rel(c.rootDir, p)
				relPath = filepath.ToSlash(relPath)
				if c.ignore.Match(relPath) {
					if d.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}
				if !d.Type().IsRegular() {
					return nil
				}
				sf, err := c.readFile(p, relPath)
				if err != nil {
					return nil
				}
				fileMap[relPath] = sf
				return nil
			})
			continue
		}

		if !info.Mode().IsRegular() {
			continue
		}

		sf, err := c.readFile(absPath, path)
		if err != nil {
			continue
		}
		fileMap[path] = sf
	}

	// Convert map to slice
	files := make([]store.SnapshotFile, 0, len(fileMap))
	for _, f := range fileMap {
		files = append(files, f)
	}

	if filesEqual(files, parentFiles) {
		return nil, nil
	}

	return c.store.CreateSnapshot(&parentID, files, message)
}

// readFile reads a file, stores its blob, and returns a SnapshotFile.
func (c *Creator) readFile(absPath, relPath string) (store.SnapshotFile, error) {
	data, err := os.ReadFile(absPath)
	if err != nil {
		return store.SnapshotFile{}, err
	}

	blobHash, err := c.store.PutBlob(data)
	if err != nil {
		return store.SnapshotFile{}, err
	}

	info, err := os.Stat(absPath)
	if err != nil {
		return store.SnapshotFile{}, err
	}

	return store.SnapshotFile{
		Path:     relPath,
		BlobHash: blobHash,
		Mode:     uint32(info.Mode().Perm()),
		ModTime:  info.ModTime(),
		Size:     info.Size(),
	}, nil
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
