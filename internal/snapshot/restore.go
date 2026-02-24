package snapshot

import (
	"os"
	"path/filepath"
	"sort"

	"github.com/nk/earwig/internal/ignore"
	"github.com/nk/earwig/internal/store"
)

type Restorer struct {
	store   *store.Store
	rootDir string
	ignore  *ignore.Matcher
}

func NewRestorer(s *store.Store, rootDir string, ig *ignore.Matcher) *Restorer {
	return &Restorer{store: s, rootDir: rootDir, ignore: ig}
}

func (r *Restorer) Restore(snapshotID int64) error {
	targetFiles, err := r.store.GetSnapshotFiles(snapshotID)
	if err != nil {
		return err
	}

	targetMap := make(map[string]store.SnapshotFile, len(targetFiles))
	for _, f := range targetFiles {
		targetMap[f.Path] = f
	}

	// Walk current filesystem to find existing tracked files
	var existingPaths []string
	filepath.WalkDir(r.rootDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		relPath, _ := filepath.Rel(r.rootDir, path)
		relPath = filepath.ToSlash(relPath)
		if relPath == "." {
			return nil
		}
		if r.ignore.Match(relPath) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Type().IsRegular() {
			existingPaths = append(existingPaths, relPath)
		}
		return nil
	})

	// Delete files not in target snapshot
	for _, path := range existingPaths {
		if _, inTarget := targetMap[path]; !inTarget {
			absPath := filepath.Join(r.rootDir, filepath.FromSlash(path))
			os.Remove(absPath)
		}
	}

	// Write files from target snapshot
	for _, f := range targetFiles {
		absPath := filepath.Join(r.rootDir, filepath.FromSlash(f.Path))

		os.MkdirAll(filepath.Dir(absPath), 0755)

		data, err := r.store.GetBlob(f.BlobHash)
		if err != nil {
			return err
		}

		if err := os.WriteFile(absPath, data, os.FileMode(f.Mode)); err != nil {
			return err
		}
	}

	// Clean up empty directories (bottom-up)
	var dirs []string
	filepath.WalkDir(r.rootDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		relPath, _ := filepath.Rel(r.rootDir, path)
		relPath = filepath.ToSlash(relPath)
		if relPath == "." {
			return nil
		}
		if r.ignore.Match(relPath) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			dirs = append(dirs, path)
		}
		return nil
	})

	// Sort deepest first
	sort.Slice(dirs, func(i, j int) bool { return len(dirs[i]) > len(dirs[j]) })
	for _, dir := range dirs {
		os.Remove(dir) // Only succeeds if empty
	}

	return nil
}
