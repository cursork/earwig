package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
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

	// Build set of existing files for quick lookup
	existingSet := make(map[string]bool, len(existingPaths))
	for _, p := range existingPaths {
		existingSet[p] = true
	}

	// Write files from target snapshot, skipping files that already match
	for _, f := range targetFiles {
		absPath := filepath.Join(r.rootDir, filepath.FromSlash(f.Path))

		// If file exists on disk, check if it already matches the target
		if existingSet[f.Path] {
			if h, err := hashFile(absPath); err == nil && h == f.BlobHash {
				continue // Already matches, skip
			}
		}

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

// hashFile computes the SHA-256 of a file by streaming, without loading it all into memory.
func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
