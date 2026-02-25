package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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
	if err := filepath.WalkDir(r.rootDir, func(path string, d os.DirEntry, err error) error {
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
		if d.Type().IsRegular() || d.Type()&os.ModeSymlink != 0 {
			existingPaths = append(existingPaths, relPath)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("walking filesystem: %w", err)
	}

	// Delete files not in target snapshot
	for _, path := range existingPaths {
		if _, inTarget := targetMap[path]; !inTarget {
			absPath, err := safePath(r.rootDir, path)
			if err != nil {
				continue // skip paths that escape root
			}
			if err := os.Remove(absPath); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("removing %s: %w", path, err)
			}
		}
	}

	// Build set of existing files for quick lookup
	existingSet := make(map[string]bool, len(existingPaths))
	for _, p := range existingPaths {
		existingSet[p] = true
	}

	// Write files from target snapshot, skipping files that already match
	for _, f := range targetFiles {
		// Never restore into ignored directories (e.g. .earwig/)
		if r.ignore.Match(f.Path) {
			continue
		}

		absPath, err := safePath(r.rootDir, f.Path)
		if err != nil {
			return fmt.Errorf("unsafe path in snapshot: %w", err)
		}

		// Remove anything at this path that conflicts with the target type.
		// Symlinks are always removed to prevent following them.
		// Regular files are removed when target is a symlink (os.Symlink
		// won't overwrite) or when they are read-only (os.WriteFile can't
		// open for writing).
		if info, err := os.Lstat(absPath); err == nil {
			isLink := info.Mode()&os.ModeSymlink != 0
			needsRemove := isLink ||
				f.Type == "symlink" ||
				(!info.Mode().IsDir() && info.Mode().Perm()&0200 == 0)
			if needsRemove {
				if err := os.Remove(absPath); err != nil {
					return fmt.Errorf("removing %s: %w", f.Path, err)
				}
			}
		}

		// If file exists on disk, check if it already matches the target
		if existingSet[f.Path] && f.Type != "symlink" {
			if h, err := hashFile(absPath); err == nil && h == f.BlobHash {
				// Content matches — but still fix permissions if they differ
				if info, err := os.Lstat(absPath); err == nil {
					if info.Mode().Perm() != os.FileMode(f.Mode).Perm() {
						if err := os.Chmod(absPath, os.FileMode(f.Mode).Perm()); err != nil {
							return fmt.Errorf("fixing permissions on %s: %w", f.Path, err)
						}
					}
				}
				continue
			}
		}

		// Check parent directories for symlinks — a symlink where
		// a directory should be would cause MkdirAll to follow it.
		if err := removeSymlinksInPath(r.rootDir, filepath.Dir(absPath)); err != nil {
			return err
		}

		if err := os.MkdirAll(filepath.Dir(absPath), 0755); err != nil {
			return fmt.Errorf("creating directory for %s: %w", f.Path, err)
		}

		data, err := r.store.GetBlob(f.BlobHash)
		if err != nil {
			return err
		}

		// Mask mode to permission bits only (strip setuid/setgid/sticky
		// that a crafted DB could set).
		mode := os.FileMode(f.Mode).Perm()

		switch f.Type {
		case "symlink":
			// Blob content is the symlink target path
			if err := os.Symlink(string(data), absPath); err != nil {
				return err
			}
		case "file", "":
			if err := os.WriteFile(absPath, data, mode); err != nil {
				return err
			}
			// WriteFile is subject to umask; explicitly set the stored mode.
			if err := os.Chmod(absPath, mode); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown file type %q for %s", f.Type, f.Path)
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

// removeSymlinksInPath walks from rootDir down to targetDir and removes any
// symlinks found along the way. This prevents MkdirAll from following a symlink
// into a location outside the root.
func removeSymlinksInPath(rootDir, targetDir string) error {
	rel, err := filepath.Rel(rootDir, targetDir)
	if err != nil {
		return nil
	}
	current := rootDir
	for _, part := range splitPath(rel) {
		current = filepath.Join(current, part)
		if info, err := os.Lstat(current); err == nil && info.Mode()&os.ModeSymlink != 0 {
			if err := os.Remove(current); err != nil {
				return fmt.Errorf("removing symlink at %s: %w", current, err)
			}
		}
	}
	return nil
}

// splitPath splits a filepath into its components.
func splitPath(path string) []string {
	var parts []string
	for path != "" && path != "." {
		dir, file := filepath.Split(path)
		if file != "" {
			parts = append([]string{file}, parts...)
		}
		path = filepath.Clean(dir)
	}
	return parts
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
