// Generative E2E test for earwig.
//
// Maintains an in-memory model of expected filesystem state,
// generates random operations, and asserts the model matches
// reality after every snapshot and restore.
//
// Usage: gen [iterations] [seed]
//   iterations: number of operations (default: 200)
//   seed:       random seed (default: current unix timestamp)

package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ── model ──────────────────────────────────────────────

type FileEntry struct {
	Content string      // file content (ignored for symlinks)
	Mode    os.FileMode // permission bits (0644, 0755, 0444)
	IsLink  bool        // true if symlink
	Target  string      // symlink target (only when IsLink)
}

type Model struct {
	files map[string]FileEntry
}

func newModel() *Model {
	return &Model{files: make(map[string]FileEntry)}
}

func (m *Model) clone() *Model {
	c := newModel()
	for k, v := range m.files {
		c.files[k] = v
	}
	return c
}

type SavedSnapshot struct {
	hash  string
	model *Model
}

// ── test harness ───────────────────────────────────────

type Harness struct {
	dir       string
	seed      int64
	model     *Model
	snapshots []SavedSnapshot
	headHash  string // hash of the current HEAD snapshot
	rng       *rand.Rand
	pass      int
	fail      int

	// Checkpoints: name -> snapshot index in h.snapshots
	checkpoints map[string]int

	// Operation counters
	opCounts    map[string]int
	bytesWritten int64
}

func (h *Harness) fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf("[seed=%d] %s", h.seed, fmt.Sprintf(format, args...)))
}

func newHarness(seed int64) *Harness {
	dir := fmt.Sprintf("/tmp/earwig-gen-%d", seed)
	if err := os.RemoveAll(dir); err != nil {
		panic(fmt.Sprintf("[seed=%d] RemoveAll %s: %v", seed, dir, err))
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		panic(fmt.Sprintf("[seed=%d] MkdirAll %s: %v", seed, dir, err))
	}

	h := &Harness{
		dir:         dir,
		seed:        seed,
		model:       newModel(),
		rng:         rand.New(rand.NewSource(seed)),
		opCounts:    make(map[string]int),
		checkpoints: make(map[string]int),
	}

	// earwig init
	h.earwig("init")
	return h
}

// ── earwig CLI ─────────────────────────────────────────

func (h *Harness) earwig(args ...string) string {
	cmd := exec.Command("earwig", args...)
	cmd.Dir = h.dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		h.fatalf("earwig %s failed: %s\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

// ── operations ─────────────────────────────────────────

var filenames = []string{
	"readme.txt", "main.go", "config.json", "data.csv", "Makefile",
	"src/app.go", "src/util.go", "src/handler.go", "src/config.go",
	"src/middleware/auth.go", "src/middleware/logging.go",
	"pkg/models/user.go", "pkg/models/post.go",
	"pkg/db/connect.go", "pkg/db/migrate.go",
	"docs/guide.md", "docs/api.md", "docs/changelog.md",
	"test/a_test.go", "test/b_test.go", "test/integration/suite_test.go",
	"web/static/style.css", "web/static/app.js",
	"web/templates/index.html", "web/templates/layout.html",
	"scripts/deploy.sh", "scripts/seed.sql",
	"deep/a/b/c.txt", "deep/a/b/d/e.txt",
}

var dirs = []string{
	"", "src/", "src/middleware/", "pkg/models/", "pkg/db/",
	"docs/", "test/", "test/integration/",
	"web/static/", "web/templates/",
	"scripts/", "deep/a/b/", "deep/a/b/d/",
}

// Random top-level prefixes and sub-paths for generating unique directories.
// With 12 prefixes * 30 suffixes = 360 possible top-level dirs, delete_dir
// (which operates on top-level) can only wipe a small fraction at a time.
var dirPrefixes = []string{
	"mod", "lib", "svc", "app", "cmd", "internal", "vendor",
	"contrib", "tools", "build", "deploy", "assets",
}

var dirSubs = []string{"", "src/", "test/", "data/", "config/"}

func (h *Harness) randomPath() string {
	r := h.rng.Intn(100)
	switch {
	case r < 50:
		// 50%: fixed filenames (realistic repeated edits)
		return filenames[h.rng.Intn(len(filenames))]
	case r < 75:
		// 25%: random filename in a fixed directory
		dir := dirs[h.rng.Intn(len(dirs))]
		name := fmt.Sprintf("f%d.txt", h.rng.Intn(1000))
		return dir + name
	default:
		// 25%: random filename in a generated directory
		// e.g. "mod-17/src/f42.txt" — spreads files across many top-level dirs
		prefix := dirPrefixes[h.rng.Intn(len(dirPrefixes))]
		n := h.rng.Intn(30)
		sub := dirSubs[h.rng.Intn(len(dirSubs))]
		name := fmt.Sprintf("f%d.txt", h.rng.Intn(1000))
		return fmt.Sprintf("%s-%d/%s%s", prefix, n, sub, name)
	}
}

func (h *Harness) randomContent() string {
	// Lognormal-ish distribution matching real source code:
	//   ~50 bytes/line, typical files 100-500 lines (5-25 KB)
	//
	//   20%  512 B -   2 KB   (configs, READMEs, short scripts)
	//   40%    2 KB - 25 KB   (typical source files)
	//   25%   25 KB - 100 KB  (larger modules)
	//   10%  100 KB -   1 MB  (generated code, data files)
	//    5%    1 MB -   5 MB  (vendored/large files)
	var n int
	r := h.rng.Intn(100)
	switch {
	case r < 20:
		n = h.rng.Intn(1536) + 512
	case r < 60:
		n = h.rng.Intn(23*1024) + 2*1024
	case r < 85:
		n = h.rng.Intn(75*1024) + 25*1024
	case r < 95:
		n = h.rng.Intn(900*1024) + 100*1024
	default:
		n = h.rng.Intn(4*1024*1024) + 1*1024*1024
	}

	// Generate content from common English words — produces compressible
	// text with realistic entropy (similar to source code / prose).
	var buf strings.Builder
	buf.Grow(n)
	for buf.Len() < n {
		// ~10% chance of newline, ~5% chance of blank line
		r := h.rng.Intn(100)
		if r < 5 {
			buf.WriteString("\n\n")
		} else if r < 15 {
			buf.WriteByte('\n')
		} else {
			if buf.Len() > 0 && buf.String()[buf.Len()-1] != '\n' {
				buf.WriteByte(' ')
			}
			buf.WriteString(wordlist[h.rng.Intn(len(wordlist))])
		}
	}
	return buf.String()[:n]
}

func (h *Harness) opWriteFile() {
	path := h.randomPath()

	// 10% chance: reuse content from an existing file (exercises dedup)
	var content string
	existing := h.existingPaths()
	if len(existing) > 0 && h.rng.Intn(10) == 0 {
		entry := h.model.files[existing[h.rng.Intn(len(existing))]]
		if !entry.IsLink {
			content = entry.Content
		} else {
			content = h.randomContent()
		}
	} else {
		content = h.randomContent()
	}

	absPath := filepath.Join(h.dir, path)
	if err := os.MkdirAll(filepath.Dir(absPath), 0755); err != nil {
		h.fatalf("MkdirAll %s: %v", filepath.Dir(absPath), err)
	}
	// Remove existing file first — it might be read-only or a symlink.
	// Use Lstat to detect symlinks: chmod follows symlinks and would
	// change the TARGET's mode, corrupting unrelated files.
	if info, err := os.Lstat(absPath); err == nil {
		if info.Mode()&os.ModeSymlink == 0 {
			os.Chmod(absPath, 0644) // ensure writable (only for regular files)
		}
		os.Remove(absPath)
	}

	// Randomly pick a permission mode
	mode := os.FileMode(0644)
	r := h.rng.Intn(100)
	if r < 10 {
		mode = 0755
	} else if r < 15 {
		mode = 0444
	}

	if err := os.WriteFile(absPath, []byte(content), mode); err != nil {
		h.fatalf("WriteFile %s: %v", absPath, err)
	}
	os.Chmod(absPath, mode) // explicit chmod (WriteFile subject to umask)

	h.model.files[path] = FileEntry{Content: content, Mode: mode}
	h.bytesWritten += int64(len(content))
	fmt.Printf("  write %s (%d bytes, mode %04o)\n", path, len(content), mode)
}

func (h *Harness) opChmod() {
	existing := h.existingRegularFiles()
	if len(existing) == 0 {
		return
	}
	path := existing[h.rng.Intn(len(existing))]
	entry := h.model.files[path]

	// Pick a new mode different from current
	modes := []os.FileMode{0644, 0755, 0444}
	newMode := modes[h.rng.Intn(len(modes))]
	for newMode == entry.Mode && len(modes) > 1 {
		newMode = modes[h.rng.Intn(len(modes))]
	}

	absPath := filepath.Join(h.dir, path)
	if err := os.Chmod(absPath, newMode); err != nil {
		h.fatalf("Chmod %s: %v", absPath, err)
	}

	oldMode := entry.Mode
	entry.Mode = newMode
	h.model.files[path] = entry
	fmt.Printf("  chmod %s %04o -> %04o\n", path, oldMode, newMode)
}

func (h *Harness) opCreateSymlink() {
	path := h.randomPath()
	absPath := filepath.Join(h.dir, path)

	// Generate a relative symlink target — point to another file in the model
	// or a random relative path (which may be dangling)
	var target string
	existing := h.existingPaths()
	if len(existing) > 0 && h.rng.Intn(2) == 0 {
		target = existing[h.rng.Intn(len(existing))]
	} else {
		target = fmt.Sprintf("link-target-%d.txt", h.rng.Intn(100))
	}

	if err := os.MkdirAll(filepath.Dir(absPath), 0755); err != nil {
		h.fatalf("MkdirAll %s: %v", filepath.Dir(absPath), err)
	}
	// Remove existing file/symlink first
	if info, err := os.Lstat(absPath); err == nil {
		if info.Mode()&os.ModeSymlink == 0 {
			os.Chmod(absPath, 0644) // ensure writable (only for regular files)
		}
		os.Remove(absPath)
	}
	if err := os.Symlink(target, absPath); err != nil {
		h.fatalf("Symlink %s -> %s: %v", absPath, target, err)
	}

	h.model.files[path] = FileEntry{IsLink: true, Target: target}
	fmt.Printf("  symlink %s -> %s\n", path, target)
}

func (h *Harness) opDeleteFile() {
	existing := h.existingPaths()
	if len(existing) == 0 {
		return
	}
	path := existing[h.rng.Intn(len(existing))]

	absPath := filepath.Join(h.dir, path)
	// Ensure parent is writable and file is removable.
	// Save and restore parent dir mode in case earwig ever tracks directories.
	parentDir := filepath.Dir(absPath)
	parentInfo, parentErr := os.Lstat(parentDir)
	if parentErr == nil {
		os.Chmod(parentDir, 0755)
	}
	if info, err := os.Lstat(absPath); err == nil && info.Mode()&os.ModeSymlink == 0 {
		os.Chmod(absPath, 0644) // ensure writable (only for regular files)
	}
	if err := os.Remove(absPath); err != nil {
		h.fatalf("Remove %s: %v", absPath, err)
	}
	if parentErr == nil {
		os.Chmod(parentDir, parentInfo.Mode().Perm())
	}
	delete(h.model.files, path)

	// Clean up empty parent directories
	for dir := filepath.Dir(absPath); dir != h.dir; dir = filepath.Dir(dir) {
		if err := os.Remove(dir); err != nil {
			break
		}
	}

	fmt.Printf("  delete %s\n", path)
}

func (h *Harness) opDeleteDir() {
	// Pick a directory that has files
	dirs := h.existingDirs()
	if len(dirs) == 0 {
		return
	}
	dir := dirs[h.rng.Intn(len(dirs))]

	if err := os.RemoveAll(filepath.Join(h.dir, dir)); err != nil {
		h.fatalf("RemoveAll %s: %v", dir, err)
	}
	prefix := dir + "/"
	for path := range h.model.files {
		if path == dir || strings.HasPrefix(path, prefix) {
			delete(h.model.files, path)
		}
	}
	fmt.Printf("  rmdir %s/\n", dir)
}

func (h *Harness) opSnapshot() {
	output := h.earwig("snapshot")

	if strings.Contains(output, "No changes") {
		fmt.Printf("  snapshot (no changes)\n")
		h.verify("after no-change snapshot")
		// Diff check: current HEAD should still match filesystem
		if h.headHash != "" {
			h.verifyDiff(h.headHash)
		}
		return
	}

	// Parse hash from "Snapshot abc123def456"
	hash := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(output), "Snapshot "))
	h.snapshots = append(h.snapshots, SavedSnapshot{
		hash:  hash,
		model: h.model.clone(),
	})
	h.headHash = hash
	fmt.Printf("  snapshot #%d: %s\n", len(h.snapshots), hash)

	h.verify(fmt.Sprintf("after snapshot #%d", len(h.snapshots)))

	// Diff check: just-created snapshot should match filesystem exactly
	h.verifyDiff(hash)

	// 10% of snapshots: verify DB file list matches model
	if h.rng.Intn(10) == 0 {
		h.verifyFiles(hash, h.model)
	}
}

func (h *Harness) opRestore() {
	if len(h.snapshots) == 0 {
		return
	}
	idx := h.rng.Intn(len(h.snapshots))
	snap := h.snapshots[idx]

	h.earwig("restore", "-y", snap.hash)
	h.model = snap.model.clone()
	h.headHash = snap.hash
	fmt.Printf("  restore -> #%d (%s)\n", idx+1, snap.hash)

	h.verify(fmt.Sprintf("after restore to #%d", idx+1))

	// Diff check: restored snapshot should match filesystem exactly
	h.verifyDiff(snap.hash)

	// 10% of restores: verify DB file list matches model
	if h.rng.Intn(10) == 0 {
		h.verifyFiles(snap.hash, snap.model)
	}
}

func (h *Harness) opForget() {
	if len(h.snapshots) < 2 {
		return
	}

	// Pick a random snapshot that is NOT the current HEAD
	var candidates []int
	for i, snap := range h.snapshots {
		if snap.hash != h.headHash {
			candidates = append(candidates, i)
		}
	}
	if len(candidates) == 0 {
		return
	}
	idx := candidates[h.rng.Intn(len(candidates))]
	snap := h.snapshots[idx]

	h.earwig("forget", snap.hash)
	fmt.Printf("  forget #%d (%s)\n", idx+1, snap.hash)

	// Remove checkpoints pointing at this snapshot
	for name, cpIdx := range h.checkpoints {
		if cpIdx == idx {
			delete(h.checkpoints, name)
		} else if cpIdx > idx {
			// Adjust indices for removed element
			h.checkpoints[name] = cpIdx - 1
		}
	}

	// Remove from our tracking slice
	h.snapshots = append(h.snapshots[:idx], h.snapshots[idx+1:]...)

	// Filesystem should be unchanged after forget
	h.verify("after forget")

	// Verify a random surviving snapshot is still intact in the DB
	// (catches blob over-deletion where forget/GC removes shared blobs)
	if len(h.snapshots) > 0 {
		check := h.snapshots[h.rng.Intn(len(h.snapshots))]
		h.verifyFiles(check.hash, check.model)
	}
}

func (h *Harness) opGC() {
	h.earwig("gc")
	fmt.Printf("  gc\n")

	// Filesystem should be unchanged after GC
	h.verify("after gc")

	// Verify a random surviving snapshot is still intact in the DB
	// (catches blob over-deletion where GC removes blobs still referenced)
	if len(h.snapshots) > 0 {
		check := h.snapshots[h.rng.Intn(len(h.snapshots))]
		h.verifyFiles(check.hash, check.model)
	}
}

func (h *Harness) opCheckpoint() {
	if len(h.snapshots) == 0 {
		return
	}

	// Pick a random snapshot to checkpoint
	idx := h.rng.Intn(len(h.snapshots))
	name := fmt.Sprintf("check-%d-%d", h.opCounts["checkpoint"], h.rng.Intn(1000))
	snap := h.snapshots[idx]

	h.earwig("check", name, snap.hash)
	fmt.Printf("  checkpoint %s -> #%d (%s)\n", name, idx+1, snap.hash)

	h.checkpoints[name] = idx

	// Verify it shows up in earwig checks
	output := h.earwig("checks")
	if !strings.Contains(output, name) {
		h.fail++
		fmt.Printf("    FAIL: checkpoint %s not in 'earwig checks' output\n", name)
	} else {
		h.pass++
	}

	// Verify it resolves via earwig show
	output = h.earwig("show", name)
	if !strings.Contains(output, snap.hash) {
		h.fail++
		fmt.Printf("    FAIL: checkpoint %s doesn't resolve to %s\n", name, snap.hash)
	} else {
		h.pass++
	}
}

func (h *Harness) opCheckpointDelete() {
	if len(h.checkpoints) == 0 {
		return
	}

	// Pick a random checkpoint to delete
	var names []string
	for n := range h.checkpoints {
		names = append(names, n)
	}
	sort.Strings(names)
	name := names[h.rng.Intn(len(names))]

	h.earwig("check", "-d", name)
	fmt.Printf("  checkpoint delete %s\n", name)
	delete(h.checkpoints, name)

	// Verify it's gone
	output := h.earwig("checks")
	if strings.Contains(output, name) {
		h.fail++
		fmt.Printf("    FAIL: checkpoint %s still in 'earwig checks' after delete\n", name)
	} else {
		h.pass++
	}
}

func (h *Harness) opCheckpointRestore() {
	if len(h.checkpoints) == 0 {
		return
	}

	// Pick a random checkpoint to restore by name
	var names []string
	for n := range h.checkpoints {
		names = append(names, n)
	}
	sort.Strings(names)
	name := names[h.rng.Intn(len(names))]
	idx := h.checkpoints[name]
	snap := h.snapshots[idx]

	h.earwig("restore", "-y", name)
	h.model = snap.model.clone()
	h.headHash = snap.hash
	fmt.Printf("  checkpoint restore %s -> #%d (%s)\n", name, idx+1, snap.hash)

	h.verify(fmt.Sprintf("after checkpoint restore %s", name))

	// Diff check: restored snapshot should match filesystem exactly
	h.verifyDiff(snap.hash)
}

// ── helpers ────────────────────────────────────────────

func (h *Harness) existingPaths() []string {
	paths := make([]string, 0, len(h.model.files))
	for p := range h.model.files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	return paths
}

func (h *Harness) existingRegularFiles() []string {
	var paths []string
	for p, entry := range h.model.files {
		if !entry.IsLink {
			paths = append(paths, p)
		}
	}
	sort.Strings(paths)
	return paths
}

func (h *Harness) existingDirs() []string {
	dirSet := make(map[string]bool)
	for path := range h.model.files {
		parts := strings.Split(filepath.Dir(path), "/")
		if parts[0] != "." {
			dirSet[parts[0]] = true
		}
	}
	dirs := make([]string, 0, len(dirSet))
	for d := range dirSet {
		dirs = append(dirs, d)
	}
	sort.Strings(dirs)
	return dirs
}

// ── verification ───────────────────────────────────────

// diskEntry represents a file found on disk during verification.
type diskEntry struct {
	Content string
	Mode    os.FileMode
	IsLink  bool
	Target  string
}

func (h *Harness) verify(context string) {
	// Walk actual filesystem
	actual := make(map[string]diskEntry)
	filepath.WalkDir(h.dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		rel, _ := filepath.Rel(h.dir, path)
		rel = filepath.ToSlash(rel)
		if rel == "." {
			return nil
		}
		// Skip .earwig
		if strings.HasPrefix(rel, ".earwig") {
			return filepath.SkipDir
		}
		if d.Type()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				return nil
			}
			actual[rel] = diskEntry{IsLink: true, Target: target}
			return nil
		}
		if d.Type().IsRegular() {
			data, err := os.ReadFile(path)
			if err != nil {
				return nil
			}
			info, err := os.Lstat(path)
			if err != nil {
				return nil
			}
			actual[rel] = diskEntry{Content: string(data), Mode: info.Mode().Perm()}
			return nil
		}
		return nil
	})

	// Compare model to actual
	ok := true

	for path, expected := range h.model.files {
		entry, exists := actual[path]
		if !exists {
			h.fail++
			ok = false
			fmt.Printf("    FAIL [%s]: model has %s but missing on disk\n", context, path)
			continue
		}

		if expected.IsLink {
			if !entry.IsLink {
				h.fail++
				ok = false
				fmt.Printf("    FAIL [%s]: %s should be symlink but is regular file\n", context, path)
			} else if entry.Target != expected.Target {
				h.fail++
				ok = false
				fmt.Printf("    FAIL [%s]: %s symlink target: got %q, want %q\n", context, path, entry.Target, expected.Target)
			} else {
				h.pass++
			}
		} else {
			if entry.IsLink {
				h.fail++
				ok = false
				fmt.Printf("    FAIL [%s]: %s should be regular file but is symlink\n", context, path)
			} else if entry.Content != expected.Content {
				h.fail++
				ok = false
				fmt.Printf("    FAIL [%s]: %s content mismatch (model %d bytes, disk %d bytes)\n",
					context, path, len(expected.Content), len(entry.Content))
			} else if entry.Mode != expected.Mode {
				h.fail++
				ok = false
				fmt.Printf("    FAIL [%s]: %s mode mismatch (model %04o, disk %04o)\n",
					context, path, expected.Mode, entry.Mode)
			} else {
				h.pass++
			}
		}
	}

	for path := range actual {
		if _, inModel := h.model.files[path]; !inModel {
			h.fail++
			ok = false
			fmt.Printf("    FAIL [%s]: %s exists on disk but not in model\n", context, path)
		}
	}

	if ok {
		fmt.Printf("    OK   [%s]: %d files match\n", context, len(h.model.files))
	}
}

// dbFileEntry holds parsed output from "earwig _files".
type dbFileEntry struct {
	Size int64
	Type string
}

// verifyFiles runs "earwig _files <hash>" and checks that the DB's file list
// matches the model exactly: same paths, sizes, and types.
func (h *Harness) verifyFiles(hash string, model *Model) {
	output := h.earwig("_files", hash)
	lines := strings.Split(strings.TrimSpace(output), "\n")

	// Parse _files output: path\tsize\thash\ttype
	dbFiles := make(map[string]dbFileEntry)
	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) < 4 {
			h.fail++
			fmt.Printf("    FAIL [_files %s]: unparseable line: %s\n", hash, line)
			continue
		}
		size, _ := strconv.ParseInt(parts[1], 10, 64)
		dbFiles[parts[0]] = dbFileEntry{Size: size, Type: parts[3]}
	}

	ok := true

	for path, entry := range model.files {
		dbEntry, exists := dbFiles[path]
		if !exists {
			h.fail++
			ok = false
			fmt.Printf("    FAIL [_files %s]: model has %s but not in DB\n", hash, path)
			continue
		}

		expectedType := "file"
		var expectedSize int64
		if entry.IsLink {
			expectedType = "symlink"
			expectedSize = int64(len(entry.Target))
		} else {
			expectedSize = int64(len(entry.Content))
		}

		if dbEntry.Type != expectedType {
			h.fail++
			ok = false
			fmt.Printf("    FAIL [_files %s]: %s type mismatch (model %s, DB %s)\n",
				hash, path, expectedType, dbEntry.Type)
		} else if dbEntry.Size != expectedSize {
			h.fail++
			ok = false
			fmt.Printf("    FAIL [_files %s]: %s size mismatch (model %d, DB %d)\n",
				hash, path, expectedSize, dbEntry.Size)
		} else {
			h.pass++
		}
	}

	for path := range dbFiles {
		if _, inModel := model.files[path]; !inModel {
			h.fail++
			ok = false
			fmt.Printf("    FAIL [_files %s]: %s in DB but not in model\n", hash, path)
		}
	}

	if ok {
		fmt.Printf("    OK   [_files %s]: %d files match DB\n", hash, len(model.files))
	}
}

// verifyDiff runs "earwig diff <hash>" and asserts the output says no differences.
// This catches bugs where Preview() computes a different result than Restore.
func (h *Harness) verifyDiff(hash string) {
	output := h.earwig("diff", hash)
	if !strings.Contains(output, "No differences") {
		h.fail++
		fmt.Printf("    FAIL [diff %s]: expected 'No differences', got:\n%s\n", hash, output)
	} else {
		h.pass++
	}
}

// ── main ───────────────────────────────────────────────

func main() {
	seed := time.Now().UnixNano()
	iterations := 200

	if len(os.Args) > 1 {
		if n, err := strconv.Atoi(os.Args[1]); err == nil {
			iterations = n
		}
	}
	if len(os.Args) > 2 {
		if s, err := strconv.ParseInt(os.Args[2], 10, 64); err == nil {
			seed = s
		}
	}

	fmt.Printf("=== Generative test: seed=%d iterations=%d ===\n\n", seed, iterations)

	h := newHarness(seed)

	// Weighted operation selection:
	//   write(43%) snapshot(23%) restore(12%) chmod(5%) delete_file(5%)
	//   symlink(3%) forget(3%) delete_dir(2%) gc(2%)
	type weightedOp struct {
		weight int
		name   string
		fn     func()
	}
	ops := []weightedOp{
		{43, "write", h.opWriteFile},
		{5, "chmod", h.opChmod},
		{3, "symlink", h.opCreateSymlink},
		{5, "delete_file", h.opDeleteFile},
		{2, "delete_dir", h.opDeleteDir},
		{23, "snapshot", h.opSnapshot},
		{12, "restore", h.opRestore},
		{3, "forget", h.opForget},
		{2, "gc", h.opGC},
		{2, "checkpoint", h.opCheckpoint},
		{1, "checkpoint_delete", h.opCheckpointDelete},
		{2, "checkpoint_restore", h.opCheckpointRestore},
	}
	totalWeight := 0
	for _, op := range ops {
		totalWeight += op.weight
	}

	for i := 0; i < iterations; i++ {
		r := h.rng.Intn(totalWeight)
		cumulative := 0
		for _, op := range ops {
			cumulative += op.weight
			if r < cumulative {
				fmt.Printf("[%d/%d] %s\n", i+1, iterations, op.name)
				h.opCounts[op.name]++
				op.fn()
				break
			}
		}
	}

	// Final snapshot + verify
	fmt.Printf("\n[final] snapshot\n")
	h.opSnapshot()

	fmt.Printf("\n================================\n")
	if h.fail == 0 {
		fmt.Printf("ALL PASSED: %d file assertions across %d iterations (seed=%d)\n", h.pass, iterations, seed)
	} else {
		fmt.Printf("FAILED: %d failures, %d passed (seed=%d)\n", h.fail, h.pass, seed)
	}

	// Operation counts
	fmt.Printf("\nOperations:\n")
	for _, name := range []string{"write", "chmod", "symlink", "delete_file", "delete_dir", "snapshot", "restore", "forget", "gc", "checkpoint", "checkpoint_delete", "checkpoint_restore"} {
		fmt.Printf("  %-12s %d\n", name, h.opCounts[name])
	}
	fmt.Printf("  %-12s %d\n", "snapshots_ok", len(h.snapshots))
	fmt.Printf("  bytes_written: %.2f MB\n", float64(h.bytesWritten)/(1024*1024))

	// Database stats
	dbPath := filepath.Join(h.dir, ".earwig", "earwig.db")
	if info, err := os.Stat(dbPath); err == nil {
		fmt.Printf("\nDatabase:\n")
		fmt.Printf("  file size:    %.2f MB\n", float64(info.Size())/(1024*1024))
	}

	// Query blob compression stats via sqlite3
	blobStats := exec.Command("sqlite3", dbPath,
		"SELECT encoding, COUNT(*), SUM(size), SUM(LENGTH(data)) FROM blobs GROUP BY encoding ORDER BY encoding;")
	if out, err := blobStats.Output(); err == nil {
		var totalOriginal, totalStored int64
		fmt.Printf("  blobs:\n")
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if line == "" {
				continue
			}
			parts := strings.Split(line, "|")
			if len(parts) == 4 {
				encoding := parts[0]
				count, _ := strconv.ParseInt(parts[1], 10, 64)
				original, _ := strconv.ParseInt(parts[2], 10, 64)
				stored, _ := strconv.ParseInt(parts[3], 10, 64)
				totalOriginal += original
				totalStored += stored
				fmt.Printf("    %s: %d blobs, %.2f MB original, %.2f MB stored",
					encoding, count, float64(original)/(1024*1024), float64(stored)/(1024*1024))
				if encoding != "raw" && original > 0 {
					fmt.Printf(" (%.1fx)", float64(original)/float64(stored))
				}
				fmt.Println()
			}
		}
		if totalOriginal > 0 {
			fmt.Printf("    total: %.2f MB original, %.2f MB stored (%.1fx overall)\n",
				float64(totalOriginal)/(1024*1024), float64(totalStored)/(1024*1024),
				float64(totalOriginal)/float64(totalStored))
		}
	}

	// Working directory size
	var dirSize int64
	filepath.WalkDir(h.dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		rel, _ := filepath.Rel(h.dir, path)
		if strings.HasPrefix(filepath.ToSlash(rel), ".earwig") {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Type().IsRegular() {
			if info, err := d.Info(); err == nil {
				dirSize += info.Size()
			}
		}
		return nil
	})
	fmt.Printf("\nWorking dir: %.2f MB (%d files)\n", float64(dirSize)/(1024*1024), len(h.model.files))
	fmt.Printf("================================\n")

	if h.fail > 0 {
		os.Exit(1)
	}
}

// 500 most common English words + code-like tokens for realistic compressible content.
var wordlist = []string{
	"the", "be", "to", "of", "and", "a", "in", "that", "have", "I",
	"it", "for", "not", "on", "with", "he", "as", "you", "do", "at",
	"this", "but", "his", "by", "from", "they", "we", "say", "her", "she",
	"or", "an", "will", "my", "one", "all", "would", "there", "their", "what",
	"so", "up", "out", "if", "about", "who", "get", "which", "go", "me",
	"when", "make", "can", "like", "time", "no", "just", "him", "know", "take",
	"people", "into", "year", "your", "good", "some", "could", "them", "see", "other",
	"than", "then", "now", "look", "only", "come", "its", "over", "think", "also",
	"back", "after", "use", "two", "how", "our", "work", "first", "well", "way",
	"even", "new", "want", "because", "any", "these", "give", "day", "most", "us",
	"great", "between", "need", "large", "often", "hand", "high", "place", "hold", "free",
	"real", "life", "few", "north", "open", "seem", "together", "next", "white", "children",
	"begin", "got", "walk", "example", "ease", "paper", "group", "always", "music", "those",
	"both", "mark", "book", "letter", "until", "mile", "river", "car", "feet", "care",
	"second", "enough", "plain", "girl", "usual", "young", "ready", "above", "ever", "red",
	"list", "though", "feel", "talk", "bird", "soon", "body", "dog", "family", "direct",
	"pose", "leave", "song", "measure", "door", "product", "black", "short", "numeral", "class",
	"wind", "question", "happen", "complete", "ship", "area", "half", "rock", "order", "fire",
	"south", "problem", "piece", "told", "knew", "pass", "since", "top", "whole", "king",
	"space", "heard", "best", "hour", "better", "true", "during", "hundred", "five", "remember",
	"step", "early", "third", "quite", "carry", "state", "once", "field", "present", "pull",
	"nothing", "kind", "among", "start", "long", "find", "each", "still", "learn", "should",
	"answer", "here", "while", "last", "right", "old", "year", "big", "must", "part",
	"call", "world", "may", "where", "help", "through", "much", "line", "before", "turn",
	"move", "live", "found", "every", "name", "same", "tell", "set", "three", "end",
	// Code-like tokens
	"func", "return", "if", "else", "for", "range", "var", "const", "type", "struct",
	"import", "package", "nil", "err", "error", "string", "int", "bool", "true", "false",
	"map", "slice", "append", "len", "make", "new", "defer", "go", "chan", "select",
	"switch", "case", "break", "continue", "default", "interface", "method", "pointer", "byte", "rune",
	"fmt.Println", "fmt.Sprintf", "os.Open", "os.Create", "io.Reader", "io.Writer", "http.Get", "json.Marshal",
	"context.Background", "filepath.Join", "strings.Split", "strconv.Atoi", "sync.Mutex", "time.Now",
	"{", "}", "(", ")", "[", "]", ":=", "==", "!=", "<=", ">=", "&&", "||",
	"//", "/*", "*/", "/**", "@param", "@return", "@throws", "TODO:", "FIXME:", "NOTE:",
	"public", "private", "static", "void", "class", "extends", "implements", "abstract", "final",
	"async", "await", "promise", "callback", "function", "=>", "let", "const", "var",
	"SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "JOIN", "GROUP", "ORDER", "BY",
	"CREATE", "TABLE", "INDEX", "DROP", "ALTER", "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "NULL",
	"<div>", "</div>", "<span>", "</span>", "<p>", "</p>", "<a", "href=", "class=", "id=",
	"0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
	"0x00", "0xFF", "127.0.0.1", "localhost", "8080", "443", "/api/v1", "/health", "GET", "POST",
	"config", "server", "client", "request", "response", "handler", "middleware", "router", "logger", "database",
	"user", "admin", "token", "session", "cache", "queue", "worker", "service", "repository", "controller",
}
