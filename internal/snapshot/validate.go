package snapshot

import (
	"path/filepath"
	"strings"
)

// hasChanges returns true if any of the four change categories is non-empty.
// This is the core predicate for deciding whether a restore would modify the
// filesystem at all.
//
// @ ensures result == (nDelete > 0 || nWrite > 0 || nModify > 0 || nChmod > 0)
func hasChanges(nDelete, nWrite, nModify, nChmod int) (result bool) {
	return nDelete > 0 || nWrite > 0 || nModify > 0 || nChmod > 0
}

// isUnsafeSymlinkTarget reports whether a symlink target is unsafe to restore:
// an absolute path, or one that escapes the repository root once resolved
// relative to the link's own directory (linkDir, the link path's directory).
// A target that merely contains ".." but stays within the root — e.g. a link at
// pkg/models pointing to ../../web/static/style.css — is safe and is NOT flagged;
// the previous "contains any .." check cried wolf on those ordinary relative
// links. Extracting this from restore.go allows formal verification of exactly
// what "unsafe" means: result is false only when the resolved target is relative
// and does not climb out of the root.
//
// @ ensures result == (filepath.IsAbs(target) || filepath.Clean(linkDir + "/" + target) == ".." || strings.HasPrefix(filepath.Clean(linkDir + "/" + target), ".." + string(filepath.Separator)))
// @ decreases
func isUnsafeSymlinkTarget(linkDir, target string) (result bool) {
	if filepath.IsAbs(target) {
		return true
	}
	resolved := filepath.Clean(linkDir + "/" + target)
	return resolved == ".." || strings.HasPrefix(resolved, ".."+string(filepath.Separator))
}

// readFileType returns the file type string for a regular file.
// Always returns "file" — this is the type tag stored in snapshot_files.
//
// @ ensures result == "file"
func readFileType() (result string) {
	return "file"
}

// readSymlinkType returns the file type string for a symlink.
// Always returns "symlink" — this is the type tag stored in snapshot_files.
//
// @ ensures result == "symlink"
func readSymlinkType() (result string) {
	return "symlink"
}
