package store

import (
	"fmt"
	"strings"
)

// validateEncoding checks that a blob encoding from the database is one of the
// two supported values. Rejects unknown encodings that could bypass validation
// in GetBlob (a crafted DB with encoding="garbage" would skip both the "raw"
// and "zstd" branches).
//
// @ ensures err == nil ==> (encoding == "raw" || encoding == "zstd")
// @ ensures err != nil ==> err.ErrorMem()
func validateEncoding(encoding string) (err error) {
	if encoding != "raw" && encoding != "zstd" {
		return fmt.Errorf("unknown blob encoding %q", encoding)
	}
	return nil
}

// chooseEncoding decides whether to use zstd compression for a blob based on
// data size and compression ratio.
//
// Properties:
//   - encoding is always a valid value ("raw" or "zstd")
//   - useCompressed is true iff encoding is "zstd"
//   - zstd is only chosen when data is large enough AND compression helped
//   - raw is chosen for small data OR when compression didn't reduce size
//
// @ requires dataLen >= 0
// @ requires compressedLen >= 0
// @ ensures  encoding == "zstd" || encoding == "raw"
// @ ensures  useCompressed == (encoding == "zstd")
// @ ensures  encoding == "zstd" ==> compressedLen < dataLen
// @ ensures  encoding == "zstd" ==> dataLen >= 128 * 1024
func chooseEncoding(dataLen, compressedLen int) (encoding string, useCompressed bool) {
	if dataLen >= 128*1024 && compressedLen < dataLen {
		return "zstd", true
	}
	return "raw", false
}

// classifyFileChange determines the type of change for a single path based on
// its presence in the old and new snapshots. Returns one of four categories.
//
// This is the core classification logic for DiffSnapshots. The postconditions
// guarantee exhaustiveness (exactly one category per input) and correctness
// (each category matches the expected condition).
//
// @ requires inOld || inNew
// @ ensures  result == "added" || result == "modified" || result == "deleted" || result == "unchanged"
// @ ensures  !inOld && inNew ==> result == "added"
// @ ensures  inOld && !inNew ==> result == "deleted"
// @ ensures  inOld && inNew && contentDiffers ==> result == "modified"
// @ ensures  inOld && inNew && !contentDiffers ==> result == "unchanged"
func classifyFileChange(inOld, inNew, contentDiffers bool) (result string) {
	if !inOld && inNew {
		return "added"
	}
	if inOld && !inNew {
		return "deleted"
	}
	if contentDiffers {
		return "modified"
	}
	return "unchanged"
}

// checkBlobSize validates that a blob's stored size is within the allowed
// maximum and that the actual data length matches the stored size. This is
// the core decompression bomb check: after this returns nil, it is guaranteed
// that dataLen <= maxSize.
//
// Called by GetBlob for both raw blobs (dataLen = len(data)) and zstd blobs
// (dataLen = len(decoded)), ensuring no oversized data is ever returned.
//
// @ requires maxSize >= 0
// @ ensures  err == nil ==> int64(dataLen) <= maxSize
// @ ensures  err == nil ==> int64(dataLen) == size
// @ ensures  err != nil ==> err.ErrorMem()
func checkBlobSize(dataLen int, size int64, maxSize int64) (err error) {
	if size > maxSize {
		return fmt.Errorf("size %d exceeds maximum %d", size, maxSize)
	}
	if int64(dataLen) != size {
		return fmt.Errorf("data length %d != stored size %d", dataLen, size)
	}
	return nil
}

// validatePathConflicts checks that no path in a sorted slice is a directory
// prefix of any other path. This detects file/directory conflicts where one
// path can't be both a file and a directory (e.g. "foo" and "foo/bar.txt").
//
// Uses O(n²) all-pairs check which correctly handles cases where intervening
// paths sort between a parent and its child (e.g. "foo", "foo-bar/baz.txt",
// "foo/bar.txt" — the adjacent-pair check misses the conflict between "foo"
// and "foo/bar.txt" because "foo-bar/baz.txt" intervenes).
//
// @ requires forall i int :: {&paths[i]} 0 <= i && i < len(paths) ==> acc(&paths[i], 1/2)
// @ ensures  err == nil ==> forall i int :: {&paths[i]} 0 <= i && i < len(paths) ==> acc(&paths[i], 1/2)
// @ ensures  err == nil ==> forall i, j int :: {&paths[i], &paths[j]} 0 <= i && i < len(paths) && i < j && j < len(paths) ==> !strings.HasPrefix(paths[j], paths[i] + "/")
// @ ensures  err != nil ==> err.ErrorMem()
func validatePathConflicts(paths []string) (err error) {
	// @ invariant 0 <= i && i <= len(paths)
	// @ invariant forall k int :: {&paths[k]} 0 <= k && k < len(paths) ==> acc(&paths[k], 1/2)
	// @ invariant forall a, b int :: {&paths[a], &paths[b]} 0 <= a && a < i && a < b && b < len(paths) ==> !strings.HasPrefix(paths[b], paths[a] + "/")
	for i := 0; i < len(paths); i++ {
		// @ invariant i + 1 <= j && j <= len(paths)
		// @ invariant forall k int :: {&paths[k]} 0 <= k && k < len(paths) ==> acc(&paths[k], 1/2)
		// @ invariant forall b int :: {&paths[b]} i < b && b < j ==> !strings.HasPrefix(paths[b], paths[i] + "/")
		// @ invariant forall a, b int :: {&paths[a], &paths[b]} 0 <= a && a < i && a < b && b < len(paths) ==> !strings.HasPrefix(paths[b], paths[a] + "/")
		for j := i + 1; j < len(paths); j++ {
			if strings.HasPrefix(paths[j], paths[i]+"/") {
				return fmt.Errorf("path conflict: %q and %q", paths[i], paths[j])
			}
		}
	}
	return nil
}

// validateFileTypes checks that every file type is either "file" or "symlink".
// Rejects unknown types that could indicate database corruption or tampering.
//
// @ requires forall i int :: {&types[i]} 0 <= i && i < len(types) ==> acc(&types[i], 1/2)
// @ ensures  err == nil ==> forall i int :: {&types[i]} 0 <= i && i < len(types) ==> acc(&types[i], 1/2)
// @ ensures  err == nil ==> forall i int :: {&types[i]} 0 <= i && i < len(types) ==> (types[i] == "file" || types[i] == "symlink")
// @ ensures  err != nil ==> err.ErrorMem()
func validateFileTypes(types []string) (err error) {
	// @ invariant 0 <= idx && idx <= len(types)
	// @ invariant forall i int :: {&types[i]} 0 <= i && i < len(types) ==> acc(&types[i], 1/2)
	// @ invariant forall k int :: {&types[k]} 0 <= k && k < idx ==> (types[k] == "file" || types[k] == "symlink")
	for idx := 0; idx < len(types); idx++ {
		if types[idx] != "file" && types[idx] != "symlink" {
			return fmt.Errorf("invalid file type %q", types[idx])
		}
	}
	return nil
}
