package store

import (
	"crypto/sha256"
	"encoding/hex"
)

// computeBlobHash computes the SHA-256 hash of data and returns it as a
// 64-character hex string. This is used by PutBlob (to compute the storage
// key) and GetBlob (to verify integrity after retrieval).
//
// Not formally verified: Gobra cannot handle [32]byte → []byte slicing (h[:]).
func computeBlobHash(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
