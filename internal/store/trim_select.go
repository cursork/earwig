package store

// trimDeletes reports whether a snapshot should be deleted by a trim: it is
// older than the cutoff and is neither the boundary (the newest snapshot older
// than the cutoff — the retained floor) nor the current HEAD.
//
// Gobra-verified safety projections: a snapshot the gate marks for deletion is
// always older than the cutoff, is never the boundary, and is never HEAD.
//
// @ ensures res ==> createdUnix < cutoffUnix
// @ ensures res ==> id != boundaryID
// @ ensures res ==> id != headID
func trimDeletes(id, createdUnix, cutoffUnix, boundaryID, headID int64) (res bool) {
	return createdUnix < cutoffUnix && id != boundaryID && id != headID
}

// selectTrimTargets returns the ids of snapshots a trim should delete: every
// snapshot created strictly before cutoffUnix, EXCEPT the boundary (the newest
// such snapshot, kept as the retained floor so a trim can never destroy all
// history) and headID (the current HEAD, never deleted so its pointer can't
// dangle). ids[i] was created at createdUnix[i]; the two slices are parallel and
// equal length. Returns nil when nothing is older than the cutoff.
//
// Gobra-verified: the returned delete-set never contains headID — a trim can
// never select the current HEAD snapshot for deletion. The set is built with
// make + index-assign + reslice rather than append, whose Gobra signature takes
// a permission and so cannot be annotated directly.
//
// @ requires len(ids) == len(createdUnix)
// @ requires forall i int :: {&ids[i]} 0 <= i && i < len(ids) ==> acc(&ids[i], 1/2)
// @ requires forall i int :: {&createdUnix[i]} 0 <= i && i < len(createdUnix) ==> acc(&createdUnix[i], 1/2)
// @ ensures acc(res)
// @ ensures forall j int :: {&res[j]} 0 <= j && j < len(res) ==> res[j] != headID
func selectTrimTargets(ids, createdUnix []int64, cutoffUnix, headID int64) (res []int64) {
	// Boundary = the eligible snapshot (createdUnix < cutoffUnix) with the
	// greatest createdUnix, tie-broken by greatest id.
	boundaryID := int64(-1)
	var boundaryCreated int64
	haveBoundary := false
	// @ invariant 0 <= i && i <= len(ids)
	// @ invariant len(ids) == len(createdUnix)
	// @ invariant forall k int :: {&ids[k]} 0 <= k && k < len(ids) ==> acc(&ids[k], 1/2)
	// @ invariant forall k int :: {&createdUnix[k]} 0 <= k && k < len(createdUnix) ==> acc(&createdUnix[k], 1/2)
	for i := 0; i < len(ids); i++ {
		if createdUnix[i] >= cutoffUnix {
			continue
		}
		if !haveBoundary ||
			createdUnix[i] > boundaryCreated ||
			(createdUnix[i] == boundaryCreated && ids[i] > boundaryID) {
			haveBoundary = true
			boundaryID = ids[i]
			boundaryCreated = createdUnix[i]
		}
	}
	if !haveBoundary {
		return nil // nothing older than the cutoff → nothing to trim
	}

	// Fill a preallocated buffer by index, then return the used prefix.
	buf := make([]int64, len(ids))
	w := 0
	// @ invariant 0 <= i && i <= len(ids)
	// @ invariant 0 <= w && w <= i
	// @ invariant len(ids) == len(createdUnix)
	// @ invariant len(buf) == len(ids)
	// @ invariant forall k int :: {&ids[k]} 0 <= k && k < len(ids) ==> acc(&ids[k], 1/2)
	// @ invariant forall k int :: {&createdUnix[k]} 0 <= k && k < len(createdUnix) ==> acc(&createdUnix[k], 1/2)
	// @ invariant acc(buf)
	// @ invariant forall j int :: {&buf[j]} 0 <= j && j < w ==> buf[j] != headID
	for i := 0; i < len(ids); i++ {
		if trimDeletes(ids[i], createdUnix[i], cutoffUnix, boundaryID, headID) {
			buf[w] = ids[i] // trimDeletes ensures ids[i] != headID
			w++
		}
	}
	return buf[:w]
}
