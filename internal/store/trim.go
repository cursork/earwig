package store

import (
	"fmt"
	"sort"
)

// TrimTargets returns the snapshots a trim to cutoffUnix would delete, given the
// protected headID (pass -1 for none). Read-only — used for the preview and by
// Trim. Sorted by id ascending (ancestors before descendants).
func (s *Store) TrimTargets(cutoffUnix, headID int64) ([]Snapshot, error) {
	snaps, err := s.ListSnapshots()
	if err != nil {
		return nil, err
	}
	ids := make([]int64, len(snaps))
	created := make([]int64, len(snaps))
	byID := make(map[int64]Snapshot, len(snaps))
	for i, sn := range snaps {
		ids[i] = sn.ID
		created[i] = sn.CreatedAt.Unix()
		byID[sn.ID] = sn
	}
	targetIDs := selectTrimTargets(ids, created, cutoffUnix, headID)
	targets := make([]Snapshot, 0, len(targetIDs))
	for _, id := range targetIDs {
		targets = append(targets, byID[id])
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].ID < targets[j].ID })
	return targets, nil
}

// Trim deletes the snapshots selected by TrimTargets and returns the count.
// Each deletion re-parents children to the deleted snapshot's parent (via the
// existing DeleteSnapshot), so survivors keep a valid lineage and the boundary
// becomes a root. Callers should GarbageCollect and Vacuum afterward to reclaim
// disk. Deletes ancestors before descendants for deterministic behaviour.
func (s *Store) Trim(cutoffUnix, headID int64) (int, error) {
	targets, err := s.TrimTargets(cutoffUnix, headID)
	if err != nil {
		return 0, err
	}
	for _, t := range targets {
		if err := s.DeleteSnapshot(t.ID); err != nil {
			return 0, fmt.Errorf("trimming snapshot %d: %w", t.ID, err)
		}
	}
	return len(targets), nil
}

// Vacuum rebuilds the database file, reclaiming pages freed by deletes and GC.
// Must run outside a transaction; safe under the trim flock (no concurrent
// writer). This is what actually shrinks earwig.db on disk after a trim.
func (s *Store) Vacuum() error {
	if _, err := s.db.Exec(`VACUUM`); err != nil {
		return fmt.Errorf("vacuuming database: %w", err)
	}
	return nil
}
