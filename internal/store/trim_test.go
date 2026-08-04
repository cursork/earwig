package store

import (
	"fmt"
	"testing"
	"time"
)

// Comparable timeline: ids 1..6, created times 10..60 (F=60 newest).
// Mirrors the A(30d) B(20d) C(10d) D(5d) E(2d) F(now) example.
func TestSelectTrimTargets(t *testing.T) {
	ids := []int64{1, 2, 3, 4, 5, 6}
	created := []int64{10, 20, 30, 40, 50, 60}

	cases := []struct {
		name    string
		ids     []int64
		created []int64
		cutoff  int64
		head    int64
		want    []int64
	}{
		{
			// cutoff between C(30) and D(40): A,B,C eligible; boundary=C(3) kept.
			name: "keep boundary and newer", ids: ids, created: created,
			cutoff: 35, head: 6, want: []int64{1, 2},
		},
		{
			// trim "to now": cutoff past the newest, so all eligible; boundary=F.
			name: "collapse to newest", ids: ids, created: created,
			cutoff: 61, head: 6, want: []int64{1, 2, 3, 4, 5},
		},
		{
			// nothing older than the cutoff → no-op.
			name: "nothing older", ids: ids, created: created,
			cutoff: 5, head: 6, want: nil,
		},
		{
			// cutoff exactly at C(30): only A,B older; C kept (>= cutoff). Models
			// `trim <ref>` keeping the ref's own snapshot.
			name: "cutoff at a snapshot keeps it", ids: ids, created: created,
			cutoff: 30, head: 6, want: []int64{1},
		},
		{
			// HEAD is an old snapshot (post-restore). cutoff=45 → A,B,C,D eligible,
			// boundary=D(4); head=B(2) also protected → targets A,C.
			name: "protects old head", ids: ids, created: created,
			cutoff: 45, head: 2, want: []int64{1, 3},
		},
		{
			// Single snapshot is always kept (it is the boundary).
			name: "single snapshot never deleted", ids: []int64{1}, created: []int64{10},
			cutoff: 99, head: 1, want: nil,
		},
		{
			// Boundary tie-break: equal created times, pick greatest id.
			name: "boundary tie-break by id", ids: []int64{1, 2, 3}, created: []int64{10, 10, 20},
			cutoff: 15, head: 3, want: []int64{1},
		},
		{
			// No head protection (head=-1): only the boundary is spared.
			name: "no head", ids: ids, created: created,
			cutoff: 35, head: -1, want: []int64{1, 2},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := selectTrimTargets(tc.ids, tc.created, tc.cutoff, tc.head)
			if !equalIDs(got, tc.want) {
				t.Fatalf("selectTrimTargets = %v, want %v", got, tc.want)
			}
			// Safety invariants (mirror the intended Gobra postconditions):
			for _, id := range got {
				if id == tc.head {
					t.Errorf("target %d is the protected HEAD", id)
				}
				// every target must be an eligible input id
				idx := indexOf(tc.ids, id)
				if idx < 0 {
					t.Errorf("target %d is not an input id", id)
				} else if tc.created[idx] >= tc.cutoff {
					t.Errorf("target %d (created %d) is not older than cutoff %d", id, tc.created[idx], tc.cutoff)
				}
			}
			// The boundary (newest eligible) must never be a target.
			if b, ok := boundaryOf(tc.ids, tc.created, tc.cutoff); ok && contains(got, b) {
				t.Errorf("boundary %d was selected for deletion", b)
			}
		})
	}
}

func TestStoreTrim(t *testing.T) {
	s := testStore(t)
	now := time.Now()
	times := []time.Time{
		now.Add(-30 * 24 * time.Hour), // A
		now.Add(-20 * 24 * time.Hour), // B
		now.Add(-10 * 24 * time.Hour), // C (boundary for trim 7d)
		now.Add(-5 * 24 * time.Hour),  // D
		now.Add(-2 * 24 * time.Hour),  // E
		now,                           // F (HEAD)
	}
	snaps := createChainWithTimes(t, s, times)
	head := snaps[5].ID
	cutoff := now.Add(-7 * 24 * time.Hour).Unix()

	// Preview: A and B are deleted; C is the retained floor.
	targets, err := s.TrimTargets(cutoff, head)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 2 || targets[0].ID != snaps[0].ID || targets[1].ID != snaps[1].ID {
		t.Fatalf("TrimTargets = %v, want [A B]", ids(targets))
	}

	n, err := s.Trim(cutoff, head)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("Trim deleted %d, want 2", n)
	}

	remaining, err := s.ListSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 4 {
		t.Fatalf("remaining = %d snapshots, want 4 (C D E F)", len(remaining))
	}
	// C is now a root; D→C, E→D, F→E lineage intact.
	byID := map[int64]Snapshot{}
	for _, sn := range remaining {
		byID[sn.ID] = sn
	}
	c := byID[snaps[2].ID]
	if c.ParentID != nil {
		t.Errorf("boundary C should be a root, got parent %v", *c.ParentID)
	}
	if d := byID[snaps[3].ID]; d.ParentID == nil || *d.ParentID != snaps[2].ID {
		t.Errorf("D should parent onto C")
	}
	if f := byID[snaps[5].ID]; f.ParentID == nil || *f.ParentID != snaps[4].ID {
		t.Errorf("F should still parent onto E")
	}

	// GC removes the two now-orphaned blobs; the boundary's content survives.
	freed, err := s.GarbageCollect()
	if err != nil {
		t.Fatal(err)
	}
	if freed != 2 {
		t.Errorf("GC freed %d blobs, want 2", freed)
	}
	assertSnapshotContent(t, s, snaps[2].ID, "f.txt", "content-2")

	if err := s.Vacuum(); err != nil {
		t.Fatalf("Vacuum: %v", err)
	}
	// Data still intact after vacuum.
	assertSnapshotContent(t, s, snaps[2].ID, "f.txt", "content-2")
}

func TestStoreTrimProtectsHead(t *testing.T) {
	s := testStore(t)
	now := time.Now()
	times := []time.Time{
		now.Add(-30 * 24 * time.Hour),
		now.Add(-20 * 24 * time.Hour),
		now.Add(-10 * 24 * time.Hour),
	}
	snaps := createChainWithTimes(t, s, times)
	// HEAD is the OLDEST snapshot (post-restore situation). Trim everything.
	head := snaps[0].ID
	cutoff := now.Unix()

	n, err := s.Trim(cutoff, head)
	if err != nil {
		t.Fatal(err)
	}
	remaining, _ := s.ListSnapshots()
	survived := map[int64]bool{}
	for _, sn := range remaining {
		survived[sn.ID] = true
	}
	// Boundary (newest older-than-cutoff = snaps[2]) and HEAD (snaps[0]) survive.
	if !survived[snaps[2].ID] {
		t.Error("boundary snapshot was deleted")
	}
	if !survived[snaps[0].ID] {
		t.Error("HEAD snapshot was deleted — pointer would dangle")
	}
	if n != 1 { // only the middle snapshot is deletable
		t.Errorf("Trim deleted %d, want 1", n)
	}
}

func TestStoreTrimNoop(t *testing.T) {
	s := testStore(t)
	now := time.Now()
	snaps := createChainWithTimes(t, s, []time.Time{now.Add(-time.Hour), now})
	// Cutoff older than everything → nothing to trim.
	cutoff := now.Add(-24 * time.Hour).Unix()
	n, err := s.Trim(cutoff, snaps[1].ID)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("Trim deleted %d, want 0", n)
	}
	if remaining, _ := s.ListSnapshots(); len(remaining) != 2 {
		t.Errorf("remaining = %d, want 2", len(remaining))
	}
}

// ── helpers ─────────────────────────────────────────────

func createChainWithTimes(t *testing.T, s *Store, times []time.Time) []*Snapshot {
	t.Helper()
	var snaps []*Snapshot
	var parent *int64
	for i, tm := range times {
		content := fmt.Sprintf("content-%d", i)
		h, err := s.PutBlob([]byte(content))
		if err != nil {
			t.Fatal(err)
		}
		files := []SnapshotFile{{Path: "f.txt", BlobHash: h, Mode: 0644, ModTime: tm, Size: int64(len(content))}}
		snap, err := s.CreateSnapshot(parent, files, "auto")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec(`UPDATE snapshots SET created_at = ? WHERE id = ?`,
			tm.UTC().Format(time.RFC3339), snap.ID); err != nil {
			t.Fatal(err)
		}
		snap.CreatedAt = tm
		snaps = append(snaps, snap)
		parent = &snap.ID
	}
	return snaps
}

func assertSnapshotContent(t *testing.T, s *Store, snapID int64, path, want string) {
	t.Helper()
	var blobHash string
	if err := s.db.QueryRow(
		`SELECT blob_hash FROM snapshot_files WHERE snapshot_id = ? AND path = ?`,
		snapID, path).Scan(&blobHash); err != nil {
		t.Fatalf("snapshot %d missing %s: %v", snapID, path, err)
	}
	data, err := s.GetBlob(blobHash)
	if err != nil {
		t.Fatalf("GetBlob for %s: %v", path, err)
	}
	if string(data) != want {
		t.Errorf("content of %s = %q, want %q", path, data, want)
	}
}

func equalIDs(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func contains(s []int64, v int64) bool { return indexOf(s, v) >= 0 }

func indexOf(s []int64, v int64) int {
	for i := range s {
		if s[i] == v {
			return i
		}
	}
	return -1
}

func boundaryOf(ids, created []int64, cutoff int64) (int64, bool) {
	best := int64(-1)
	var bestCreated int64
	found := false
	for i := range ids {
		if created[i] >= cutoff {
			continue
		}
		if !found || created[i] > bestCreated || (created[i] == bestCreated && ids[i] > best) {
			found, best, bestCreated = true, ids[i], created[i]
		}
	}
	return best, found
}

func ids(snaps []Snapshot) []int64 {
	out := make([]int64, len(snaps))
	for i, sn := range snaps {
		out[i] = sn.ID
	}
	return out
}
