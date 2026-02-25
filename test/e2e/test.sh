#!/usr/bin/env bash
# ---------------------------------------------------------
# earwig E2E test suite
#
# Tests the full CLI through many operations:
# create, modify, delete, snapshot, restore, branching.
# ---------------------------------------------------------
set -euo pipefail
source "$(dirname "$0")/dsl.sh"

# =========================================================
# TEST 1: Basic snapshot and restore
# =========================================================
blue "=== TEST 1: Basic snapshot and restore ==="

init_project /tmp/earwig-test-1

write_file "hello.txt"     "hello world"
write_file "sub/nested.txt" "nested content"
snapshot                                        # snapshot #1

expect_snapshot_count 1
expect_show_change 1 A "hello.txt"
expect_show_change 1 A "sub/nested.txt"

write_file "hello.txt" "hello modified"
snapshot                                        # snapshot #2

expect_snapshot_count 2
expect_show_change 2 M "hello.txt"

restore 1
expect_file "hello.txt"      "hello world"
expect_file "sub/nested.txt"  "nested content"

# =========================================================
# TEST 2: Deletes and additions across snapshots
# =========================================================
blue "=== TEST 2: Deletes and additions ==="

init_project /tmp/earwig-test-2

write_file "a.txt" "aaa"
write_file "b.txt" "bbb"
write_file "c.txt" "ccc"
snapshot                                        # snapshot #1

delete_file "b.txt"
write_file  "d.txt" "ddd"
snapshot                                        # snapshot #2

expect_show_change 2 D "b.txt"
expect_show_change 2 A "d.txt"

write_file "a.txt" "aaa-modified"
delete_file "c.txt"
delete_file "d.txt"
write_file  "e.txt" "eee"
write_file  "f.txt" "fff"
snapshot                                        # snapshot #3

expect_show_change 3 M "a.txt"
expect_show_change 3 D "c.txt"
expect_show_change 3 D "d.txt"
expect_show_change 3 A "e.txt"
expect_show_change 3 A "f.txt"
expect_snapshot_count 3

# Restore all the way back to #1
restore 1
expect_file    "a.txt" "aaa"
expect_file    "b.txt" "bbb"
expect_file    "c.txt" "ccc"
expect_no_file "d.txt"
expect_no_file "e.txt"
expect_no_file "f.txt"

# Restore forward to #3
restore 3
expect_file    "a.txt" "aaa-modified"
expect_no_file "b.txt"
expect_no_file "c.txt"
expect_no_file "d.txt"
expect_file    "e.txt" "eee"
expect_file    "f.txt" "fff"

# =========================================================
# TEST 3: Directory operations
# =========================================================
blue "=== TEST 3: Directory operations ==="

init_project /tmp/earwig-test-3

write_file "src/main.go"       "package main"
write_file "src/util/helper.go" "package util"
write_file "docs/readme.txt"    "readme"
snapshot                                        # snapshot #1

# Delete entire directory
delete_dir "src"
snapshot                                        # snapshot #2

expect_show_change 2 D "src/main.go"
expect_show_change 2 D "src/util/helper.go"
expect_no_file "src/main.go"
expect_no_dir  "src"

# Restore brings back the directory and all its files
restore 1
expect_file "src/main.go"        "package main"
expect_file "src/util/helper.go"  "package util"
expect_dir  "src"
expect_dir  "src/util"

# =========================================================
# TEST 4: Branching
# =========================================================
blue "=== TEST 4: Branching ==="

init_project /tmp/earwig-test-4

write_file "base.txt" "base"
snapshot                                        # snapshot #1

write_file "base.txt" "branch-a"
snapshot                                        # snapshot #2

# Go back to #1 and create a different branch
restore 1
expect_file "base.txt" "base"

write_file "base.txt" "branch-b"
snapshot                                        # snapshot #3

expect_log_has_branch
expect_snapshot_count 3

# Both branches are independently restorable
restore 2
expect_file "base.txt" "branch-a"

restore 3
expect_file "base.txt" "branch-b"

restore 1
expect_file "base.txt" "base"

# =========================================================
# TEST 5: No-change detection
# =========================================================
blue "=== TEST 5: No-change detection ==="

init_project /tmp/earwig-test-5

write_file "x.txt" "content"
snapshot                                        # snapshot #1

expect_no_changes                               # should detect no diff
expect_snapshot_count 1                         # still 1

# =========================================================
# TEST 6: Many rapid changes
# =========================================================
blue "=== TEST 6: Many rapid changes ==="

init_project /tmp/earwig-test-6

# Create 20 files
for i in $(seq 1 20); do
    write_file "file-$i.txt" "content-$i"
done
snapshot                                        # snapshot #1
expect_snapshot_count 1

# Modify half, delete a quarter, add 5 new
for i in $(seq 1 10); do
    write_file "file-$i.txt" "modified-$i"
done
for i in $(seq 16 20); do
    delete_file "file-$i.txt"
done
for i in $(seq 21 25); do
    write_file "file-$i.txt" "new-$i"
done
snapshot                                        # snapshot #2

# Verify the changes
for i in $(seq 1 10); do
    expect_show_change 2 M "file-$i.txt"
done
for i in $(seq 16 20); do
    expect_show_change 2 D "file-$i.txt"
done
for i in $(seq 21 25); do
    expect_show_change 2 A "file-$i.txt"
done

# Restore back to #1 and verify all 20 original files
restore 1
for i in $(seq 1 20); do
    expect_file "file-$i.txt" "content-$i"
done
for i in $(seq 21 25); do
    expect_no_file "file-$i.txt"
done

# =========================================================
# TEST 7: Deep nesting and path edge cases
# =========================================================
blue "=== TEST 7: Deep nesting and edge cases ==="

init_project /tmp/earwig-test-7

write_file "a/b/c/d/e/deep.txt" "deep"
write_file "spaces in name.txt"  "spaces"
write_file "unicode-cafe.txt"    "cafe"
snapshot                                        # snapshot #1

delete_dir "a"
delete_file "spaces in name.txt"
snapshot                                        # snapshot #2

expect_no_file "a/b/c/d/e/deep.txt"
expect_no_dir  "a"
expect_no_file "spaces in name.txt"
expect_file    "unicode-cafe.txt" "cafe"

restore 1
expect_file "a/b/c/d/e/deep.txt" "deep"
expect_file "spaces in name.txt"  "spaces"

# =========================================================
# TEST 8: Duplicate content across files (dedup)
# =========================================================
blue "=== TEST 8: Duplicate content dedup ==="

init_project /tmp/earwig-test-8

write_file "copy1.txt" "identical content"
write_file "copy2.txt" "identical content"
write_file "copy3.txt" "identical content"
write_file "different.txt" "different"
snapshot                                        # snapshot #1

# All copies should be restorable independently
delete_file "copy1.txt"
delete_file "copy2.txt"
snapshot                                        # snapshot #2

expect_no_file "copy1.txt"
expect_no_file "copy2.txt"
expect_file    "copy3.txt" "identical content"

restore 1
expect_file "copy1.txt" "identical content"
expect_file "copy2.txt" "identical content"
expect_file "copy3.txt" "identical content"

# =========================================================
# TEST 9: Restore to middle of chain, edit, branch
# =========================================================
blue "=== TEST 9: Multi-step restore and branch ==="

init_project /tmp/earwig-test-9

write_file "log.txt" "line1"
snapshot                                        # snapshot #1

append_file "log.txt" "\nline2"
snapshot                                        # snapshot #2

append_file "log.txt" "\nline3"
snapshot                                        # snapshot #3

append_file "log.txt" "\nline4"
snapshot                                        # snapshot #4

append_file "log.txt" "\nline5"
snapshot                                        # snapshot #5

expect_snapshot_count 5

# Restore to #2 (3 snapshots back)
restore 2
expect_file "log.txt" "line1\nline2"

# Branch from #2
write_file "log.txt" "line1\nline2\nalternate3"
snapshot                                        # snapshot #6

expect_snapshot_count 6
expect_log_has_branch

# Can still get to any point
restore 5
expect_file "log.txt" "line1\nline2\nline3\nline4\nline5"

restore 6
expect_file "log.txt" "line1\nline2\nalternate3"

restore 1
expect_file "log.txt" "line1"

# =========================================================
# TEST 10: Empty file and overwrite with empty
# =========================================================
blue "=== TEST 10: Empty files ==="

init_project /tmp/earwig-test-10

write_file "empty.txt" ""
write_file "full.txt"  "content"
snapshot                                        # snapshot #1

expect_file "empty.txt" ""

write_file "full.txt" ""
write_file "empty.txt" "now has content"
snapshot                                        # snapshot #2

expect_show_change 2 M "empty.txt"
expect_show_change 2 M "full.txt"

restore 1
expect_file "empty.txt" ""
expect_file "full.txt"  "content"

# =========================================================
# TEST 11: Path traversal protection
# =========================================================
blue "=== TEST 11: Path traversal protection ==="

init_project /tmp/earwig-test-11

# Create a canary file outside the project
echo "canary" > /tmp/earwig-canary

# Create a legit snapshot first
write_file "legit.txt" "legit"
snapshot                                        # snapshot #1

# Manually inject a malicious snapshot_files row with a ".." path.
# First, store a blob for the malicious content.
db=".earwig/earwig.db"
snap_id=$(sqlite3 "$db" "SELECT id FROM snapshots ORDER BY id DESC LIMIT 1")
blob_hash=$(sqlite3 "$db" "SELECT hash FROM blobs LIMIT 1")

# Insert a new snapshot with a traversal path
sqlite3 "$db" "INSERT INTO snapshots (hash, parent_id, created_at, message) VALUES ('deadbeef', $snap_id, datetime('now'), 'malicious')"
mal_id=$(sqlite3 "$db" "SELECT id FROM snapshots WHERE hash='deadbeef'")
sqlite3 "$db" "INSERT INTO snapshot_files (snapshot_id, path, blob_hash, mode, mod_time, size) VALUES ($mal_id, '../earwig-canary', '$blob_hash', 420, datetime('now'), 5)"
sqlite3 "$db" "INSERT INTO snapshot_files (snapshot_id, path, blob_hash, mode, mod_time, size) VALUES ($mal_id, 'legit.txt', '$blob_hash', 420, datetime('now'), 5)"

# Attempt restore — should fail
if earwig restore deadbeef 2>/dev/null; then
    fail "restore with traversal path should fail" "restore succeeded"
else
    pass "restore with traversal path rejected"
fi

# Canary must be untouched
canary_content=$(cat /tmp/earwig-canary)
if [ "$canary_content" = "canary" ]; then
    pass "canary file not modified"
else
    fail "canary file not modified" "content changed to: $canary_content"
fi

rm -f /tmp/earwig-canary

# =========================================================
# TEST 12: Symlink protection during restore
# =========================================================
blue "=== TEST 12: Symlink protection ==="

init_project /tmp/earwig-test-12

write_file "target.txt" "original content"
write_file "sub/deep.txt" "deep content"
snapshot                                        # snapshot #1

# Create a canary outside the project
echo "do not touch" > /tmp/earwig-symlink-canary

# Replace target.txt with a symlink to the canary
rm -f target.txt
ln -s /tmp/earwig-symlink-canary target.txt

# Replace sub/ directory with a symlink to /tmp
rm -rf sub
ln -s /tmp sub

# Restore should replace the symlinks with real files
restore 1

# target.txt should be a regular file with original content
if [ -L target.txt ]; then
    fail "target.txt should not be a symlink after restore" "still a symlink"
else
    expect_file "target.txt" "original content"
fi

# sub should be a real directory
if [ -L sub ]; then
    fail "sub/ should not be a symlink after restore" "still a symlink"
else
    expect_file "sub/deep.txt" "deep content"
fi

# Canary must be untouched
canary=$(cat /tmp/earwig-symlink-canary)
if [ "$canary" = "do not touch" ]; then
    pass "symlink canary not modified"
else
    fail "symlink canary not modified" "content changed to: $canary"
fi

rm -f /tmp/earwig-symlink-canary

# =========================================================
# TEST 13: Watch mode E2E
# =========================================================
blue "=== TEST 13: Watch mode ==="

init_project /tmp/earwig-test-13

write_file "initial.txt" "initial"
snapshot                                        # snapshot #1

# Write a new file BEFORE starting watcher — so the initial snapshot has changes
write_file "watched.txt" "created before watcher"

# Start watcher in background
earwig watch > /tmp/earwig-watch.log 2>&1 &
WATCH_PID=$!
sleep 5  # Let watcher start and take initial snapshot (immediate, no debounce)

# Kill watcher
kill $WATCH_PID 2>/dev/null || true
wait $WATCH_PID 2>/dev/null || true

# Check that the watcher created at least one snapshot (its initial full walk)
watch_snaps=$(earwig log 2>/dev/null | grep -c '^\*' || true)
# We expect: snapshot #1 (manual) + watcher initial snapshot = at least 2
if [ "$watch_snaps" -ge 2 ]; then
    pass "watcher created snapshots ($watch_snaps total)"
else
    fail "watcher created snapshots" "only $watch_snaps found"
fi

# The watched file should appear in the watcher's initial snapshot
latest_hash=$(earwig log 2>/dev/null | head -1 | awk '{print $2}')
files_output=$(earwig _files "$latest_hash" 2>/dev/null)
if echo "$files_output" | grep -q "watched.txt"; then
    pass "watched.txt appears in watcher snapshot"
else
    fail "watched.txt appears in watcher snapshot" "not found in $latest_hash"
fi

# =========================================================
# TEST 14: Empty files
# =========================================================
blue "=== TEST 14: Empty files ==="

init_project /tmp/earwig-test-14

write_file "empty1.txt" ""
write_file "empty2.txt" ""
write_file "notempty.txt" "has content"
snapshot                                        # snapshot #1

expect_file "empty1.txt" ""
expect_file "empty2.txt" ""
expect_file_size "empty1.txt" "0"
expect_file_size "empty2.txt" "0"

# Verify _files shows zero size
files_output=$(earwig _files "${SNAPSHOTS[0]}" 2>/dev/null)
if echo "$files_output" | grep "empty1.txt" | grep -q "	0	"; then
    pass "empty1.txt has size 0 in DB"
else
    fail "empty1.txt has size 0 in DB" "output: $(echo "$files_output" | grep empty1)"
fi

# Make empty file non-empty, and non-empty file empty
write_file "empty1.txt" "now has content"
write_file "notempty.txt" ""
snapshot                                        # snapshot #2

expect_show_change 2 M "empty1.txt"
expect_show_change 2 M "notempty.txt"

# Restore and verify empty files come back correctly
restore 1
expect_file "empty1.txt" ""
expect_file "notempty.txt" "has content"
expect_file_size "empty1.txt" "0"

# =========================================================
# TEST 15: Special filenames
# =========================================================
blue "=== TEST 15: Special filenames ==="

init_project /tmp/earwig-test-15

write_file "spaces in name.txt" "spaces"
write_file "tab	here.txt" "tab"
write_file "UPPER.TXT" "upper"
write_file "MiXeD.CaSe" "mixed"
write_file "dots.in.name.txt" "dots"
write_file "no-extension" "noext"
write_file ".hidden" "hidden"
write_file "special-chars_v2 (copy).txt" "parens"
write_file "sub dir/file name.txt" "nested spaces"
snapshot                                        # snapshot #1

# Delete all and restore
rm -rf "spaces in name.txt" "tab	here.txt" "UPPER.TXT" "MiXeD.CaSe" \
       "dots.in.name.txt" "no-extension" ".hidden" "special-chars_v2 (copy).txt" "sub dir"

restore 1
expect_file "spaces in name.txt" "spaces"
expect_file "tab	here.txt" "tab"
expect_file "UPPER.TXT" "upper"
expect_file "MiXeD.CaSe" "mixed"
expect_file "dots.in.name.txt" "dots"
expect_file "no-extension" "noext"
expect_file ".hidden" "hidden"
expect_file "special-chars_v2 (copy).txt" "parens"
expect_file "sub dir/file name.txt" "nested spaces"

# =========================================================
# TEST 16: Restore idempotency
# =========================================================
blue "=== TEST 16: Restore idempotency ==="

init_project /tmp/earwig-test-16

write_file "a.txt" "aaa"
write_file "b.txt" "bbb"
write_file "sub/c.txt" "ccc"
snapshot                                        # snapshot #1

write_file "a.txt" "modified"
write_file "d.txt" "ddd"
delete_file "b.txt"
snapshot                                        # snapshot #2

# Restore to #1
restore 1
expect_file "a.txt" "aaa"
expect_file "b.txt" "bbb"
expect_file "sub/c.txt" "ccc"
expect_no_file "d.txt"

# Restore to #1 again — should be a no-op, nothing should break
restore 1
expect_file "a.txt" "aaa"
expect_file "b.txt" "bbb"
expect_file "sub/c.txt" "ccc"
expect_no_file "d.txt"

# And a third time
restore 1
expect_file "a.txt" "aaa"
expect_file "b.txt" "bbb"
expect_file "sub/c.txt" "ccc"
expect_no_file "d.txt"
pass "triple restore is idempotent"

# =========================================================
# TEST 17: File permissions round-trip
# =========================================================
blue "=== TEST 17: File permissions ==="

init_project /tmp/earwig-test-17

write_file "normal.txt" "normal"
write_file "exec.sh" "#!/bin/sh\necho hi"
chmod 755 "exec.sh"
write_file "readonly.txt" "readonly"
chmod 444 "readonly.txt"
snapshot                                        # snapshot #1

expect_file_mode "normal.txt" "644"
expect_file_mode "exec.sh" "755"
expect_file_mode "readonly.txt" "444"

# Modify permissions and content
chmod 644 "readonly.txt"
write_file "readonly.txt" "was readonly"
chmod 644 "exec.sh"
snapshot                                        # snapshot #2

# Restore to #1 — permissions should come back
restore 1
expect_file "exec.sh" "#!/bin/sh\necho hi"
expect_file "readonly.txt" "readonly"
expect_file_mode "exec.sh" "755"
expect_file_mode "readonly.txt" "444"

# =========================================================
# TEST 18: Schema migration v1 -> v3
# =========================================================
blue "=== TEST 18: Schema migration ==="

init_project /tmp/earwig-test-18

db=".earwig/earwig.db"

# Downgrade to v1: remove type and encoding columns, set version to 1
sqlite3 "$db" "
    CREATE TABLE snapshot_files_backup AS SELECT snapshot_id, path, blob_hash, mode, mod_time, size FROM snapshot_files;
    DROP TABLE snapshot_files;
    ALTER TABLE snapshot_files_backup RENAME TO snapshot_files;
    CREATE TABLE blobs_backup AS SELECT hash, size, data FROM blobs;
    DROP TABLE blobs;
    ALTER TABLE blobs_backup RENAME TO blobs;
    UPDATE meta SET value = '1' WHERE key = 'schema_version';
"

# Verify it's v1
v=$(sqlite3 "$db" "SELECT value FROM meta WHERE key='schema_version'")
if [ "$v" = "1" ]; then
    pass "downgraded to v1"
else
    fail "downgraded to v1" "version is $v"
fi

# Now open with earwig — migration should run (v1 -> v2 -> v3)
write_file "migrated.txt" "after migration"
snapshot                                        # snapshot #1

# Verify schema is now v3
v=$(sqlite3 "$db" "SELECT value FROM meta WHERE key='schema_version'")
if [ "$v" = "3" ]; then
    pass "migrated to v3"
else
    fail "migrated to v3" "version is $v"
fi

# Verify the type column exists and works
type_val=$(sqlite3 "$db" "SELECT type FROM snapshot_files LIMIT 1")
if [ "$type_val" = "file" ]; then
    pass "type column works after migration"
else
    fail "type column works after migration" "got $type_val"
fi

# Verify encoding column exists and works
enc_val=$(sqlite3 "$db" "SELECT encoding FROM blobs LIMIT 1")
if [ "$enc_val" = "raw" ] || [ "$enc_val" = "zstd" ]; then
    pass "encoding column works after migration"
else
    fail "encoding column works after migration" "got $enc_val"
fi

# Verify snapshot/restore still works on migrated DB
expect_file "migrated.txt" "after migration"
restore 1
expect_file "migrated.txt" "after migration"

# =========================================================
# TEST 19: Very long paths
# =========================================================
blue "=== TEST 19: Very long paths ==="

init_project /tmp/earwig-test-19

# Create a deeply nested path (10 levels, ~80 chars total)
write_file "a/bb/ccc/dddd/eeeee/ffffff/ggggggg/hhhhhhhh/iiiiiiiii/deep.txt" "deep content"

# Create a file with a long name (~200 chars)
longname=$(printf 'x%.0s' $(seq 1 200))
write_file "dir/${longname}.txt" "long name content"

snapshot                                        # snapshot #1

# Delete everything and restore
rm -rf a dir
restore 1
expect_file "a/bb/ccc/dddd/eeeee/ffffff/ggggggg/hhhhhhhh/iiiiiiiii/deep.txt" "deep content"
expect_file "dir/${longname}.txt" "long name content"

# =========================================================
# TEST 20: Binary content
# =========================================================
blue "=== TEST 20: Binary content ==="

init_project /tmp/earwig-test-20

# Create files with binary content (null bytes, high bytes, all byte values)
printf '\x00\x01\x02\xff\xfe\xfd' > "binary.bin"
printf 'text\x00with\x00nulls' > "nulls.txt"

# Create a file with every byte value 0-255
awk 'BEGIN { for (i=0; i<256; i++) printf "%c", i }' > "allbytes.bin"

snapshot                                        # snapshot #1

# Record checksums
sum_binary=$(sha256sum "binary.bin" | awk '{print $1}')
sum_nulls=$(sha256sum "nulls.txt" | awk '{print $1}')
sum_allbytes=$(sha256sum "allbytes.bin" | awk '{print $1}')

# Delete and restore
rm -f "binary.bin" "nulls.txt" "allbytes.bin"
restore 1

# Verify via checksums (can't use string comparison for binary)
new_sum_binary=$(sha256sum "binary.bin" | awk '{print $1}')
new_sum_nulls=$(sha256sum "nulls.txt" | awk '{print $1}')
new_sum_allbytes=$(sha256sum "allbytes.bin" | awk '{print $1}')

if [ "$sum_binary" = "$new_sum_binary" ]; then
    pass "binary.bin restored correctly"
else
    fail "binary.bin restored correctly" "checksum mismatch"
fi

if [ "$sum_nulls" = "$new_sum_nulls" ]; then
    pass "nulls.txt restored correctly"
else
    fail "nulls.txt restored correctly" "checksum mismatch"
fi

if [ "$sum_allbytes" = "$new_sum_allbytes" ]; then
    pass "allbytes.bin restored correctly (all 256 byte values)"
else
    fail "allbytes.bin restored correctly" "checksum mismatch"
fi

expect_file_size "allbytes.bin" "256"

# =========================================================
# TEST 21: .gitignore integration
# =========================================================
blue "=== TEST 21: .gitignore integration ==="

init_project /tmp/earwig-test-21

printf '*.log\nbuild/\n' > ".gitignore"
write_file "main.go" "package main"
write_file "debug.log" "should be ignored"
write_file "build/output.bin" "should be ignored"
write_file "src/app.go" "package src"
snapshot                                        # snapshot #1

# Verify ignored files are not in the snapshot
files_output=$(earwig _files "${SNAPSHOTS[0]}" 2>/dev/null)

if echo "$files_output" | grep -q "debug.log"; then
    fail "debug.log should be ignored" "found in snapshot"
else
    pass "debug.log excluded by .gitignore"
fi

if echo "$files_output" | grep -q "build/output.bin"; then
    fail "build/output.bin should be ignored" "found in snapshot"
else
    pass "build/output.bin excluded by .gitignore"
fi

if echo "$files_output" | grep -q "main.go"; then
    pass "main.go included in snapshot"
else
    fail "main.go included in snapshot" "not found"
fi

if echo "$files_output" | grep -q "src/app.go"; then
    pass "src/app.go included in snapshot"
else
    fail "src/app.go included in snapshot" "not found"
fi

# .gitignore itself should be tracked
if echo "$files_output" | grep -q ".gitignore"; then
    pass ".gitignore itself is tracked"
else
    fail ".gitignore itself is tracked" "not found"
fi

# =========================================================
# DONE
# =========================================================
summary
