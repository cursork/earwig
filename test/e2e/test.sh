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
# DONE
# =========================================================
summary
