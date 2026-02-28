#!/usr/bin/env bash
# ---------------------------------------------------------
# earwig E2E test DSL
#
# Provides readable helper functions for writing E2E tests.
# Source this file, then call the functions.
# ---------------------------------------------------------
set -euo pipefail

PASS=0
FAIL=0
SNAPSHOTS=()   # ordered list of snapshot hashes

red()   { printf '\033[1;31m%s\033[0m\n' "$*"; }
green() { printf '\033[1;32m%s\033[0m\n' "$*"; }
blue()  { printf '\033[1;34m%s\033[0m\n' "$*"; }

# ── assertions ──────────────────────────────────────────

pass() {
    PASS=$((PASS + 1))
    green "  PASS: $1"
}

fail() {
    FAIL=$((FAIL + 1))
    red "  FAIL: $1"
    red "        $2"
}

# ── setup ───────────────────────────────────────────────

init_project() {
    local dir="$1"
    rm -rf "$dir"
    mkdir -p "$dir"
    cd "$dir"
    earwig init > /dev/null
    SNAPSHOTS=()
    blue "---- project: $dir ----"
}

# ── file operations ─────────────────────────────────────

write_file() {
    local path="$1" content="$2"
    mkdir -p "$(dirname "$path")"
    printf '%s' "$content" > "$path"
}

append_file() {
    local path="$1" content="$2"
    printf '%s' "$content" >> "$path"
}

delete_file() {
    rm -f "$1"
}

delete_dir() {
    rm -rf "$1"
}

make_dir() {
    mkdir -p "$1"
}

# ── earwig operations ──────────────────────────────────

snapshot() {
    local output
    output=$(earwig snapshot 2>&1)
    if echo "$output" | grep -q "^Snapshot "; then
        local hash
        hash=$(echo "$output" | grep "^Snapshot " | awk '{print $2}')
        SNAPSHOTS+=("$hash")
        blue "  snapshot #${#SNAPSHOTS[@]}: $hash"
    else
        blue "  snapshot: (no changes)"
    fi
}

restore() {
    local n="$1"  # 1-indexed snapshot number
    local hash="${SNAPSHOTS[$((n - 1))]}"
    earwig restore -y "$hash" > /dev/null
    blue "  restore -> snapshot #$n ($hash)"
}

show_log() {
    earwig log
}

# ── expect: files ──────────────────────────────────────

expect_file() {
    local path="$1" expected="$2"
    if [ ! -f "$path" ]; then
        fail "$path exists" "file not found"
        return
    fi
    local actual
    actual=$(cat "$path")
    if [ "$actual" = "$expected" ]; then
        pass "$path == '$expected'"
    else
        fail "$path == '$expected'" "got '$actual'"
    fi
}

expect_no_file() {
    local path="$1"
    if [ -e "$path" ] || [ -L "$path" ]; then
        fail "$path should not exist" "file or symlink exists"
    else
        pass "$path does not exist"
    fi
}

expect_no_dir() {
    local path="$1"
    if [ -d "$path" ]; then
        fail "$path/ should not exist" "directory exists"
    else
        pass "$path/ does not exist"
    fi
}

expect_dir() {
    local path="$1"
    if [ -d "$path" ]; then
        pass "$path/ exists"
    else
        fail "$path/ exists" "directory not found"
    fi
}

# ── expect: snapshots ──────────────────────────────────

expect_snapshot_count() {
    local expected="$1"
    local actual
    actual=$(earwig log 2>/dev/null | grep -c '[*]' || true)
    if [ "$actual" -eq "$expected" ]; then
        pass "snapshot count == $expected"
    else
        fail "snapshot count == $expected" "got $actual"
    fi
}

expect_show_change() {
    local n="$1" type="$2" path="$3"
    local hash="${SNAPSHOTS[$((n - 1))]}"
    local output
    output=$(earwig show "$hash" 2>&1)
    if echo "$output" | grep -q "  $type $path"; then
        pass "snapshot #$n shows $type $path"
    else
        fail "snapshot #$n shows $type $path" "output: $output"
    fi
}

expect_log_has_branch() {
    local output
    output=$(earwig log 2>&1)
    if echo "$output" | grep -q '|/'; then
        pass "log contains branch marker"
    else
        fail "log contains branch marker" "output: $output"
    fi
}

expect_no_changes() {
    local output
    output=$(earwig snapshot 2>&1)
    if echo "$output" | grep -q "No changes"; then
        pass "no changes to snapshot"
    else
        fail "no changes to snapshot" "output: $output"
    fi
}

# ── expect: file properties ────────────────────────────

expect_file_mode() {
    local path="$1" expected="$2"
    if [ ! -e "$path" ]; then
        fail "$path mode == $expected" "file not found"
        return
    fi
    local actual
    actual=$(stat -c '%a' "$path" 2>/dev/null || stat -f '%Lp' "$path" 2>/dev/null)
    if [ "$actual" = "$expected" ]; then
        pass "$path mode == $expected"
    else
        fail "$path mode == $expected" "got $actual"
    fi
}

expect_is_symlink() {
    local path="$1"
    if [ -L "$path" ]; then
        pass "$path is a symlink"
    else
        fail "$path is a symlink" "not a symlink"
    fi
}

expect_symlink_target() {
    local path="$1" expected="$2"
    if [ ! -L "$path" ]; then
        fail "$path -> $expected" "not a symlink"
        return
    fi
    local actual
    actual=$(readlink "$path")
    if [ "$actual" = "$expected" ]; then
        pass "$path -> $expected"
    else
        fail "$path -> $expected" "got $actual"
    fi
}

expect_file_size() {
    local path="$1" expected="$2"
    if [ ! -f "$path" ]; then
        fail "$path size == $expected" "file not found"
        return
    fi
    local actual
    actual=$(wc -c < "$path" | tr -d ' ')
    if [ "$actual" = "$expected" ]; then
        pass "$path size == $expected bytes"
    else
        fail "$path size == $expected bytes" "got $actual"
    fi
}

# ── summary ─────────────────────────────────────────────

summary() {
    echo ""
    echo "================================"
    if [ "$FAIL" -eq 0 ]; then
        green "ALL PASSED: $PASS assertions"
    else
        red "FAILED: $FAIL failures, $PASS passed"
    fi
    echo "================================"
    exit $(( FAIL > 0 ? 1 : 0 ))
}
