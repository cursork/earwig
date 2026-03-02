#!/bin/bash
# Verify earwig Go files with Gobra annotations.
# Run inside Docker: docker build -t earwig-gobra -f test/gobra/Dockerfile . && docker run --rm earwig-gobra test/gobra/verify.sh
set -e

STUBS="/earwig/test/gobra/stubs"
PASS=0
FAIL=0
ERRORS=""

# Collect all Go files that have Gobra annotations (// @)
FILES=$(grep -rl '// @' /earwig/internal/ --include='*.go' 2>/dev/null || true)

if [ -z "$FILES" ]; then
    echo "No annotated Go files found."
    exit 0
fi

for f in $FILES; do
    name=${f#/earwig/}
    echo "=== Verifying: $name ==="
    if java -Xss128m -jar /gobra/gobra.jar \
        -I "$STUBS" \
        -i "$f" 2>&1; then
        echo "--- PASS: $name ---"
        PASS=$((PASS + 1))
    else
        echo "--- FAIL: $name ---"
        FAIL=$((FAIL + 1))
        ERRORS="$ERRORS  $name\n"
    fi
    echo
done

echo "================================"
echo "Results: $PASS passed, $FAIL failed"
if [ "$FAIL" -gt 0 ]; then
    printf "Failures:\n$ERRORS"
    exit 1
fi
