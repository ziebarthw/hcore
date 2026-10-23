#!/bin/sh
# Refresh hcore's copy of htstring (third_party/htstring) from GitHub.
#
#   scripts/update-htstring.sh          # the latest main
#   scripts/update-htstring.sh v0.2.0   # a tag or another branch
#
# The copy is replaced as a whole and its source commit recorded in
# third_party/htstring/UPSTREAM. Review the result with `git diff`, run
# `make check`, then commit it like any other change.
set -eu

URL=https://github.com/ziebarthw/htstring
REF=${1:-main}

cd "$(dirname "$0")/.."
DEST=third_party/htstring

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

git clone --quiet --depth 1 --branch "$REF" "$URL" "$tmp/htstring"
commit=$(git -C "$tmp/htstring" rev-parse HEAD)

rm -rf "$DEST"
mkdir -p "$DEST"
git -C "$tmp/htstring" archive HEAD | tar -x -C "$DEST"
printf '%s %s (%s)\n' "$URL" "$commit" "$REF" > "$DEST/UPSTREAM"

echo "third_party/htstring is now $URL at $commit ($REF)."
echo "Next: git diff --stat, make check, commit."
