#!/usr/bin/env bash
# Compare goflate (Rust) against Go archive/zip deflate output byte-for-byte.
# usage: gotest/compare.sh [WORKDIR] [N] [SEED]
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
crate=$(dirname "$here")
work=${1:-$(mktemp -d)}
n=${2:-400}
seed=${3:-1}
GO=${GO:-/home/mazrean/.local/share/mise/installs/go/1.27.3/bin/go}
CARGO=${CARGO:-$HOME/.cargo/bin/cargo}

mkdir -p "$work/in" "$work/go" "$work/gosplit" "$work/rs"
rm -f "$work"/in/* "$work"/go/* "$work"/gosplit/* "$work"/rs/*
python3 "$here/gen_corpus.py" "$work/in" "$n" "$seed" >/dev/null
(cd "$here" && "$GO" build -o "$work/gotest" .)
(cd "$crate" && "$CARGO" build --release -q)
"$work/gotest" -batch "$work/in" "$work/go"
"$work/gotest" -batch -split 3 "$work/in" "$work/gosplit"
"$crate/target/release/goflate-cli" --batch "$work/in" "$work/rs"

total=0; bad=0; splitbad=0
for f in "$work"/in/*; do
  b=$(basename "$f")
  total=$((total+1))
  if ! cmp -s "$work/go/$b.go" "$work/rs/$b.rs"; then
    bad=$((bad+1)); echo "MISMATCH $b: $(cmp "$work/go/$b.go" "$work/rs/$b.rs" 2>&1 | head -1)"
  fi
  if ! cmp -s "$work/go/$b.go" "$work/gosplit/$b.go.split"; then
    splitbad=$((splitbad+1)); echo "GO SPLIT DIFFERS $b"
  fi
done
echo "compared=$total mismatches=$bad go_split_differs=$splitbad"
"$crate/target/release/goflate-cli" --bench "$work/in"
[ "$bad" -eq 0 ]
