#!/usr/bin/env bash
# Link a DeltaForge revision once per predeclared seed, each link with its
# functions in a seeded order (LLD `--shuffle-sections`), for
# layout-randomized qualification:
#
#   crates/perf-harness/layout-links.sh <git-rev> <out-dir>
#   canonical-perf --layout base=<out-dir-a> --layout new=<out-dir-b> ...
#
# The seeds are fixed here, before any result is seen, and every one is
# used: a qualification never picks or drops a link by its result. Each
# revision builds in a worktree and target directory of its own under
# <out-dir> (a target directory is never shared between trees). Writes
# <out-dir>/runner-s<seed> and <out-dir>/manifest.json (the revision, and
# every link's seed, file and SHA-256). Remove the worktree afterwards with
# `git worktree remove <out-dir>/src`.
set -euo pipefail

SEEDS=(101 202 303 404 505)

[ $# -eq 2 ] || { echo "usage: $0 <git-rev> <out-dir>" >&2; exit 2; }
rev=$1
mkdir -p "$2"
out=$(cd "$2" && pwd)
root=$(git rev-parse --show-toplevel)
full=$(git -C "$root" rev-parse --verify "$rev^{commit}")

src="$out/src"
[ -e "$src" ] || git -C "$root" worktree add --detach "$src" "$full" >/dev/null
[ "$(git -C "$src" rev-parse HEAD)" = "$full" ] || {
    echo "$src is not at $full" >&2
    exit 1
}
[ -z "$(git -C "$src" status --porcelain)" ] || {
    echo "$src is not clean" >&2
    exit 1
}

export CARGO_INCREMENTAL=0 CARGO_TARGET_DIR="$out/target"
links=()
for seed in "${SEEDS[@]}"; do
    (cd "$src" && cargo rustc --release -p runner --bin runner -- \
        -C "link-arg=-Wl,--shuffle-sections=*=$seed") \
        >"$out/build-$seed.log" 2>&1 || {
        echo "seed $seed: build failed, see $out/build-$seed.log" >&2
        exit 1
    }
    readelf -p .comment "$CARGO_TARGET_DIR/release/runner" | grep -q LLD || {
        echo "seed $seed: not linked by LLD (no seeded layout)" >&2
        exit 1
    }
    cp "$CARGO_TARGET_DIR/release/runner" "$out/runner-s$seed"
    hash=$(sha256sum "$out/runner-s$seed" | cut -d' ' -f1)
    links+=("{\"seed\": $seed, \"path\": \"runner-s$seed\", \"sha256\": \"$hash\"}")
    echo "seed $seed: $hash"
done
printf '{"rev": "%s", "links": [%s]}\n' "$full" \
    "$(IFS=,; echo "${links[*]}")" >"$out/manifest.json"
echo "$out/manifest.json"
