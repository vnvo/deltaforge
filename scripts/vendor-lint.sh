#!/usr/bin/env bash
# Lint a `warn` vendored crate (scripts/gate.sh): run in the crate's copy,
# with CARGO_TARGET_DIR set.
#
#   scripts/vendor-lint.sh <findings-file> [patched-file...]
#
# rustfmt and clippy run over the whole crate. Fails (exit 1) on a clippy
# error, or on any rustfmt diff or clippy warning in a patched file (a file
# changed by a DeltaForge patch). Findings in untouched upstream files are
# printed and summarized in <findings-file>, but do not fail.
set -uo pipefail
findings=$1
shift
patched=" $* "
fail=0

fmt_files=$(cargo fmt --check 2>&1 | sed -n "s|^Diff in $PWD/\(.*\):[0-9]*:$|\1|p" | sort -u)

clippy_json=$(mktemp)
trap 'rm -f "$clippy_json"' EXIT
cargo clippy --all-targets --message-format=json > "$clippy_json"
clippy_rc=$?
# file:line level code, one per distinct finding.
clippy=$(jq -r 'select(.reason == "compiler-message") | .message
  | select(.level == "warning" or .level == "error")
  | ((.spans[] | select(.is_primary) | "\(.file_name):\(.line_start)") // "-:0")
    + " " + .level + " " + (.code.code // "-")' "$clippy_json" | sort -u)
if [ "$clippy_rc" -ne 0 ]; then
  echo "clippy failed (exit $clippy_rc)"
  fail=1
fi

upstream_fmt=()
for f in $fmt_files; do
  if [[ $patched == *" $f "* ]]; then
    echo "patched file not formatted: $f"
    fail=1
  else
    upstream_fmt+=("$f")
  fi
done

upstream_clippy=()
while read -r at level code; do
  [ -n "$at" ] || continue
  f=${at%%:*}
  if [ "$level" = error ]; then
    echo "clippy error: $at $code"
    fail=1
  elif [[ $patched == *" $f "* ]]; then
    echo "clippy finding in patched file: $at $code"
    fail=1
  else
    upstream_clippy+=("$f")
  fi
done <<<"$clippy"

{
  printf 'fmt %d file(s)' "${#upstream_fmt[@]}"
  [ "${#upstream_fmt[@]}" -eq 0 ] || printf ' [%s]' "${upstream_fmt[*]}"
  printf '; clippy %d finding(s)' "${#upstream_clippy[@]}"
  if [ "${#upstream_clippy[@]}" -gt 0 ]; then
    printf ' [%s]' "$(printf '%s\n' "${upstream_clippy[@]}" | sort | uniq -c |
      awk '{printf "%s%s (%d)", (NR > 1 ? ", " : ""), $2, $1}')"
  fi
  echo
} > "$findings"
echo "upstream findings (not failing): $(cat "$findings")"
printf '%s\n' "$clippy" | grep -v "^$" | sed 's/^/  /'
exit $fail
