#!/usr/bin/env bash
# Retain one completed series: write its SHA-256 manifest, copy it to the
# durable location outside the repository and the build directory, and
# verify the copy against the manifest. Run after each completed series and
# before removing any of its resources.
#
#   retain.sh <series-dir>      (copies to $HOME/deltaforge-evidence/<name>)
set -euo pipefail
SRC=$(cd "$1" && pwd)
NAME=$(basename "$SRC")
DEST="${DELTAFORGE_EVIDENCE:-$HOME/deltaforge-evidence}/$NAME"
cd "$SRC"
find . -type f ! -name MANIFEST.sha256 -print0 | sort -z | xargs -0 sha256sum > MANIFEST.sha256
mkdir -p "$DEST"
cp -a "$SRC/." "$DEST/"
( cd "$DEST" && sha256sum --quiet -c MANIFEST.sha256 )
echo "retained $NAME: $(wc -l < MANIFEST.sha256) files verified at $DEST"
echo "manifest sha256: $(sha256sum MANIFEST.sha256 | cut -d' ' -f1)"
