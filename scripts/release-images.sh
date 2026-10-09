#!/usr/bin/env bash
# Build the release images (Dockerfile and Dockerfile.debug) exactly as the
# release workflow does, from the committed tree only (`git archive HEAD`, so
# untracked or ignored files cannot hide a missing COPY), then smoke-test them:
# both answer `--help`, and the release image serves its API and metrics.
# Run by `scripts/gate.sh --release`; also runnable alone. Builds the host
# architecture only (the workflow adds arm64 through QEMU).
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
rev=$(git rev-parse --short HEAD)
tag="deltaforge-release-check:$rev"
labels=()
if [ -n "${DELTAFORGE_GATE_RUN:-}" ] && [ -n "${DELTAFORGE_GATE_OWNER:-}" ]; then
  labels=(--label "deltaforge.gate.run=$DELTAFORGE_GATE_RUN" --label "deltaforge.gate.owner=$DELTAFORGE_GATE_OWNER")
fi
name="deltaforge-release-check-$$"
cleanup() {
  docker rm -f -v "$name" >/dev/null 2>&1 || true
  docker rmi -f "$tag" "$tag-debug" >/dev/null 2>&1 || true
}
trap cleanup EXIT

for spec in "Dockerfile:$tag" "Dockerfile.debug:$tag-debug"; do
  file=${spec%%:*} image=${spec#*:}
  echo "== build $file from the committed tree"
  git archive --format=tar HEAD | docker build -f "$file" -t "$image" -
  echo "== $image --help"
  docker run --rm "${labels[@]}" "$image" --help >/dev/null
done

echo "== $tag serves its API and metrics"
docker run -d --name "$name" "${labels[@]}" -p 127.0.0.1::8080 -p 127.0.0.1::9000 "$tag" \
  --storage-backend memory --api-addr 0.0.0.0:8080 --metrics-addr 0.0.0.0:9000 >/dev/null
api=$(docker port "$name" 8080/tcp | head -1)
metrics=$(docker port "$name" 9000/tcp | head -1)
for _ in $(seq 60); do
  curl -fsS "http://$api/pipelines" >/dev/null 2>&1 && break
  sleep 1
done
curl -fsS "http://$api/pipelines" | grep -q '^\[' || { docker logs "$name"; echo "API did not answer"; exit 1; }
curl -fsS "http://$metrics/metrics" | grep -q '^deltaforge_build_info' || { docker logs "$name"; echo "metrics missing"; exit 1; }
echo "release images: ok ($rev)"
