#!/usr/bin/env bash
# Build stripped release binaries into dist/ for the common platforms.
#   tools/release.sh v0.1.0-beta.1
set -euo pipefail
cd "$(dirname "$0")/.."
VERSION=${1:-$(git describe --tags --always)}
rm -rf dist && mkdir -p dist
for target in linux/amd64 linux/arm64 darwin/arm64 darwin/amd64 windows/amd64; do
  os=${target%/*}; arch=${target#*/}; out="dist/datacom-$VERSION-$os-$arch"
  [ "$os" = windows ] && out="$out.exe"
  CGO_ENABLED=0 GOOS=$os GOARCH=$arch go build -trimpath -ldflags="-s -w -X main.version=$VERSION" -o "$out" .
done
(cd dist && sha256sum * > SHA256SUMS)
ls -l dist | awk 'NR>1{printf "%-44s %6.1f MB\n", $9, $5/1048576}'
