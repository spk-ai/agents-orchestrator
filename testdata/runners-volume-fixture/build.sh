#!/usr/bin/env bash
set -euo pipefail

registry=${1:?reviewed Runners checkout required}
output=${2:?absolute output binary path required}
[[ "$registry" = /* && "$output" = /* ]] || { printf '%s\n' 'both paths must be absolute' >&2; exit 1; }
source_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
[[ -f "$registry/go.mod" && -d "$registry/.gen/go/agynio/api/runners/v1" ]] || { printf '%s\n' 'generate the reviewed registry APIs first' >&2; exit 1; }
mkdir -p -- "$registry/tmp"
fixture=$(mktemp -d "$registry/tmp/checked-controller.XXXXXX")
trap 'rm -rf -- "$fixture"' EXIT
cp -- "$source_dir/main.go" "$fixture/main.go"
cp -- "$registry/go.mod" "$registry/go.sum" "$fixture/"
cd -- "$registry"
go build -race -mod=mod -modfile="$fixture/go.mod" -o "$output" "$fixture/main.go"
