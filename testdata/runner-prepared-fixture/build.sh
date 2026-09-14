#!/usr/bin/env bash
set -euo pipefail

runner=${1:?reviewed k8s-runner checkout required}
output=${2:?absolute output binary path required}
[[ "$runner" = /* && "$output" = /* ]] || { printf '%s\n' 'both paths must be absolute' >&2; exit 1; }
source_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
[[ -f "$runner/go.mod" && -d "$runner/internal/.gen/agynio/api/runner/v1" ]] || { printf '%s\n' 'generate the reviewed runner APIs first' >&2; exit 1; }
mkdir -p -- "$runner/tmp"
fixture=$(mktemp -d "$runner/tmp/prepared-controller.XXXXXX")
trap 'rm -rf -- "$fixture"' EXIT
cp -- "$source_dir/main.go" "$fixture/main.go"
cp -- "$runner/go.mod" "$runner/go.sum" "$fixture/"
cd -- "$runner"
go build -race -mod=mod -modfile="$fixture/go.mod" -o "$output" "$fixture/main.go"
