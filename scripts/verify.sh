#!/usr/bin/env bash
set -euo pipefail

BENCHTIME="${BENCHTIME:-100ms}"
COUNT="${COUNT:-1}"
SKIP_BENCH="${SKIP_BENCH:-0}"

export GOWORK=off
export GOTOOLCHAIN=local

formatted="$(gofmt -l .)"
if [[ -n "${formatted}" ]]; then
  printf '%s\n' "${formatted}"
  exit 1
fi

go test ./... -count="${COUNT}"
go vet ./...

if [[ "${SKIP_BENCH}" != "1" ]]; then
  go test ./... -run '^$' -bench . -benchmem -benchtime="${BENCHTIME}" -count=1
fi