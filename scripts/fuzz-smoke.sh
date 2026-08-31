#!/usr/bin/env bash
set -euo pipefail

fuzztime="${FUZZTIME:-10000x}"
minimizetime="${FUZZMINIMIZETIME:-1x}"
parallel="${FUZZ_PARALLEL:-1}"

packages="$(go list ./...)"

while IFS= read -r package; do
	targets="$(go test "$package" -list '^Fuzz')"
	while IFS= read -r target; do
		case "$target" in
		Fuzz*)
			printf '== %s %s ==\n' "$package" "$target"
			go test "$package" -run '^$' -fuzz "^${target}$" \
				-fuzztime "$fuzztime" -fuzzminimizetime "$minimizetime" \
				-parallel "$parallel"
			;;
		esac
	done <<<"$targets"
done <<<"$packages"
