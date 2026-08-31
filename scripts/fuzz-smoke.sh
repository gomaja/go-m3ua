#!/usr/bin/env bash
set -euo pipefail

fuzztime="${FUZZTIME:-2s}"
parallel="${FUZZ_PARALLEL:-1}"

while IFS= read -r package; do
	while IFS= read -r target; do
		[ -n "$target" ] || continue
		printf '== %s %s ==\n' "$package" "$target"
		go test "$package" -run '^$' -fuzz "^${target}$" \
			-fuzztime "$fuzztime" -parallel "$parallel"
	done < <(go test "$package" -list '^Fuzz' | sed -n '/^Fuzz/p')
done < <(go list ./...)
