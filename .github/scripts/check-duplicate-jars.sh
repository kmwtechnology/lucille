#!/usr/bin/env bash
# Fails if the given modules' runtime dependencies, taken together, hold more than one version of the same
# groupId:artifactId. The core Docker image copies each module's separately resolved target/lib into one
# /lucille/lib, so a version split between the modules ships both jars.
# Classifier jars of one version (e.g. netty natives per platform) count once.
# Usage (from the repo root): check-duplicate-jars.sh <module> [<module> ...]
set -euo pipefail

modules=$(IFS=,; echo "$*")
out=target/runtime-deps.txt

# -am puts the parent poms in the reactor, so this works on a runner that has not installed them
mvn -B -q dependency:list -DincludeScope=runtime -DoutputFile="$out" -DappendOutput=false \
  -pl "$modules" -am

# Lines look like "   group:artifact:type[:classifier]:version:scope[ -- module ...]"
coords=$(
  for m in "$@"; do
    awk '$1 ~ /^[^:]+:[^:]+:[^:]+:/ {print $1}' "$m/$out"
  done | awk -F: '{print $1 ":" $2, $(NF-1)}' | sort -u
)
if [[ -z "$coords" ]]; then
  echo "No dependencies listed for: $*"
  exit 1
fi

dupes=$(echo "$coords" | awk '{print $1}' | sort | uniq -d)
if [[ -n "$dupes" ]]; then
  echo "Runtime dependencies of $* hold more than one version of:"
  for d in $dupes; do
    echo "  $d: $(echo "$coords" | awk -v d="$d" '$1 == d {print $2}' | paste -sd' ' -)"
  done
  exit 1
fi
echo "No duplicate artifacts across the runtime dependencies of: $*"
