#!/bin/sh
set -e

docker compose up -d --build

# Block until the verifier container exits. Runner, Worker, and Indexer are long-running
# or exit on their own as part of a normal successful run, so `docker compose up
# --abort-on-container-exit` would race on whichever of them exits first and could tear
# down the stack before verification even runs. Waiting on verifier specifically avoids that.
docker compose wait verifier || true

# `docker compose ps -q` only lists running containers by default, but verifier has already
# exited by this point (that's what we just waited on), so we need `-a` to see it. Without it,
# `docker compose ps -aq verifier` returns empty and `docker inspect` fails with
# "invalid container name or ID: value is empty", aborting the script under `set -e`.
EXIT_CODE=$(docker inspect --format='{{.State.ExitCode}}' "$(docker compose ps -aq verifier)")
echo "verifier exited with code ${EXIT_CODE}"

docker compose logs verifier
docker compose down

exit "$EXIT_CODE"
