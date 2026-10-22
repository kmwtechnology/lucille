#!/bin/sh
set -e

# run from the example's root so docker compose picks up docker-compose.yml
cd "$(dirname "$0")/.."

docker compose up -d --build

# The api container is long-running, so wait on verifier specifically rather than using
# `docker compose up --abort-on-container-exit`.
docker compose wait verifier || true

# verifier has already exited, so `-a` is needed for `docker compose ps` to list it.
EXIT_CODE=$(docker inspect --format='{{.State.ExitCode}}' "$(docker compose ps -aq verifier)")
echo "verifier exited with code ${EXIT_CODE}"

docker compose logs verifier
if [ "$EXIT_CODE" != "0" ]; then
  docker compose logs api
fi
docker compose down -v

exit "$EXIT_CODE"
