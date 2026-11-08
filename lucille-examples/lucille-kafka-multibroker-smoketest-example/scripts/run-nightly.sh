#!/bin/sh
set -e

# run from the example's root, so the compose file, data/, conf/ and target/ resolve
cd "$(dirname "$0")/.."

export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092,localhost:19093,localhost:19094}"
INPUT=data/source.csv

# Seconds to let the run get going before stopping a broker. A run over the 5000-document input takes about a minute,
# so the stop lands mid-run.
STOP_AFTER_SECS=15

# stop the cluster however the script exits, including an unexpected failure under set -e
trap 'docker compose down -v' EXIT

# Starts a fresh cluster, and creates the source, dest and fail topics the way an operator would: replicated 3 ways,
# with min.insync.replicas=2. Lucille creates its own event topic, so it isn't listed here.
start_cluster() {
  # a scenario must start from an empty cluster: leftover topics or consumer offsets would change what it proves
  docker compose down -v
  docker compose up -d --wait kafka-1 kafka-2 kafka-3
  for topic in pipeline1_source pipeline1_dest pipeline1_fail; do
    docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 \
      --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 3 \
      --config min.insync.replicas=2 >/dev/null
  done
}

# One -external run: workers and indexer in the Runner's JVM, communicating through Kafka.
run_lucille() {
  INPUT_CSV="$INPUT" OUTPUT_CSV="$1" java -Dconfig.file=conf/kafka-multibroker.conf \
    -cp 'target/lib/*' com.kmwllc.lucille.core.Runner -external
}

verify() {
  INPUT_CSV="$INPUT" OUTPUT_CSV="$1" java -cp 'target/lib/*:target/test-classes' \
    org.junit.runner.JUnitCore com.kmwllc.lucille.examples.kafkamultibroker.VerifyKafkaRun
}

mkdir -p output

echo "== scenario 1: all three brokers up"
start_cluster
rm -f output/all-brokers.csv
run_lucille output/all-brokers.csv
verify output/all-brokers.csv
docker compose down -v

echo "== scenario 2: broker 3 stops during the run"
start_cluster
rm -f output/broker-loss.csv
run_lucille output/broker-loss.csv &
run_pid=$!
sleep "$STOP_AFTER_SECS"
if ! kill -0 "$run_pid" 2>/dev/null; then
  echo "the run finished before broker 3 was stopped, so this scenario proved nothing"
  exit 1
fi
docker compose stop kafka-3
wait "$run_pid"
verify output/broker-loss.csv

echo "Multi-broker Kafka smoke test passed"
