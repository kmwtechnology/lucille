# Multi-Broker Kafka Smoke Test

Runs Lucille in external mode (`-external`) against a three-broker Kafka cluster, and checks that the run survives a
broker loss without losing documents. Replication is set so that writes need two in-sync replicas. The unit tests use
embedded single-broker Kafka, so they can't show that replication and `min.insync.replicas` behave as intended.

## What it runs

`scripts/run-nightly.sh` runs two scenarios. Each starts from an empty cluster, and the verifier runs before the cluster
is torn down.

| Scenario | What happens | What the verifier checks |
|---|---|---|
| 1. all brokers up | a full run over `data/source.csv` (5000 documents) | every input id is in the output, and nothing else is |
| 2. broker loss | the same run, with broker 3 stopped 15 seconds in | the same checks as scenario 1 |

The verifier also checks the event topic that Lucille created for the run. It must have three replicas and
`min.insync.replicas=2`.

The script refuses to pass scenario 2 if the run had already finished when broker 3 was stopped, since then the stop
would have tested nothing.

## The cluster

`docker-compose.yml` runs three KRaft brokers, each acting as both broker and controller. Their settings:

- Replication factor 3 by default, with `min.insync.replicas=2`, including for the offsets and transaction topics.
- Two listeners per broker. `INTERNAL` (`kafka-N:9092`) is for broker-to-broker traffic and for commands run inside the
  containers. `EXTERNAL` (`localhost:1909N`) is what Lucille uses, since Lucille runs on the host.

The script creates the three pipeline topics (`pipeline1_source`, `pipeline1_dest`, `pipeline1_fail`) with replication
factor 3 and `min.insync.replicas=2`. Lucille creates its own event topic (`pipeline1_event_<runId>`) with the settings
in `conf/kafka-multibroker.conf`:

- `kafka.eventTopicReplicationFactor: 3`
- `kafka.eventTopicMinInsyncReplicas: 2`
- `kafka.producer.acks: all`

Without the first two settings, Lucille creates the event topic with one replica. The verifier fails on that.

## Running it

Requirements: Docker with Compose v2, Java 21, and Maven. Ports 19092 to 19094 must be free on the host.

```
mvn -pl lucille-examples/lucille-kafka-multibroker-smoketest-example -am package -DskipTests
./lucille-examples/lucille-kafka-multibroker-smoketest-example/scripts/run-nightly.sh
```

Or, from the example directory, `mvn verify -Pnightly`, which runs the same script in the `verify` phase.

The script exits non-zero if any scenario or verifier fails. It always stops the cluster (`docker compose down -v`).

## Delivery guarantee

Delivery is at-least-once, so the output may contain duplicate rows. A consumer that has to restart after a broker loss
can re-send documents it had not yet committed. The verifier allows duplicates and fails only on missing or unexpected
ids. The output of the runs so far has had no duplicates.

## Not covered

- A second broker loss, which should make producers fail rather than lose data. That needs a stable way to observe the
  failure, and it is not part of this smoke test.
