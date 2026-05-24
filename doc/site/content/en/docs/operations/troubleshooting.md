---
title: Troubleshooting
weight: 9
date: 2024-10-15
description: A guide to some common issues and their resolution.
---

If something isn't working, **read logs first**. Lucille logs are detailed and will usually identify where the problem is coming from. See [Log Analysis]({{< relref "docs/operations/log-analysis" >}}) for a guide to reading Lucille logs.

## Debugging Failed Documents

The run summary reports how many documents failed, but not which ones or why. Here's the workflow for investigating failures.

### Step 1: Find the failed document IDs and reasons

Search the logs for failure messages. These are logged at ERROR level by default:

```bash
# Pipeline failures (a stage threw StageException)
grep "Error processing document:" lucille.log

# Indexer failures (search backend rejected the document)
grep "Sent failure message for doc" lucille.log
```

Each pipeline failure is followed by a stack trace identifying the stage and exception. Each indexer failure includes the backend's rejection reason. See [Which documents failed?]({{< relref "docs/operations/log-analysis#which-documents-failed" >}}) for more detail.

### Step 2: Understand the failure reason

Common causes:

| Failure type | Typical reasons |
|---|---|
| Pipeline (`StageException`) | Required field missing, external service unavailable, malformed data the stage can't parse |
| Indexer (backend rejection) | Field type mismatch with index mapping, document too large, version conflict |
| Poison pill (distributed mode) | Document causes a Worker crash repeatedly — often an out-of-memory condition or a bug in a stage |

If the reason is clear from the error message (e.g., "field X is missing"), fix the pipeline config or source data and re-run.

### Step 3: Inspect the document's contents

If you need to see what fields and values the document had at the point of failure:

**Add a `Print` stage before the failing stage.** `Print` logs document contents to a file or stdout. Use conditions to limit output to the specific document:

```hocon
{
  class: "com.kmwllc.lucille.stage.Print"
  conditions: [{ fields: ["id"], values: ["the-failing-doc-id"] }]
}
```

**Re-run with a CSV indexer.** Swap your indexer to CSV and re-run. Documents that would have been rejected by the real backend are written to the CSV file where you can inspect their fields. This helps with indexer rejections but not pipeline failures (the document never reaches the indexer if a stage throws).

```hocon
indexer { type: "CSV" }
csv { path: "./debug-output.csv", columns: ["id", "title", "problematic_field"] }
```

**Re-run in test mode (Java).** Use `Runner.runInTestMode()` with the same config and source data. After the run, inspect documents programmatically:

```java
RunResult result = Runner.runInTestMode(config);
List<Document> docs = result.getDocsSentForIndexing("my-connector");
Document problem = docs.stream()
    .filter(d -> d.getId().equals("the-failing-doc-id"))
    .findFirst().orElse(null);
```

**Consume the Kafka fail topic (distributed mode).** Documents that exceeded `worker.maxRetries` are sent to the `{pipeline}_fail` topic with their full serialized content. Consume the topic to inspect the document.

### Step 4: Fix and verify

Once you understand the cause:

- **Missing or malformed field** — add a stage earlier in the pipeline to clean or populate the field, or add conditions so the failing stage skips documents without the required data.
- **Index mapping mismatch** — update the index mapping to accept the field type, or add a stage to convert the field to the expected type.
- **Stage bug** — fix the stage code and add a test case for the problematic input.

Re-run with the fix and confirm the failure count drops to zero in the run summary.

## Build & Java Environment Issues

### Java Command Not Found / Wrong Version

**Symptoms:**

* java: command not found.
* Lucille fails with version errors.

**Fix:**

* Ensure Java 17+ is installed and on `PATH`.
* Ensure `JAVA_HOME` points to a JDK (not a JRE).

```bash
java -version
echo $JAVA_HOME
```

Example output:

```bash
java -version

java version "22.0.1" 2024-04-16
Java(TM) SE Runtime Environment (build 22.0.1+8-16)
Java HotSpot(TM) 64-Bit Server VM (build 22.0.1+8-16, mixed mode, sharing)

echo $JAVA_HOME

/Library/Java/JavaVirtualMachines/jdk-22.jdk/Contents/Home
```

If unset, refer to the [installation guide]({{< relref "docs/getting-started/installation" >}}).

## Configuration Issues

### Missing or Misspelled Keys

**Symptoms:**

* Configuration does not contain key "name".
* Stage/Indexer throws for missing required property.

**Fix:**

* Compare with component `SPEC` docs and ensure required fields exist and are correctly cased.

### Invalid Types or List/Map Shapes

**Symptoms:**

* Expected NUMBER got STRING.

**Fix:**

* Match the `SPEC` type exactly and convert scalars to lists/maps when required.

## Kafka / Messaging

### Cannot Connect to Kafka

**Symptoms:**

* Timed out waiting for connection from Kafka.
* Connection to Kafka could not be established.

**Fix:**

* Verify your `kafka.bootstrapServers` in your config.
* Check Kafka connectivity to ensure it is reachable.

## Solr / Elasticsearch / OpenSearch Connectivity

### Cannot Connect to Solr / Elasticsearch / OpenSearch

**Symptoms:**

* Failed to talk to the search cluster.
* Connection refused or host not found.

**Fix:**

* Ensure that the URL/port in your config is correct.
* Ensure that the service is reachable from the machine running Lucille.

### Certificate / Authorization Problems

**Symptoms:**

* Handshake failure, certificate not trusted, or unauthorized.

**Fix:**

* Test connection with auth/TLS disabled for debugging.
* Verify that credentials are correct.

### Schema / Mapping mismatches

**Symptoms:**

* Bulk index partially fails and some documents are rejected.
* Mapping or parsing exceptions.

**Fix:**

* Read the exact field and reason in the error details and fix that field in your pipeline or mapping.

## Timeouts

Lucille has a default timeout specified in `runner.connectorTimeout` of 24 hours. You can override this timeout in the configuration file. This may be necessary in the case of very large or long-running jobs that may take more than 24 hours to complete.

