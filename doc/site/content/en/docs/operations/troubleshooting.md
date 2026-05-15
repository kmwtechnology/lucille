---
title: Troubleshooting
date: 2024-10-15
description: A guide to some common issues and their resolution.
---

If something isn't working, **read logs first**. Lucille logs are detailed and will usually identify where the problem is coming from.

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

