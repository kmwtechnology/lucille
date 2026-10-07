---
title: "Security Configuration"
weight: 6
date: 2025-06-09
description: >
  Configuring TLS, authentication, and credentials for search backends and Kafka in Lucille deployments.
---

Lucille connects to external systems — search backends (Solr, OpenSearch, Elasticsearch, Pinecone) and Kafka — that may require TLS encryption and authentication. This page covers how to configure security for each.

## General Principles

**Never hard-code credentials in config files.** Use HOCON's environment variable substitution to inject secrets at runtime:

```hocon
opensearch {
  url: "https://localhost:9200"
  url: ${?OPENSEARCH_URL}  # override from env var
}
```

In Kubernetes, inject credentials via Secrets mounted as environment variables. In Docker, use `--env` or `--env-file`. The config file can live in version control with defaults; secrets come from the environment.

**Use `acceptInvalidCert: false` in production.** The `acceptInvalidCert` option exists for development against localhost with self-signed certificates. In production, always use valid certificates and leave this disabled.

---

## OpenSearch / Elasticsearch Security

### URL-Based Authentication

The simplest approach: embed credentials in the URL:

```hocon
opensearch {
  url: "https://admin:password@opensearch-host:9200"
  url: ${?OPENSEARCH_URL}
  index: "my-index"
}
```

Lucille's `OpenSearchUtils` parses the `userInfo` from the URL and configures HTTP Basic authentication automatically. The username and password are extracted and applied to all requests.

### TLS Configuration

OpenSearch connections use TLS when the URL scheme is `https://`. Lucille builds an SSL context and TLS strategy for the HTTP client automatically.

**For valid certificates (production):**

```hocon
opensearch {
  url: "https://opensearch-host:9200"
  index: "my-index"
  acceptInvalidCert: false  # default — validates certificates normally
}
```

No additional configuration needed if the server's certificate is signed by a CA in the JVM's default trust store.

**For self-signed certificates (development only):**

```hocon
opensearch {
  url: "https://localhost:9200"
  index: "my-index"
  acceptInvalidCert: true  # disables certificate validation and hostname verification
}
```

When `acceptInvalidCert: true`, Lucille:
- Trusts all certificates (including self-signed)
- Disables hostname verification (`NoopHostnameVerifier`)

**Warning:** Never use `acceptInvalidCert: true` in production. It disables all TLS security guarantees.

### Custom Trust Store

If your OpenSearch cluster uses certificates signed by an internal CA not in the JVM's default trust store, provide a custom trust store via JVM system properties:

```bash
java \
  -Djavax.net.ssl.trustStore=/path/to/truststore.jks \
  -Djavax.net.ssl.trustStorePassword=changeit \
  -Dconfig.file=config.conf \
  -cp 'target/lib/*' com.kmwllc.lucille.core.Runner
```

Or configure via HOCON (Lucille sets these as system properties if not already set):

```hocon
javax.net.ssl.trustStore: "/path/to/truststore.jks"
javax.net.ssl.trustStore: ${?TRUSTSTORE_PATH}
javax.net.ssl.trustStorePassword: "changeit"
javax.net.ssl.trustStorePassword: ${?TRUSTSTORE_PASSWORD}
```

---

## Solr Security

### HTTP Basic Authentication

```hocon
solr {
  url: "https://solr-host:8983/solr/my-collection"
  userName: "solr-user"
  userName: ${?SOLR_USER}
  password: "solr-password"
  password: ${?SOLR_PASSWORD}
}
```

### TLS with Custom Keystore/Truststore

For Solr clusters with mutual TLS (mTLS) or custom CA certificates:

```hocon
solr {
  url: "https://solr-host:8983/solr/my-collection"
  userName: ${?SOLR_USER}
  password: ${?SOLR_PASSWORD}
  acceptInvalidCert: false
}

# SSL system properties — Lucille sets these if not already set via -D flags
javax.net.ssl.keyStore: "/path/to/keystore.jks"
javax.net.ssl.keyStore: ${?KEYSTORE_PATH}
javax.net.ssl.keyStorePassword: "changeit"
javax.net.ssl.keyStorePassword: ${?KEYSTORE_PASSWORD}
javax.net.ssl.trustStore: "/path/to/truststore.jks"
javax.net.ssl.trustStore: ${?TRUSTSTORE_PATH}
javax.net.ssl.trustStorePassword: "changeit"
javax.net.ssl.trustStorePassword: ${?TRUSTSTORE_PASSWORD}
```

Lucille's `SSLUtils.setSSLSystemProperties(config)` reads these from the config and sets them as JVM system properties (without overriding properties already set via `-D` flags). This means you can provide them either in the config file or on the command line.

### SolrCloud with ZooKeeper

For SolrCloud deployments, the Solr client connects to ZooKeeper for cluster state. If ZooKeeper requires authentication, configure it separately (ZooKeeper auth is not managed by Lucille's config — use ZooKeeper's own client configuration mechanisms).

```hocon
solr {
  useCloudClient: true
  zkHosts: ["zk1:2181", "zk2:2181", "zk3:2181"]
  zkChroot: "/solr"
  defaultCollection: "my-collection"
  userName: ${?SOLR_USER}
  password: ${?SOLR_PASSWORD}
}
```

### Development: Accepting Invalid Certificates

```hocon
solr {
  url: "https://localhost:8983/solr/my-collection"
  acceptInvalidCert: true  # development only
}
```

---

## Kafka Security

### Security Protocol

Kafka supports four security protocols. Set the protocol in the `kafka` config block:

```hocon
kafka {
  bootstrapServers: "kafka-host:9093"
  securityProtocol: "SSL"  # or PLAINTEXT, SASL_PLAINTEXT, SASL_SSL
}
```

| Protocol | Encryption | Authentication |
|---|---|---|
| `PLAINTEXT` | None | None |
| `SSL` | TLS | TLS client certificates (optional) |
| `SASL_PLAINTEXT` | None | SASL (username/password, Kerberos, etc.) |
| `SASL_SSL` | TLS | SASL over TLS |

### Using External Property Files

For complex Kafka security configurations (SASL mechanisms, Kerberos, custom SSL settings), use external property files:

```hocon
kafka {
  bootstrapServers: "kafka-host:9093"
  securityProtocol: "SASL_SSL"
  consumerPropertyFile: "/path/to/consumer.properties"
  producerPropertyFile: "/path/to/producer.properties"
  adminPropertyFile: "/path/to/admin.properties"
}
```

When a property file is specified, it **completely replaces** the programmatic Kafka client configuration (except for `CLIENT_ID_CONFIG` which is always set by Lucille). This gives you full control over every Kafka client property.

**Example `consumer.properties` for SASL_SSL with SCRAM-SHA-256:**

```properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="kafka-user" \
  password="kafka-password";
ssl.truststore.location=/path/to/truststore.jks
ssl.truststore.password=changeit
ssl.endpoint.identification.algorithm=https
```

**Example `consumer.properties` for SSL with client certificates (mTLS):**

```properties
security.protocol=SSL
ssl.truststore.location=/path/to/truststore.jks
ssl.truststore.password=changeit
ssl.keystore.location=/path/to/keystore.jks
ssl.keystore.password=changeit
ssl.key.password=changeit
ssl.endpoint.identification.algorithm=https
```

### Property File Loading

Lucille loads property files via `FileContentFetcher`, which supports:
- Local filesystem paths (`/path/to/file.properties`)
- S3 paths (`s3://bucket/path/to/file.properties`)
- Azure Blob paths
- GCS paths

This means you can store Kafka property files in cloud storage and reference them from the config — useful for centralized secret management.

### Minimal SSL Configuration (Without Property Files)

For simple SSL setups where you only need to set the security protocol:

```hocon
kafka {
  bootstrapServers: "kafka-host:9093"
  securityProtocol: "SSL"
  consumerGroupId: "lucille_workers"
  maxPollIntervalSecs: 600
  maxRequestSize: 250000000
}
```

If the Kafka broker's certificate is signed by a CA in the JVM's default trust store, this is sufficient. For custom CAs, provide the trust store via JVM system properties:

```bash
java \
  -Djavax.net.ssl.trustStore=/path/to/kafka-truststore.jks \
  -Djavax.net.ssl.trustStorePassword=changeit \
  -Dconfig.file=config.conf \
  -cp 'target/lib/*' com.kmwllc.lucille.core.Runner
```

---

## Pinecone Security

Pinecone uses API key authentication:

```hocon
pinecone {
  apiKey: ${PINECONE_API_KEY}
  environment: "us-east-1-aws"
  index: "my-index"
}
```

All Pinecone communication uses HTTPS by default. No additional TLS configuration is needed.

---

## Weaviate Security

Weaviate supports API key and OIDC authentication:

```hocon
weaviate {
  scheme: "https"
  host: "weaviate-host:8080"
  apiKey: ${WEAVIATE_API_KEY}
  className: "MyClass"
}
```

---

## Credential Management Best Practices

### Environment Variable Pattern

The standard pattern for all credentials in Lucille configs:

```hocon
# Default for development (or omit entirely)
opensearch.url: "http://localhost:9200"
# Override from environment in production
opensearch.url: ${?OPENSEARCH_URL}
```

### Kubernetes Secrets

Mount secrets as environment variables in your pod spec:

```yaml
env:
- name: OPENSEARCH_URL
  valueFrom:
    secretKeyRef:
      name: opensearch-credentials
      key: url
- name: KEYSTORE_PASSWORD
  valueFrom:
    secretKeyRef:
      name: tls-credentials
      key: keystore-password
```

### Docker

Pass credentials via `--env` or `--env-file`:

```bash
docker run \
  --env OPENSEARCH_URL=https://admin:secret@opensearch:9200 \
  --env PINECONE_API_KEY=pk-abc123 \
  -it lucille-image
```

### What NOT to Do

- ❌ Hard-code passwords in config files committed to version control
- ❌ Use `acceptInvalidCert: true` in production
- ❌ Store API keys in plain text in Docker images
- ❌ Use the same credentials for development and production
- ❌ Log credentials (Lucille does not log config values, but custom Connectors/Stages might)

---

## Summary of Security Options by Backend

| Backend | Authentication | TLS | Config Location |
|---|---|---|---|
| OpenSearch | URL userinfo (Basic) | `https://` scheme + `acceptInvalidCert` | `opensearch {}` block |
| Elasticsearch | URL userinfo (Basic) | `https://` scheme + `acceptInvalidCert` | `elastic {}` block |
| Solr | `userName` + `password` | `https://` + keystore/truststore | `solr {}` block |
| Kafka | `securityProtocol` + property files | SSL/SASL_SSL | `kafka {}` block |
| Pinecone | `apiKey` | Always HTTPS | `pinecone {}` block |
| Weaviate | `apiKey` | `scheme: "https"` | `weaviate {}` block |
| ZooKeeper | External config | External config | Not managed by Lucille |
