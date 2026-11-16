---
title: Kubernetes
weight: 6
date: 2025-06-09
description: Deploying Lucille on Kubernetes as CronJobs and scalable pod deployments.
---

## Kubernetes Deployment

Lucille's architecture maps naturally onto Kubernetes primitives.

### Batch Jobs as Kubernetes CronJobs

For scheduled batch ingests, run Lucille's published Docker image as a `CronJob`. When the run completes, the container exits — there is no long-running process to manage between runs.

Use `kmwtechnology/lucille-all`, which includes every plugin. The smaller `kmwtechnology/lucille` image holds only `lucille-core` and logging, so it has no Solr, OpenSearch or Elasticsearch indexer. The image's entrypoint is configured through environment variables:

| Variable | Purpose |
|---|---|
| `LUCILLE_CONF` | Path to the Lucille config file inside the container. Required. |
| `LUCILLE_ROLE` | Component to run: `runner` (default), `worker`, `indexer` or `worker-indexer`. |
| `LUCILLE_PIPELINE` | Pipeline name. Required when `LUCILLE_ROLE` is `worker`, `indexer` or `worker-indexer`. |
| `LUCILLE_OPTS` | Runner flags, such as `-distributed`. Used only when `LUCILLE_ROLE` is `runner`. |
| `JAVA_OPTS` | JVM flags, such as `-Xmx4g`. |

Mount your config somewhere other than `/lucille/conf`, such as `/mnt/conf`. `/lucille/conf` holds the image's `log4j2.xml`, and a volume mounted over it hides that file.

Run it with Docker:

```bash
docker run -v "$(pwd)/conf:/mnt/conf" \
           --env LUCILLE_CONF=/mnt/conf/my-pipeline.conf \
           --env JAVA_OPTS=-Xmx4g \
           --env OPENSEARCH_URL=https://opensearch:9200 \
           kmwtechnology/lucille-all:VERSION.0
```

The same image can run any Lucille component by setting `LUCILLE_ROLE`:

```bash
# Run as a Worker (distributed mode)
docker run -v "$(pwd)/conf:/mnt/conf" \
           --env LUCILLE_CONF=/mnt/conf/my-pipeline.conf \
           --env LUCILLE_ROLE=worker --env LUCILLE_PIPELINE=my-pipeline \
           --env JAVA_OPTS=-Xmx2g \
           kmwtechnology/lucille-all:VERSION.0

# Run as an Indexer (distributed mode)
docker run -v "$(pwd)/conf:/mnt/conf" \
           --env LUCILLE_CONF=/mnt/conf/my-pipeline.conf \
           --env LUCILLE_ROLE=indexer --env LUCILLE_PIPELINE=my-pipeline \
           --env JAVA_OPTS=-Xmx1g \
           kmwtechnology/lucille-all:VERSION.0
```

**Custom image (optional):** to ship the config inside the image instead of mounting it, build on the published image:

```dockerfile
FROM kmwtechnology/lucille-all:VERSION.0
COPY conf/my-pipeline.conf /lucille/conf/my-pipeline.conf
ENV LUCILLE_CONF=/lucille/conf/my-pipeline.conf
```

**CronJob:** the manifests below read the config from a ConfigMap, created with `kubectl create configmap lucille-config --from-file=conf/my-pipeline.conf`.
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: lucille-nightly-ingest
spec:
  schedule: "0 2 * * *"  # 2am daily
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          containers:
          - name: lucille
            image: kmwtechnology/lucille-all:VERSION.0
            env:
            - name: LUCILLE_CONF
              value: /mnt/conf/my-pipeline.conf
            - name: JAVA_OPTS
              value: "-Xmx3g"
            - name: OPENSEARCH_URL
              valueFrom:
                secretKeyRef:
                  name: opensearch-credentials
                  key: url
            volumeMounts:
            - name: lucille-config
              mountPath: /mnt/conf
            resources:
              requests:
                memory: "2Gi"
                cpu: "1"
              limits:
                memory: "4Gi"
                cpu: "4"
          volumes:
          - name: lucille-config
            configMap:
              name: lucille-config
```

> **Exit code behavior:** The Runner exits 0 on success (including runs with individual document failures) and exits 1 only for infrastructure-level failures such as connector exceptions, indexer connection failures, or timeouts. This exit code drives the Kubernetes container restart policy (`restartPolicy`) and Job retry behavior (`backoffLimit`). See [Exit Codes]({{< relref "production-operations#exit-codes" >}}) for the full list of conditions.

### Distributed Deployment as Kubernetes Pods

In distributed mode, each Lucille component runs as its own pod:

**Worker Deployment:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: lucille-workers
spec:
  replicas: 4  # Scale by changing replicas
  selector:
    matchLabels:
      app: lucille-worker
  template:
    metadata:
      labels:
        app: lucille-worker
    spec:
      containers:
      - name: worker
        image: kmwtechnology/lucille-all:VERSION.0
        env:
        - name: LUCILLE_CONF
          value: /mnt/conf/my-pipeline.conf
        - name: LUCILLE_ROLE
          value: worker
        - name: LUCILLE_PIPELINE
          value: my-pipeline
        - name: JAVA_OPTS
          value: "-Xmx1536m"
        volumeMounts:
        - name: lucille-config
          mountPath: /mnt/conf
        resources:
          requests:
            memory: "2Gi"
            cpu: "2"
      volumes:
      - name: lucille-config
        configMap:
          name: lucille-config
```

Run the Indexer the same way with `LUCILLE_ROLE: indexer`, and start the run with a Runner whose `LUCILLE_OPTS` is `-distributed`.

Workers are the natural scaling target. When the Kafka source topic backlog grows, increase `replicas`. Kubernetes' Horizontal Pod Autoscaler can drive this automatically using Kafka consumer group lag as a metric (via KEDA or similar).

