---
title: Kubernetes
weight: 4
date: 2025-06-09
description: Deploying Lucille on Kubernetes as CronJobs and scalable pod deployments.
---

## Kubernetes Deployment

Lucille's architecture maps naturally onto Kubernetes primitives.

### Batch Jobs as Kubernetes CronJobs

For scheduled batch ingests, package Lucille as a container and run it as a `CronJob`. When the run completes, the container exits — there is no long-running process to manage between runs.

**Minimal Dockerfile:**
```dockerfile
FROM eclipse-temurin:17-jre

WORKDIR /app

# Copy all JARs (lucille-core + dependencies) from the Maven build output
COPY target/lib/ lib/

# Copy your pipeline configuration
COPY conf/ conf/

# The config file path is provided via an environment variable at runtime
ENV CONF=""
ENTRYPOINT ["sh", "-c", "java -Xmx4g -Dconfig.file=${CONF} -cp 'lib/*' com.kmwllc.lucille.core.Runner"]
```

Build the image after running `mvn clean install` in your project (which copies all dependencies to `target/lib/`):

```bash
docker build -t my-lucille-image .
```

Run it:

```bash
docker run --env CONF=conf/my-pipeline.conf \
           --env OPENSEARCH_URL=https://opensearch:9200 \
           my-lucille-image
```

The same image can run any Lucille component by overriding the entrypoint:

```bash
# Run as a Worker (distributed mode)
docker run --env CONF=conf/my-pipeline.conf \
           --entrypoint sh my-lucille-image \
           -c "java -Xmx2g -Dconfig.file=\${CONF} -cp 'lib/*' com.kmwllc.lucille.core.Worker my-pipeline"

# Run as an Indexer (distributed mode)
docker run --env CONF=conf/my-pipeline.conf \
           --entrypoint sh my-lucille-image \
           -c "java -Xmx1g -Dconfig.file=\${CONF} -cp 'lib/*' com.kmwllc.lucille.core.Indexer my-pipeline"
```

**CronJob:**
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
            image: my-registry/lucille:latest
            env:
            - name: OPENSEARCH_URL
              valueFrom:
                secretKeyRef:
                  name: opensearch-credentials
                  key: url
            resources:
              requests:
                memory: "2Gi"
                cpu: "1"
              limits:
                memory: "4Gi"
                cpu: "4"
```

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
        image: my-registry/lucille:latest
        command: ["java", "-cp", "/app/lucille.jar:/app/lib/*",
                  "com.kmwllc.lucille.core.Worker", "my-pipeline"]
        env:
        - name: config.file
          value: /app/config.conf
        resources:
          requests:
            memory: "2Gi"
            cpu: "2"
```

Workers are the natural scaling target. When the Kafka source topic backlog grows, increase `replicas`. Kubernetes' Horizontal Pod Autoscaler can drive this automatically using Kafka consumer group lag as a metric (via KEDA or similar).

