# Using the Lucille Docker Base Image

The Lucille base Docker image provides a ready-to-run Lucille environment. Downstream consumers supply their own configuration files and optionally customize JVM and Runner settings via environment variables.

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `LUCILLE_CONF` | **Yes** | *(none)* | Path to the Lucille config file inside the container |
| `JAVA_OPTS` | No | `-Xms256m -Xmx1g` | JVM flags (heap size, GC tuning, etc.) |
| `LUCILLE_OPTS` | No | *(empty)* | Lucille Runner CLI flags (e.g. `-local`, `-usekafka`) |

## Building the Base Image

From the Lucille project root, after running `mvn clean install`:

```bash
docker build -t lucille:latest .
```

## Usage

### Option A: Custom Dockerfile (Recommended for Production)

Create your own `Dockerfile` using Lucille as a base image. This produces a self-contained, reproducible image suitable for CI/CD and orchestration platforms.

```dockerfile
FROM lucille:latest

# Copy in your config and any additional resources
COPY my-config.conf /lucille/conf/my-config.conf
COPY data/ /lucille/data/

# Set the config file path and JVM options
ENV LUCILLE_CONF=/lucille/conf/my-config.conf
ENV JAVA_OPTS="-Xms512m -Xmx4g"
```

Build and run:

```bash
docker build -t my-lucille-job .
docker run my-lucille-job
```

### Option B: Volume Mount (Recommended for Development)

Use the base image directly, mounting your config and data directories from the host:

```bash
docker run \
  --env LUCILLE_CONF=/lucille/conf/my-config.conf \
  --env JAVA_OPTS="-Xms1g -Xmx8g" \
  -v /path/on/host/conf:/lucille/conf \
  -v /path/on/host/data:/lucille/data \
  lucille:latest
```

## Configuring JVM Arguments

Control JVM behavior through the `JAVA_OPTS` environment variable:

```bash
# Set heap size
docker run --env JAVA_OPTS="-Xms1g -Xmx4g" ...

# Use container-aware memory settings (adapts to container memory limits)
docker run --env JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0" ...

# Enable G1 GC tuning
docker run --env JAVA_OPTS="-Xms1g -Xmx4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200" ...

# Enable remote debugging
docker run -p 5005:5005 --env JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005" ...
```

## Configuring Lucille Runner Options

Pass Runner CLI flags through the `LUCILLE_OPTS` environment variable:

```bash
# Run in local mode (default when LUCILLE_OPTS is empty)
docker run --env LUCILLE_CONF=/lucille/conf/my-config.conf lucille:latest

# Run with Kafka
docker run --env LUCILLE_CONF=/lucille/conf/my-config.conf --env LUCILLE_OPTS="-usekafka" lucille:latest
```

## Logging

By default, Lucille writes log output to STDOUT, which is captured by Docker. View logs with:

```bash
docker logs <container-id>
```

To persist logs to the host filesystem, mount a volume to the `/log` directory:

```bash
docker run -v /path/on/host/logs:/log ...
```

## Example: Complete Production Setup

```dockerfile
FROM lucille:0.5.0

COPY conf/production.conf /lucille/conf/production.conf
COPY conf/application.conf /lucille/conf/application.conf
COPY certs/ /lucille/certs/

ENV LUCILLE_CONF=/lucille/conf/production.conf
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -XX:+UseG1GC"
ENV LUCILLE_OPTS="-local"
```

```bash
docker build -t my-lucille-prod:1.0.0 .
docker run --memory=4g my-lucille-prod:1.0.0
```
