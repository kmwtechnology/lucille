# Using the Lucille Docker Base Image

The Lucille base Docker image provides a ready-to-run Lucille environment. Downstream consumers supply their own configuration files and optionally customize JVM and Runner settings via environment variables.

## Which Image?

Lucille publishes a few images for different needs:

| Image | Contents | Use when |
|---|---|---|
| `kmwtechnology/lucille` | `lucille-core` only. Built from `Dockerfile` (the default `core` stage). | You want a minimal image and will either stick to core components or extend the image with the specific plugin jars you need. |
| `kmwtechnology/lucille-all` | `lucille-core` **plus every component plugin** (Solr, OpenSearch, Elasticsearch, Pinecone, Weaviate, Parquet, Tika, OCR, Jlama, entity-extraction, video). Built from the same `Dockerfile` with `--target all`. | You want everything available out of the box and are willing to accept a larger image. Lowest barrier to entry. |
| `kmwtechnology/lucille-api` | `lucille-core` plus the Dropwizard-based Admin API web layer. Built from `lucille-plugins/lucille-api/Dockerfile`. | You want to drive/monitor Lucille over the REST Admin API. |

`lucille-all` is a large image. It is also tied to one CPU architecture: `lucille-jlama` resolves its native library to the architecture the image is **built** on (e.g. `osx-aarch_64`), so build `lucille-all` on — or cross-build it for — the architecture you will run it on, or the Jlama stage will fail to load its native code.

If you only need one or two components, the smaller `lucille` (core) image plus just those plugin jars is much leaner. See [Adding a Plugin to the Core Image](#adding-a-plugin-to-the-core-image) below.

The rest of this document describes the `lucille` image. The `lucille-all` image takes the same environment variables and entrypoint, so everything below applies to it as well.

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `LUCILLE_CONF` | **Yes** | *(none)* | Path to the Lucille config file inside the container |
| `JAVA_OPTS` | No | *(empty)* | JVM flags (heap size, GC tuning, etc.) |
| `LUCILLE_OPTS` | No | *(empty)* | Lucille Runner CLI flags (e.g. `-distributed`, `-external`) |

## Building the Images

Both images are built from the single multi-stage `Dockerfile`. From the Lucille project root, after running `mvn clean install`:

```bash
# Core image (minimal) — the default stage, so no --target needed
docker build -t lucille:latest .

# All-plugins image (batteries included) — explicit opt-in
docker build --target all -t lucille-all:latest .
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
ENV JAVA_OPTS="-Xms512m -Xmx512m"
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
  --env JAVA_OPTS="-Xms512m -Xmx512m" \
  -v /path/on/host/conf:/lucille/conf \
  -v /path/on/host/data:/lucille/data \
  lucille:latest
```

## Adding a Plugin to the Core Image

There are multiple ways to do this. Below will explain how to use what exists in DockerHub and Maven Central without needing to build lucille yourself.

The published `kmwtechnology/lucille` image contains only `lucille-core`. If your pipeline uses a plugin component, add that plugin on top of the image.

Pull the published `lucille` image from Docker Hub as your base, and get the plugin jar (and its dependencies) from Maven Central, where every Lucille module is published. 
The image supplies `lucille-core` on the classpath (`/lucille/lib`), so you only need to add the plugin and its *non-core* dependencies. This keeps your image lean instead of shipping the whole `lucille-all` bundle.

### 1. Gather the plugin jars from Maven Central

Declare `lucille-core` as `provided`, the image already has it, so you don't want to copy it in. Then add the plugin you need, and let `copy-dependencies` collect the plugin and its transitive dependencies:

```xml
<dependencies>
  <dependency>
    <groupId>com.kmwllc</groupId>
    <artifactId>lucille-core</artifactId>
    <version>0.12.0</version>  <!-- match the image tag -->
    <scope>provided</scope>
  </dependency>
  <dependency>
    <groupId>com.kmwllc</groupId>
    <artifactId>lucille-solr</artifactId>
    <version>0.12.0</version>
  </dependency>
</dependencies>
```

```bash
mvn dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=plugin-lib
# plugin-lib/ now holds lucille-solr and the SolrJ client stack, but not lucille-core
```

### 2. Extend the published image

```dockerfile
# Use the same version tag you compiled the plugin jars against.
FROM kmwtechnology/lucille:0.12.0

# Everything on /lucille/lib is on the classpath; lucille-core is already there.
COPY plugin-lib/ /lucille/lib/

COPY my-config.conf /lucille/conf/my-config.conf
ENV LUCILLE_CONF=/lucille/conf/my-config.conf
```

```bash
docker build -t my-lucille-solr .
```

> Match the plugin version to the image tag. A plugin built against a different `lucille-core` version than the image ships is not supported.

### Alternative: a single shaded plugin jar (from a source build)

If you are already building Lucille from source and would rather add one file than a directory, build the shaded `*-plugin.jar`. It bundles the plugin and its dependencies into a single artifact and deliberately excludes `lucille-core` (the image already provides it):

```bash
# from the Lucille repo root
mvn clean package -pl lucille-plugins/lucille-solr -am -Pshaded-plugin-jars
# produces lucille-plugins/lucille-solr/target/lucille-solr-plugin.jar
```

```dockerfile
FROM kmwtechnology/lucille:0.13.0
COPY lucille-solr-plugin.jar /lucille/lib/
COPY my-config.conf /lucille/conf/my-config.conf
ENV LUCILLE_CONF=/lucille/conf/my-config.conf
```

> If you need several plugins, or would rather not manage any of this, use the `lucille-all` image instead — it already contains every component plugin.

## Configuring JVM Arguments

Control JVM behavior through the `JAVA_OPTS` environment variable:

```bash
# Example: Adjust heap size
docker run --env JAVA_OPTS="-Xms512m -Xmx512m" ...

# Example: Remote debugging
docker run -p 5005:5005 --env JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005" ...
```

## Configuring Lucille Runner Options

Pass Runner CLI flags through the `LUCILLE_OPTS` environment variable:

```bash
# Run in local mode (default when LUCILLE_OPTS is empty)
docker run --env LUCILLE_CONF=/lucille/conf/my-config.conf lucille:latest

# Run in distributed mode: only connectors run here; Workers and Indexers are
# deployed separately and communicate via Kafka
docker run --env LUCILLE_CONF=/lucille/conf/my-config.conf --env LUCILLE_OPTS="-distributed" lucille:latest

# Run in external mode: Workers and Indexer are threads in this JVM, but
# inter-component communication goes through Kafka
docker run --env LUCILLE_CONF=/lucille/conf/my-config.conf --env LUCILLE_OPTS="-external" lucille:latest
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
ENV JAVA_OPTS="-Xms512m"
ENV LUCILLE_OPTS=""
```

```bash
docker build -t my-lucille-prod:1.0.0 .
docker run --memory=4g my-lucille-prod:1.0.0
```
