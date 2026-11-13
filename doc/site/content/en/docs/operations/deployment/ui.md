---
title: UI
weight: 8
date: 2026-11-10
description: Running the Lucille Admin UI, which the Lucille REST API serves at /admin.
---

## Admin UI

The Lucille Admin UI is a browser console for the [Lucille REST API]({{< relref "docs/operations/api" >}}). It ships as its own JAR, `lucille-admin-ui.jar`, and runs inside the API process. When that JAR is on the API's classpath, the API serves the UI at `http://<api-host>:8080/admin`. There is no separate server to start.

> **The API is a prerequisite:** The Admin UI has no backend of its own. Every page reads from or writes to the API's `/v1` endpoints. The UI is only available while the API that serves it is running.

### Quick Start

```bash
docker run --env LUCILLE_API_PASSWORD=<password> -p 8080:8080 kmwtechnology/lucille-api:<version>
```

Open `http://localhost:8080/admin` and sign in with `<password>`. For `<version>`, use an image tag for Lucille 1.0 or later (see [With Docker](#with-docker)).

The `lucille-api` image only has `lucille-core`'s components. To use plugin components such as the Solr, Elasticsearch or OpenSearch indexers from the UI, run the `lucille-all` image in API mode instead:

```bash
docker run --env LUCILLE_ROLE=api --env LUCILLE_API_PASSWORD=<password> -p 8080:8080 kmwtechnology/lucille-all:<version>
```

See [Choosing Which Components Are Available](#choosing-which-components-are-available).

### Prerequisites

To run the Admin UI you need:

1. **A way to run the API** — Docker, or Java 21 and Maven to build and run it from source.
2. **A build that includes the UI** — the `lucille-api` Docker image (1.0 or later), or both JARs built from source. See [Getting a Build with the UI](#getting-a-build-with-the-ui).
3. **An admin password** — the default API config requires one. See [Step 1: Determine the Admin Password](#step-1-determine-the-admin-password).

The UI runs on port `8080` by default.

---

## Getting a Build with the UI

| Source | Includes the UI? |
|---|---|
| `kmwtechnology/lucille-api` Docker image, 1.0 or later | Yes — both JARs are in the image |
| `kmwtechnology/lucille-all` Docker image, with `LUCILLE_ROLE=api` | Yes — the API, the UI and every plugin |
| Source build with `-Padmin-ui` | Yes — builds `lucille-admin-ui.jar` next to the API JAR, and adds it to `lucille-all/target/lib` |
| Source build without `-Padmin-ui` | No — API only |
| Maven: `com.kmwllc:lucille-api` | No — add `com.kmwllc:lucille-admin-ui` as well (see [Embedding the API](#embedding-the-api)) |

A `-Padmin-ui` build downloads its own Node.js, so you don't need to install it. The build does need network access to nodejs.org and the npm registry.

---

## Step 1: Determine the Admin Password

The default API config turns authentication on and reads the password from the `LUCILLE_API_PASSWORD` environment variable. Choose a password now. You set it when you start the API in [Step 2](#step-2-start-the-api-with-the-ui). If it is unset or blank, the API refuses to start with:

```
java.lang.IllegalArgumentException: auth.password must be set and not blank when auth.enabled is true.
```

This is the password you enter on the UI's sign-in page. There is no username.

> **Use HTTPS when auth is on:** Over plain HTTP the password and the UI's session token travel unencrypted, and the API logs a warning at startup. Configure an HTTPS connector (see [Local HTTPS Configuration](https://github.com/kmwtechnology/lucille/tree/main/lucille-plugins/lucille-api#local-https-configuration)) or terminate TLS in front of the API.

### Running Without Authentication

For local experimentation only, you can turn authentication off. The API then needs no password, and the UI skips the sign-in page.

1. Make your own copy of the default config (see [Using Your Own API Config](#using-your-own-api-config)).
2. In the copy, set `enabled: false` under `auth`.
3. Start the API with your copy instead of `conf/api.yml`.

## Step 2: Start the API with the UI

### With Docker

Start a container from the `lucille-api` image. It includes the UI and starts the API with the default `conf/api.yml`:

```bash
docker run \
  --env LUCILLE_API_PASSWORD=<password> \
  -p 8080:8080 \
  kmwtechnology/lucille-api:<version>
```

- `<password>` — the admin password you will sign in with.
- `<version>` — an image tag for Lucille 1.0 or later. Tags are the Lucille release version followed by an image revision number, so `1.0.0.0` would be the first image for release 1.0.0. See the [available tags](https://hub.docker.com/r/kmwtechnology/lucille-api/tags) on Docker Hub.

**Building the image yourself.** To run an image built from source instead, build both JARs, then build the image from the `lucille-api` module:

```bash
mvn clean install -DskipTests -Pshaded-plugin-jars -Padmin-ui
cd lucille-plugins/lucille-api
docker build -t lucille-api .
```

Then use `lucille-api` in place of `kmwtechnology/lucille-api:<version>` in the `docker run` commands on this page.

**With every plugin.** The `lucille-all` image also runs the API and UI when you set `LUCILLE_ROLE=api`, and then every plugin component is available:

```bash
docker run \
  --env LUCILLE_ROLE=api \
  --env LUCILLE_API_PASSWORD=<password> \
  -p 8080:8080 \
  kmwtechnology/lucille-all:<version>
```

To build it yourself, run `mvn clean install -DskipTests -Padmin-ui` and then `docker build --target all -t lucille-all .` from the repository root. Its default API config is `/lucille/conf/api.yml`; to use your own, mount it and set `DROPWIZARD_CONF` as in [Using Your Own API Config](#using-your-own-api-config).

### From Source

From the repository root, build the API JAR and the UI JAR:

```bash
mvn clean install -DskipTests -Pshaded-plugin-jars -Padmin-ui
```

Then start the API with both JARs on the classpath:

```bash
cd lucille-plugins/lucille-api
export LUCILLE_API_PASSWORD=<password>
java -cp 'target/lucille-api-plugin.jar:lucille-admin-ui/target/lucille-admin-ui.jar' \
  com.kmwllc.lucille.api.APIApplication server conf/api.yml
```

Use `-cp` with the main class, not `java -jar`. `java -jar` ignores any other JARs on the classpath, so the API starts without the UI.

To run it with every plugin instead, use the classpath `lucille-all` assembles. From the repository root, after the same build:

```bash
export LUCILLE_API_PASSWORD=<password>
java -cp 'lucille-all/target/lib/*' \
  com.kmwllc.lucille.api.APIApplication server lucille-plugins/lucille-api/conf/api.yml
```

`lucille-all/target/lib` contains the API, the UI JAR (with `-Padmin-ui`) and every plugin with their dependencies. Relative paths in the configs you run, such as input files, resolve against the directory you start the API in.

### Embedding the API

If you run the API from your own Maven project, add `lucille-admin-ui` next to `lucille-api`. Both versions are managed by `lucille-bom`:

```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-api</artifactId>
</dependency>
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-admin-ui</artifactId>
</dependency>
```

Keep the two at the same version. Add the plugin modules you want available too (for example `com.kmwllc:lucille-opensearch`); `lucille-api` only depends on `lucille-core`.

### Choosing Which Components Are Available

The UI lists every connector, stage and indexer on the API's classpath, with the descriptions from each component's Javadoc.

| Option | Components available |
|---|---|
| `lucille-api` image, or `lucille-api` + `lucille-admin-ui` from source or Maven | `lucille-core` only |
| Any of the above plus specific plugin JARs on the classpath | `lucille-core` plus those plugins |
| `lucille-all` image with `LUCILLE_ROLE=api`, or `lucille-all/target/lib` from source | `lucille-core` plus every plugin |

To add one plugin to the `lucille-api` image, put the plugin's shaded JAR (built with `-Pshaded-plugin-jars`) in the image's `/lucille-api/lib/`; the image's classpath is `lib/*`. See [Using a Plugin]({{< relref "docs/operations/api#using-a-plugin" >}}).

## Step 3: Open the UI

1. Confirm the API is up:

   ```bash
   curl http://localhost:8080/v1/livez
   ```

2. Open `http://localhost:8080/admin`. The UI redirects to its sign-in page.
3. Enter the admin password and select **Sign in**. The UI opens on the dashboard.

---

## Using Your Own API Config

To change API settings, such as turning authentication off, edit a copy of the default `conf/api.yml` rather than writing one from scratch.

- **From source:** copy `lucille-plugins/lucille-api/conf/api.yml`, edit the copy, and pass its path in place of `conf/api.yml` when you start the API.
- **With Docker:** copy the default out of the image into a local `conf/` directory, edit it, then mount that directory and point `DROPWIZARD_CONF` at your copy:

  ```bash
  mkdir -p conf
  docker run --rm --entrypoint cat kmwtechnology/lucille-api:<version> conf/api.yml > conf/api.yml
  ```

  ```bash
  docker run \
    -v "$(pwd)/conf:/mnt/conf" \
    --env DROPWIZARD_CONF=/mnt/conf/api.yml \
    --env LUCILLE_API_PASSWORD=<password> \
    -p 8080:8080 \
    kmwtechnology/lucille-api:<version>
  ```

  Run both commands from the directory that contains `conf/`. If your copy turns authentication off, you can drop `--env LUCILLE_API_PASSWORD`.

For the settings the API config supports, see [Configuring the API]({{< relref "docs/operations/api#configuring-the-api" >}}).

---

## Sign-In and Sessions

- **Sessions last 4 hours.** After that, the UI returns to the sign-in page.
- **Restarting the API signs everyone out.**
- **Each browser tab signs in separately.** A session survives a page refresh but ends when the tab closes.
- **Sign out** is in the sidebar. It only appears when auth is enabled.

---

## Developing the UI

To change the UI, run the API on its own (any build, with or without the UI JAR), then start the Vite dev server in a second terminal. This needs Node.js 24.10.0 (see `.nvmrc`):

```bash
cd lucille-plugins/lucille-api/lucille-admin-ui
npm ci
npm run dev
```

Open `http://localhost:5173/admin/`. The dev server proxies `/v1` requests to the API at `http://localhost:8080` (see `vite.config.ts`). For scripts, tests, and the Maven module, see the [Admin UI README](https://github.com/kmwtechnology/lucille/tree/main/lucille-plugins/lucille-api/lucille-admin-ui#readme).
