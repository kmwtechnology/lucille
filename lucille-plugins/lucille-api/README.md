# Lucille API

## Running the API

1. Navigate into the `lucille-api` module
2. Run `mvn clean install` to generate the api jar

The API can be run locally or in a Docker container:

### Local Run

1. Replace the placeholders with your config file names or export them as environment variables. See [Configuration](#configuration) for more information
2. `java -jar target/lucille-api-plugin.jar server ${DROPWIZARD_CONF}`

### Dockerized Run

1. Create the docker image: `docker build -t lucille-api .`
2. Replace the placeholders with your config file names or export them as environment variables. See [Configuration](#configuration) for more information
3. Run the image: `docker run --env LUCILLE_CONF=${LUCILLE_CONF} --env DROPWIZARD_CONF=${DROPWIZARD_CONF} -p 8080:8080 lucille-api`

### Upload Lucille Config
1. This should be a HOCON file
2. `curl -X POST http://localhost:8080/v1/config -H "Content-Type: application/hocon" -d @${LUCILLE_CONF}`
3. This will respond with a configId for your config

### Run Lucille with the Config
1. `curl -X POST http://localhost:8080/v1/run -H "Content-Type: application/json" -d '{"configId":"CONFIG_ID"}'`
2. You can do `curl http://localhost:8080/v1/run` to check the status of your runs

## Available Endpoints

All requests are served from `localhost:8080` (or `localhost:8443` if HTTPS is enabled — see [Setting up HTTPS](#setting-up-https-with-dropwizard)). When auth is enabled, every endpoint except the Dropwizard admin port requires Basic Auth credentials.

| Method | Path | Description | Body |
|--------|------|-------------|------|
| GET    | `/v1/livez`                          | Liveness probe — returns 200 if the process is running | — |
| GET    | `/v1/readyz`                         | Readiness probe — returns 200 once the app is ready to serve | — |
| GET    | `/v1/systemstats`                    | CPU, memory, and JVM resource usage | — |
| GET    | `/v1/systemstats/metrics`            | Detailed metrics breakdown | — |
| GET    | `/v1/config-info/connector-list`     | Lists the available connector classes | — |
| GET    | `/v1/config-info/stage-list`         | Lists the available stage classes | — |
| GET    | `/v1/config-info/indexer-list`       | Lists the available indexer classes | — |
| POST   | `/v1/config`                         | Register a new Lucille config. Responds with a generated `configId` to use in `POST /v1/run` | Full Lucille config as JSON (see [Upload Lucille Config](#upload-lucille-config)) |
| GET    | `/v1/config`                         | List all registered configs, keyed by `configId` | — |
| GET    | `/v1/config/{configId}`              | Get a specific registered config | — |
| POST   | `/v1/run`                            | Start a Lucille run using a previously registered config | `{"configId": "<uuid>"}` |
| GET    | `/v1/run`                            | List all runs and their statuses | — |
| GET    | `/v1/run/{runId}`                    | Get details of a specific run | — |

### Examples

```bash
# Health
curl http://localhost:8080/v1/livez

# Register a config (returns {"configId": "<uuid>"})
curl -X POST http://localhost:8080/v1/config \
  -H "Content-Type: application/json" \
  -d @conf/simple-config.json

# Kick off a run with that configId
curl -X POST http://localhost:8080/v1/run \
  -H "Content-Type: application/json" \
  -d '{"configId":"<uuid>"}'

# Check run status
curl http://localhost:8080/v1/run
curl http://localhost:8080/v1/run/<runId>
```

If auth is enabled, add `-u <anyuser>:<password>` (the username is not validated, only the password). If HTTPS is enabled with a self-signed cert, also add `-k`:

```bash
curl -k -u user:password https://localhost:8443/v1/run
```

## Configuration

### Lucille Configuration

The Lucille Configuration file can be supplied via the LUCILLE_CONF environment variable. If building via Docker, this file should
reside in the `lucille-api/conf` directory, or be mounted as volume when starting the Docker container (more details in Dockerfile).

### DropWizard Configuration

Dropwizard can be configured via the `conf/api.yml` file. Details about available parameters can be found
[here](https://www.dropwizard.io/en/stable/manual/configuration.html#man-configuration).

If configuration beyond what Dropwizard supplies is required, the functionality can be extended via the LucilleAPIConfiguration class.
Details on how can be found [here](https://www.dropwizard.io/en/stable/manual/configuration.html#man-configuration).

### Auth Configuration

Authentication is configured under the `auth` block. When `enabled: true`, every endpoint in the table above (except the Dropwizard admin port, which is separate) requires Basic Auth credentials.

| Property |            Description                |
|----------|-------------------------------------- |
| type     | currently only `basicAuth` is supported |
| enabled  | `true` to require auth, `false` to disable |
| password | the password Basic Auth requests must match |

Note: Only the password is validated, so any non-empty username works.

#### Example

```yaml
auth:
  type: basicAuth
  enabled: true
  password: KEYSTORE_PASSWORD
```

### Local HTTPS Configuration

Add an `applicationConnectors` block to the `server:` section of your DropWizard yml config file:

```yaml
server:
  applicationConnectors:
    - type: https
      port: 8443
      keyStorePath: PATH_TO_JKS
      keyStorePassword: KEYSTORE_PASSWORD
      keyStoreType: JKS
```
We provide an example of this for you at conf/https_api.yml.

#### Local development (self-signed cert)

For local development you can generate a self-signed keystore. Run this from the `lucille-plugins/lucille-api/` directory so the file lands at `conf/keystore.jks` (matching the `keyStorePath` in `api.yml`):

```bash
keytool -genkeypair -alias dw-server -keyalg RSA -keysize 2048 \
  -storetype JKS -keystore PATH_TO_JKS -storepass KEYSTORE_PASSWORD \
  -validity 365 -dname "CN=localhost, OU=Dev, O=Local, L=., ST=., C=US" \
  -ext san=dns:localhost,ip:127.0.0.1
```

The `-ext san=...` is necessary, or else Jetty's SNI host check rejects requests to `localhost` with `400 Invalid SNI`. As an alternative to embedding SANs, you can disable the SNI check on the connector:

```yaml
    - type: https
      port: 8443
      keyStorePath: PATH_TO_JKS
      keyStorePassword: KEYSTORE_PASSWORD
      disableSniHostCheck: true
```

Hit endpoints with `-k` (skip cert verification, since the cert isn't in any trust store) and `-u <anyuser>:<password>` if auth is on:

```bash
curl -k -u user:password https://localhost:8443/v1/config -H "Content-Type: application/json" -d @${LUCILLE_CONF}
```

#### Production HTTPS
For a real deployment you want a CA-issued cert, the strict defaults turned back on, and secrets out of the YAML file.

## Logging and Integration Testing with Dropwizard

### Important Notes for Logging Configuration

The lucille-api module uses Dropwizard, which is tightly coupled with Logback for logging, especially during integration and test runs. If you attempt to use Log4j2 or mix multiple SLF4J providers (e.g., log4j-slf4j2-impl and logback-classic), you will encounter errors such as:

- `NoClassDefFoundError: ch/qos/logback/classic/LoggerContext`
- `IllegalStateException: Unable to acquire the logger context`
- SLF4J multiple provider warnings

#### **Best Practices for Integration Tests:**
- **Use only Logback for integration and test runs.**
- Remove all log4j2 dependencies and configuration files from the classpath for tests.
- Ensure only `logback-classic` is present as a test dependency in your `pom.xml`.
- Exclude `log4j-slf4j2-impl` from any transitive dependencies (such as lucille-core) to prevent SLF4J provider conflicts.
- If you want to use Log4j2 for production, you can do so in your main application module, but keep Logback for Dropwizard-based tests.

#### **Troubleshooting**
If you see logger context or SLF4J errors during integration tests:
- Make sure no log4j2 SLF4J bridge jars are present in the test classpath.
- Check your dependency tree for `log4j-slf4j2-impl` and exclude it if found.
- Make sure only one SLF4J provider (logback-classic) is on the test classpath.
- Free up any ports (e.g., 8080) required by integration tests before running them.

This setup ensures reliable Dropwizard integration testing and avoids logger conflicts.

## Development

Any functionality exposed by the Admin API should be mirrored by an internal API in lucille-core which handles all of the logic.
Currently, this internal API class is the `RunnerManager`, but future expansions could require more classes. Business logic should
not go in the `lucille-api` module and should all be contained within `lucille-core`.
