# Lucille API

## Running the API

1. Navigate into the `lucille-api` module
2. Run `mvn clean install` to generate the api jar

The API can be run locally or in a Docker container:

### Local Run

1. Replace the placeholders with your config file names or export them as environment variables. See [Configuration](#configuration) for more information
2. `java -Dconfig.file=${LUCILLE_CONF} -jar target/lucille-api-X.X.X-SNAPSHOT.jar server ${DROPWIZARD_CONF}`

### Dockerized Run

1. Create the docker image: `docker build -t lucille-api .`
2. Replace the placeholders with your config file names or export them as environment variables. See [Configuration](#configuration) for more information
3. Run the image: `docker run --env LUCILLE_CONF=${LUCILLE_CONF} --env DROPWIZARD_CONF=${DROPWIZARD_CONF} -p 8080:8080 lucille-api`

## Available Endpoints

In both the local or dockerized deployment, all requests should be sent to localhost:8080.

The available endpoints are as follows:

### Core Management

 |        | GET                                        | POST                                               | DELETE          |
 |--------|--------------------------------------------|----------------------------------------------------|-----------------|
 | /v1/config | Retrieve all configurations | Create a new configuration | Not Implemented |
 | /v1/config/{configId} | Get a specific configuration by ID | Not Implemented | Not Implemented |
 | /v1/run | Retrieve all pipeline runs | Start a new pipeline run | Not Implemented |
 | /v1/run/{runId} | Get a specific run by ID | Not Implemented | Not Implemented |

### Health & Monitoring

 |        | GET                                        | POST                                               | DELETE          |
 |--------|--------------------------------------------|----------------------------------------------------|-----------------|
 | /v1/livez  | Liveness check (HTTP 200 if running) | Not Implemented | Not Implemented |
 | /v1/readyz | Readiness check (HTTP 200 if ready) | Not Implemented | Not Implemented |
 | /v1/systemstats | System resource usage (CPU, RAM, JVM, disk) | Not Implemented | Not Implemented |
 | /v1/systemstats/metrics | Dropwizard metrics registry | Not Implemented | Not Implemented |

### Configuration Metadata & Documentation

 |        | GET                                        | POST                                               | DELETE          |
 |--------|--------------------------------------------|----------------------------------------------------|-----------------|
 | /v1/config-info/connector-list | List all available connector classes with specs | Not Implemented | Not Implemented |
 | /v1/config-info/stage-list | List all available pipeline stage classes with specs | Not Implemented | Not Implemented |
 | /v1/config-info/indexer-list | List all available indexer classes with specs | Not Implemented | Not Implemented |
 | /v1/config-info/javadoc-list/{type} | Get javadoc and metadata for a component type (connector, stage, or indexer) | Not Implemented | Not Implemented |

### API Documentation

 | Resource | Description |
 |----------|-------------|
 | /swagger | Interactive Swagger/OpenAPI UI for exploring and testing endpoints |

## CORS Support

The API includes built-in CORS (Cross-Origin Resource Sharing) support to enable browser-based clients to make requests. The following CORS headers are automatically added to all responses:

- `Access-Control-Allow-Origin: *` - Allows requests from any origin
- `Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS, HEAD`
- `Access-Control-Allow-Headers: Content-Type, Authorization, X-Requested-With`
- `Access-Control-Max-Age: 3600` - Preflight requests cached for 1 hour

This allows the Lucille Admin UI (running on a different port) to communicate with the API without CORS blocking.

## Admin UI Integration

The Lucille API can be paired with the **Lucille Admin UI** (a Next.js application) to provide a web-based interface for:

- **Managing Configurations**: Create, view, and manage pipeline configurations
- **Monitoring Runs**: Track pipeline execution status, document counts, and errors
- **Browsing Components**: Explore available connectors, stages, and indexers
- **Viewing Documentation**: Access generated JavaDocs for configuration parameters

### Architecture: How API and UI Work Together

**Development Mode:**
- API runs on `http://localhost:8080`
- UI dev server runs on `http://localhost:3000` (via `npm run dev`)
- Services communicate via CORS (enabled by default)
- Changes to UI code auto-refresh via hot reloading

**Production Mode:**
- UI is built as static files (`npm run build`)
- Static files are served by the API on port 8080 at `/` (or a configured path)
- Single service deployment - no separate frontend server needed
- All requests go through the API

### Running Locally for Development

The Admin UI is located in the `lucille-admin-ui/` directory.

**For detailed setup and development instructions**, see the [Admin UI README](./lucille-admin-ui/README.md).

**Quick Start:**

```bash
# Terminal 1: Start the API (from lucille-api directory)
export LUCILLE_CONF=$(pwd)/conf/simple-config.conf
export DROPWIZARD_CONF=$(pwd)/conf/api.yml
java -Dconfig.file=$LUCILLE_CONF -jar target/lucille-api-plugin.jar server $DROPWIZARD_CONF

# Terminal 2: Start the UI dev server (from lucille-admin-ui directory)
cd lucille-admin-ui
npm install
npm run dev
```

Then visit `http://localhost:3000` in your browser. The development server includes hot reloading for rapid development.

### Building for Production

To build the static UI and prepare for deployment with the API:

```bash
cd lucille-admin-ui
npm run build
```

This creates a static export in the `out/` directory. The files can then be:
1. Copied into the API's static resources directory for serving by the API, or
2. Deployed separately as a static site

See the [Admin UI README](./lucille-admin-ui/README.md) for production deployment options.

### Features

- **Real-time API Status**: Dashboard shows API health, system resources, and run statistics
- **Configuration Builder**: Interactive UI to create and manage Lucille configurations
- **Run Management**: Start new runs and monitor execution progress
- **Component Documentation**: Browse all available connectors, indexers, and stages with auto-generated documentation
- **JavaDocs Viewer**: Click "JavaDocs" on any component to see its full documentation including configuration parameters

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

Authentication is configured under the `auth` JSONProperty.

| Property |            Description                |
|----------|-------------------------------------- |
|  type    | currently only basicAuth is supported |
| password |   supported for basic authentication  |

#### Example

```yaml
auth:
  type: basicAuth
  password: password
```

### JavaDoc Generation

The API can serve auto-generated JavaDoc documentation for connectors, stages, and indexers through the `/v1/config-info/javadoc-list/{type}` endpoints.

To generate the JavaDoc JSON files:

```bash
cd ../lucille
./run-jsondoclet.sh
```

This script uses a custom JsonDoclet to extract documentation from the source code and generates three JSON files:

- `connector-javadocs.json` - Connector class documentation
- `stage-javadocs.json` - Pipeline stage class documentation
- `indexer-javadocs.json` - Indexer class documentation

These files are copied into the API JAR during the build and are served by the `/v1/config-info/javadoc-list/{type}` endpoints. The Admin UI uses these endpoints to display component documentation in the JavaDocs modal.

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
