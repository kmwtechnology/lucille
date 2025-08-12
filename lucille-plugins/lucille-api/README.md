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

In both the local or dockerized deployment, all requests should be sent to localhost:8080. Currently, none of the endpoints expect
any query parameters or payloads.

The available endpoints are as follows:

 |        | GET                                        | POST                                               | DELETE          |
 |--------|--------------------------------------------|----------------------------------------------------|-----------------|
 | /v1/lucille| Gets the status of the current lucille run | Kicks off a new lucille run, if one is not running | Not Implemented |
 | /v1/livez  | liveness health check endpoint             | Not Implemented                                    | Not Implemented |
 | /v1/readyz | readiness health check endpoint            | Not Implemented                                    | Not Implemented |
 | /v1/systemstats | system resource usage endpoint             | Not Implemented                                    | Not Implemented |

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
