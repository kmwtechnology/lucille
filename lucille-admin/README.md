# Lucille Admin API

## Running the API

1. Navigate into the `lucille-admin` module
2. Run `mvn clean install` to generate the admin jar

The API can be ran locally or in a Docker container:

### Local Run:
1. Replace the placeholders with your config file names or export them as environment variables. See [Configuration](#Configuration) for more information
2. `java -Dconfig.file=${LUCILLE_CONF} -jar target/lucille-admin-0.2.3-SNAPSHOT.jar server ${DROPWIZARD_CONF}`

### Dockerized Run:
1. Create the docker image: `docker build -t lucille-admin-api .`
2. Replace the placeholders with your config file names or export them as environment variables. See [Configuration](#Configuration) for more information
3. Run the image: `docker run --env LUCILLE_CONF=${LUCILLE_CONF} --env DROPWIZARD_CONF=${DROPWIZARD_CONF} -p 8080:8080 lucille-admin-api`

## Available Endpoints

In both the local or dockerized deployment, all requests should be sent to localhost:8080. Currently, none of the endpoints expect 
any query parameters or payloads.

The available endpoints are as follows:

 |        | GET                                        | POST                                               | DELETE          |
 |--------|--------------------------------------------|----------------------------------------------------|-----------------|
 | lucille| Gets the status of the current lucille run | Kicks off a new lucille run, if one is not running | Not Implemented |
 | livez  | liveness health check endpoint             | Not Implemented                                    | Not Implemented |
 | readyz | readiness health check endpoint            | Not Implemented                                    | Not Implemented |

## Configuration

### Lucille Configuration

The Lucille Configuration file can be supplied via the LUCILLE_CONF environment variable. If building via Docker, this file should 
reside in the `lucille-admin/conf` directory, or be mounted as volume when starting the Docker container (more details in Dockerfile).

### DropWizard Configuration

Dropwizard can be configured via the `conf/admin.yml` file. Details about available parameters can be found 
[here](https://www.dropwizard.io/en/stable/manual/configuration.html#man-configuration).

If configuration beyond what Dropwizard supplies is required, the functionality can be extended via the LucilleAPIConfiguration class.
Details on how can be found [here](https://www.dropwizard.io/en/stable/manual/configuration.html#man-configuration).

### Auth Configuration

Authentication is configured under the `auth` JSONProperty.

| Property |            Description               |
|----------|--------------------------------------|
|  type    | currently only basicAuth is supprted |
| password |   supported for basic authentication | 

#### Example

```yaml
auth:
  type: basicAuth
  password: password
```

## Development

Any functionality exposed by the Admin API should be mirrored by an internal API in lucille-core which handles all of the logic. 
Currently, this internal API class is the `RunnerManager`, but future expansions could require more classes. Business logic should 
not go in the `lucille-admin` module and should all be contained within `lucille-core`.