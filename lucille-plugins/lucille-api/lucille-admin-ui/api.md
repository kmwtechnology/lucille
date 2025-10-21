# Lucille API Documentation

This document provides comprehensive documentation for the Lucille API endpoints. It serves as a reference for UI development and API integration.

## Table of Contents

- [Authentication](#authentication)
- [Health Check Endpoints](#health-check-endpoints)
  - [Liveness Check](#liveness-check)
  - [Readiness Check](#readiness-check)
- [Configuration Management](#configuration-management)
  - [Create Configuration](#create-configuration)
  - [Get All Configurations](#get-all-configurations)
  - [Get Configuration by ID](#get-configuration-by-id)
- [Run Management](#run-management)
  - [Start Run](#start-run)
  - [Get All Runs](#get-all-runs)
  - [Get Run by ID](#get-run-by-id)
- [API Explorer](#api-explorer)

## Authentication

The Lucille API supports Basic Authentication, which can be enabled or disabled in the configuration. By default, authentication is disabled for development purposes.

**Authentication Configuration:**
```yaml
auth:
  type: "basicAuth"  # Authentication type (basicAuth or noAuth)
  enabled: false     # Whether authentication is enabled
  password: "password"  # Password for basic authentication
```

When authentication is enabled, you must include the Authorization header with your requests:

```
Authorization: Basic YWRtaW46cGFzc3dvcmQ=
```

This is the Base64 encoding of `admin:password`.

## Health Check Endpoints

### Liveness Check

Checks if the Lucille API service is running.

**Endpoint:** `GET /v1/livez`

**Authentication Required:** No

**Response:**
- `200 OK` - Service is live

**Example:**
```bash
curl -i http://localhost:8080/v1/livez
```

**Example Response:**
```
HTTP/1.1 200 OK
Date: Fri, 25 Apr 2025 14:25:21 GMT
Content-Length: 0
```

### Readiness Check

Checks if the Lucille API service is ready to receive traffic.

**Endpoint:** `GET /v1/readyz`

**Authentication Required:** No

**Response:**
- `200 OK` - Service is ready

**Example:**
```bash
curl -i http://localhost:8080/v1/readyz
```

**Example Response:**
```
HTTP/1.1 200 OK
Date: Fri, 25 Apr 2025 14:25:21 GMT
Content-Length: 0
```

## Configuration Management

### Create Configuration

Creates a new Lucille configuration that can be used to start runs.

**Endpoint:** `POST /v1/config`

**Authentication Required:** Yes (if enabled)

**Request Body:**
A JSON object containing the Lucille configuration with the following structure:
```json
{
  "connectors": [
    {
      "class": "com.kmwllc.lucille.connector.CSVConnector",
      "path": "conf/dummy2.csv",
      "name": "connector1",
      "pipeline": "pipeline1"
    }
  ],
  "pipelines": [
    {
      "name": "pipeline1",
      "stages": []
    }
  ],
  "indexer": {
    "type": "CSV"
  },
  "csv": {
    "columns": ["Name", "Age", "City"],
    "path": "conf/dummy.csv",
    "includeHeader": false
  }
}
```

**Response:**
- `200 OK` - Configuration created successfully
- `400 Bad Request` - Invalid configuration
- `401 Unauthorized` - Authentication failed

**Response Body (Success):**
```json
{
  "configId": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Example:**
```bash
curl -i -X POST \
  -H "Content-Type: application/json" \
  -d '{
    "connectors": [
      {
        "class": "com.kmwllc.lucille.connector.CSVConnector",
        "path": "conf/dummy2.csv",
        "name": "connector1",
        "pipeline": "pipeline1"
      }
    ],
    "pipelines": [
      {
        "name": "pipeline1",
        "stages": []
      }
    ],
    "indexer": {
      "type": "CSV"
    },
    "csv": {
      "columns": ["Name", "Age", "City"],
      "path": "conf/dummy.csv",
      "includeHeader": false
    }
  }' \
  http://localhost:8080/v1/config
```

### Get All Configurations

Retrieves all Lucille configurations.

**Endpoint:** `GET /v1/config`

**Authentication Required:** Yes (if enabled)

**Response:**
- `200 OK` - Returns all configurations
- `401 Unauthorized` - Authentication failed

**Response Body (Success):**
```json
{
  "configId1": {
    "connectors": [...],
    "pipelines": [...],
    ...
  },
  "configId2": {
    ...
  }
}
```

**Example:**
```bash
curl -i http://localhost:8080/v1/config
```

### Get Configuration by ID

Retrieves a specific Lucille configuration by its ID.

**Endpoint:** `GET /v1/config/{configId}`

**Authentication Required:** Yes (if enabled)

**Path Parameters:**
- `configId` - The UUID of the configuration to retrieve

**Response:**
- `200 OK` - Returns the specified configuration
- `404 Not Found` - Configuration not found
- `401 Unauthorized` - Authentication failed

**Response Body (Success):**
```json
{
  "connectors": [...],
  "pipelines": [...],
  ...
}
```

**Example:**
```bash
curl -i http://localhost:8080/v1/config/550e8400-e29b-41d4-a716-446655440000
```

## Run Management

### Start Run

Starts a new Lucille run with the specified configuration.

**Endpoint:** `POST /v1/run`

**Authentication Required:** Yes (if enabled)

**Request Body:**
```json
{
  "configId": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Response:**
- `200 OK` - Run started successfully
- `400 Bad Request` - Invalid configuration ID or missing configId
- `401 Unauthorized` - Authentication failed

**Response Body (Success):**
A RunDetails object containing information about the run:
```json
{
  "runId": "550e8400-e29b-41d4-a716-446655440000",
  "configId": "550e8400-e29b-41d4-a716-446655440000",
  "startTime": "2025-04-25T14:25:21.000Z",
  "endTime": null,
  "status": "RUNNING",
  "documentCount": 0,
  "errorCount": 0
}
```

**Example:**
```bash
curl -i -X POST \
  -H "Content-Type: application/json" \
  -d '{"configId": "550e8400-e29b-41d4-a716-446655440000"}' \
  http://localhost:8080/v1/run
```

### Get All Runs

Retrieves a list of all Lucille runs.

**Endpoint:** `GET /v1/run`

**Authentication Required:** Yes (if enabled)

**Response:**
- `200 OK` - Returns all runs
- `401 Unauthorized` - Authentication failed

**Response Body (Success):**
```json
{
  "runId1": {
    "runId": "runId1",
    "configId": "configId1",
    "startTime": "2025-04-25T14:25:21.000Z",
    "endTime": "2025-04-25T14:30:21.000Z",
    "status": "COMPLETED",
    "documentCount": 100,
    "errorCount": 0
  },
  "runId2": {
    "runId": "runId2",
    "configId": "configId2",
    "startTime": "2025-04-25T14:35:21.000Z",
    "endTime": null,
    "status": "RUNNING",
    "documentCount": 50,
    "errorCount": 0
  }
}
```

**Example:**
```bash
curl -i http://localhost:8080/v1/run
```

### Get Run by ID

Retrieves the details of a specific Lucille run by its run ID.

**Endpoint:** `GET /v1/run/{runId}`

**Authentication Required:** Yes (if enabled)

**Path Parameters:**
- `runId` - The ID of the run to retrieve

**Response:**
- `200 OK` - Returns the specified run
- `400 Bad Request` - Run not found
- `401 Unauthorized` - Authentication failed

**Response Body (Success):**
```json
{
  "runId": "550e8400-e29b-41d4-a716-446655440000",
  "configId": "550e8400-e29b-41d4-a716-446655440000",
  "startTime": "2025-04-25T14:25:21.000Z",
  "endTime": "2025-04-25T14:30:21.000Z",
  "status": "COMPLETED",
  "documentCount": 100,
  "errorCount": 0
}
```

**Example:**
```bash
curl -i http://localhost:8080/v1/run/550e8400-e29b-41d4-a716-446655440000
```

## API Explorer

The Lucille API includes Swagger UI for interactive API exploration and testing. You can access the Swagger UI at:

```
http://localhost:8080/swagger
```

This provides a user-friendly interface to:
- View all available endpoints
- See request/response schemas
- Test API calls directly from the browser
- Understand authentication requirements

The Swagger UI is particularly useful during development and testing phases, allowing you to quickly test API functionality without writing code.
