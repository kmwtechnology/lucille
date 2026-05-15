---
title: "SPEC Validation System"
weight: 20
date: 2025-06-09
description: >
  How Lucille validates configuration before a run starts, catching typos and missing fields at startup.
---

## Overview

The SPEC system is Lucille's configuration validation framework. It catches config errors — typos, missing required fields, wrong types — before any processing starts. Every Stage, Connector, and Indexer declares a `public static final Spec SPEC` that defines what configuration properties it accepts.

The philosophy: fail loudly at startup, not silently at runtime.

## What a SPEC Is

A `Spec` is an immutable set of `Property` declarations that describes the legal configuration for a component. Each property has:
- A **name** (the config key)
- A **required/optional** flag
- A **type** (string, number, boolean, list, or object/parent)

```java
public class Spec {
    private final String name;           // Non-null for "parent" specs (nested config blocks)
    private final Set<Property> properties;
}
```

When `validate(config, displayName)` is called, the Spec checks:
1. All required properties are present
2. All properties have the correct type
3. No unknown/unrecognized properties exist

## How SpecBuilder Works

`SpecBuilder` is the fluent API for constructing Specs. It provides factory methods for different component types:

```java
// For a Stage — includes name, class, conditions, conditionPolicy as defaults
SpecBuilder.stage()

// For a Connector — includes name, class, pipeline, docIdPrefix, collapse as defaults
SpecBuilder.connector()

// For a specific Indexer implementation — no defaults
SpecBuilder.indexer()

// For a FileHandler — includes class, docIdPrefix as defaults
SpecBuilder.fileHandler()

// For arbitrary config blocks — no defaults
SpecBuilder.withoutDefaults()

// For nested config objects
SpecBuilder.parent("parentName")
```

Each factory method pre-populates the builder with default legal properties appropriate for that component type.

## Builder Methods

### Basic Types

```java
.requiredString("fieldName")       // Must be present, must be a string
.optionalString("fieldName")       // May be absent, must be string if present
.requiredNumber("fieldName")       // Must be present, must be numeric
.optionalNumber("fieldName")
.requiredBoolean("fieldName")
.optionalBoolean("fieldName")
```

### Objects (Nested Config Blocks)

```java
// With a named Spec describing the object's structure
.requiredParent(myParentSpec)
.optionalParent(myParentSpec)

// With a TypeReference for unstructured objects (e.g., Map<String, String>)
.requiredParent("name", new TypeReference<Map<String, String>>(){})
.optionalParent("name", new TypeReference<Map<String, String>>(){})
```

### Lists

```java
// List of configs with known structure
.requiredList("name", objectSpec)
.optionalList("name", objectSpec)

// List with a TypeReference (e.g., List<String>)
.requiredList("name", new TypeReference<List<String>>(){})
.optionalList("name", new TypeReference<List<String>>(){})
```

### With Descriptions

Every method has a `WithDescription` variant for documentation generation:
```java
.requiredStringWithDescription("url", "The Solr endpoint URL")
```

## How Validation Is Triggered

### Stages

In the `Stage` base class constructor:

```java
public Stage(Config config) {
    this.name = ConfigUtils.getOrDefault(config, "name", null);
    this.config = config;

    // Validate using the subclass's SPEC
    getSpec().validate(config, getDisplayName());

    this.condition = getMergedConditions();
}
```

The `getSpec()` method uses reflection to access the subclass's static `SPEC` field:

```java
public Spec getSpec() {
    try {
        return (Spec) this.getClass().getDeclaredField("SPEC").get(null);
    } catch (Exception e) {
        throw new RuntimeException(
            "Error accessing " + getClass() + " Spec. Is it publicly and statically available under \"SPEC\"?", e);
    }
}
```

This means validation happens automatically when any Stage is constructed — including during the Runner's validation pass.

### Indexers

The `Indexer` base class validates both the generic `indexer` config block and the implementation-specific block:

```java
private void validateIndexerConfigs(Config config) {
    // Validate generic "indexer" block
    Config indexerConfig = config.getConfig("indexer");
    SpecBuilder.withoutDefaults()
        .optionalString("type", "class", "idOverrideField", ...)
        .optionalNumber("batchSize", "batchTimeout", ...)
        .build()
        .validate(indexerConfig, "Indexer");

    // Validate implementation-specific block (e.g., "solr", "elasticsearch")
    String indexerConfigKey = getIndexerConfigKey();
    if (indexerConfigKey != null && config.hasPath(indexerConfigKey)) {
        Config specificImplConfig = config.getConfig(indexerConfigKey);
        getImplementationSpec().validate(specificImplConfig, indexerConfigKey);
    }
}
```

### Connectors

Connectors validate via `Connector.getConnectorConfigExceptions()` which instantiates the connector (triggering its constructor validation).

## Required vs Optional Enforcement

The `Property` base class handles this:
- **Required**: If the property is not present in the config, validation fails with an error message
- **Optional**: If the property is absent, no error. If present, type checking still applies.

## Nested Specs (Parent Specs)

For complex connectors with cloud provider configs, nested specs describe sub-objects:

```java
// FileConnector declares parent specs for each cloud provider
public static final Spec GCP_PARENT_SPEC = SpecBuilder.parent("gcp")
    .requiredString("pathToServiceKey")
    .optionalNumber("maxNumOfPages").build();

public static final Spec S3_PARENT_SPEC = SpecBuilder.parent("s3")
    .optionalString("accessKeyId", "secretAccessKey", "region")
    .optionalNumber("maxNumOfPages").build();

public static final Spec AZURE_PARENT_SPEC = SpecBuilder.parent("azure")
    .optionalString("connectionString", "accountName", "accountKey")
    .optionalNumber("maxNumOfPages").build();

// Used in the connector's SPEC
public static final Spec SPEC = SpecBuilder.connector()
    .requiredList("paths", new TypeReference<List<String>>(){})
    .optionalParent(GCP_PARENT_SPEC, AZURE_PARENT_SPEC, S3_PARENT_SPEC)
    .build();
```

When a parent spec is validated, it checks the properties within that nested config block. The parent's name (e.g., "gcp") becomes a legal top-level property, and its children (e.g., "gcp.pathToServiceKey") are validated against the parent spec's property set.

## Error Collection (Not Fail-Fast)

Validation collects **all** errors before reporting:

```java
public void validate(Config config, String displayName) {
    Set<String> errorMessages = new HashSet<>();

    // Check for unknown properties
    for (String key : keys) {
        if (!legalProperties.contains(key)) {
            errorMessages.add("Config contains unknown property " + key);
        }
    }

    // Check each declared property
    for (Property property : properties) {
        try {
            property.validate(config);
        } catch (IllegalArgumentException e) {
            errorMessages.add(e.getMessage());
        }
    }

    if (!errorMessages.isEmpty()) {
        throw new IllegalArgumentException("Errors with " + displayName + " Config: " + errorMessages);
    }
}
```

This means a developer sees all config problems at once, not one at a time.

## Unrecognized Property Rejection

The Spec explicitly rejects any property not declared as legal:

```java
Set<String> legalProperties = getLegalProperties();
for (String key : keys) {
    if (!legalProperties.contains(key)) {
        String parentName = getParent(key);
        if (parentName == null) {
            errorMessages.add("Config contains unknown property " + key);
        } else if (!legalProperties.contains(parentName)) {
            errorMessages.add("Config contains unknown parent " + parentName);
        }
    }
}
```

This catches typos. If you write `batchSze` instead of `batchSize`, you get an error instead of silent default behavior.

The `getParent()` helper handles dotted paths: for a key like `s3.region`, it checks if `s3` is a legal parent before flagging `s3.region` as unknown.

## The Validate-Without-Execution Mode

The Runner's `-validate` flag triggers `runInValidationMode()`:

```java
public static Map<String, List<Exception>> runInValidationMode(Config config) throws Exception {
    config = config.resolve();
    Map<String, List<Exception>> allExceptionsMap = new HashMap<>();

    Map<String, List<Exception>> pipelineExceptions = validatePipelines(config);
    Map<String, List<Exception>> connectorExceptions = validateConnectors(config);
    List<Exception> indexerExceptions = validateIndexer(config);
    List<Exception> otherParentExceptions = validateOtherParents(config);

    // Merge all into allExceptionsMap
    return allExceptionsMap;
}
```

This instantiates every Stage, Connector, and Indexer (triggering their SPEC validation) without actually running any connectors. It also validates top-level config blocks (publisher, runner, kafka, etc.) using a `validConfigProperties.conf` resource file.

## Default Legal Properties Per Component Type

Each component type gets automatic defaults via its `SpecBuilder` factory:

**Stages** (`SpecBuilder.stage()`):
- `name` (optional string)
- `class` (optional string)
- `conditions` (optional list with its own sub-spec: operator, valuesPath, values, fields)
- `conditionPolicy` (optional string)

**Connectors** (`SpecBuilder.connector()`):
- `name` (optional string)
- `class` (optional string)
- `pipeline` (optional string)
- `docIdPrefix` (optional string)
- `collapse` (optional boolean)

**Indexers** (`SpecBuilder.indexer()`):
- No defaults — each implementation defines its own properties

**FileHandlers** (`SpecBuilder.fileHandler()`):
- `class` (optional string)
- `docIdPrefix` (optional string)

These defaults mean a Stage implementation only needs to declare its own unique properties — the common ones are already covered.

## Example: A Complete SPEC Declaration

```java
public class CopyFields extends Stage {
    public static final Spec SPEC = SpecBuilder.stage()
        .requiredParent("fieldMapping", new TypeReference<Map<String, String>>(){})
        .optionalBoolean("updateMode")
        .build();

    public CopyFields(Config config) {
        super(config);  // Triggers validation
        // Safe to read config here — it's been validated
        this.fieldMapping = ...;
    }
}
```

If someone configures this stage with `feildMapping` (typo), they get:
```
Error with CopyFields Config: [Config contains unknown parent feildMapping, Required property fieldMapping is missing]
```
