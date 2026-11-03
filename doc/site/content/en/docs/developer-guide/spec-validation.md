---
title: "SPEC Validation System"
weight: 20
date: 2025-06-09
description: >
  How Lucille validates configuration before a run starts, catching typos and missing fields at startup.
---

## Overview

The SPEC system is Lucille's configuration validation framework. It catches config errors — typos, missing required fields, wrong types — before any processing starts. Every Stage, Connector, and Indexer declares a `public static final Spec SPEC` that defines what configuration properties it accepts.

For example, this is how a Stage would declare that its config must contain a string property called `foo`:

```java
public static final Spec SPEC = SpecBuilder.stage().requiredString("foo").build();
```

The philosophy: fail loudly at startup, not silently at runtime. If a user's config omits a required property or includes an unrecognized one (like a typo), Lucille reports the error before any documents are processed.

## What a SPEC Is

In practice, you'll create Specs using `SpecBuilder` (described below) and won't need to look into the `Spec` class itself. This section explains what's under the hood.

A `Spec` is an immutable set of `Property` declarations that describes the legal configuration for a component. Each property has:
- A **name** (the config key)
- A **required/optional** flag
- A **type** (string, number, boolean, list, or object/parent)

```java
public class Spec {
    private final List<Property> properties;   // in declaration order
}
```

`Spec` has a `validate` method that accepts a config and verifies that the config adheres to the spec by checking:
1. All required properties are present
2. All properties have the correct type
3. No unknown/unrecognized properties exist

## Why SPEC Is a `public static final` Field

You might wonder why Lucille uses a `public static final Spec SPEC` field rather than an abstract method that returns a Spec. The primary reason is **validation without full instantiation**. A Spec describes what configuration a class accepts — this is a property of the class itself, not of any particular instance. Making it a static field communicates this clearly and allows tooling or documentation generators to inspect a component's legal properties without constructing it.

Secondary reasons:

- **Immutability.** A `static final` field is initialized once at class-load time and cannot change. An instance method could theoretically return different values depending on constructor arguments, which would be confusing for a validation contract.
- **Minimal boilerplate.** Declaring a single field is simpler than implementing an accessor method in every subclass.
- **One source of truth.** Validation, the admin UI and documentation tooling all read the same field through `Spec.of(Class)`, so there is no overridable method that could make them disagree.

**The tradeoff: no compile-time enforcement.** The compiler does not force you to declare a `SPEC` field. If you forget it, you won't get a compile error — you'll get an `IllegalStateException` the first time the component is instantiated. This happens because the `Stage` base class constructor calls `Spec.of(getClass()).validate(config, ...)`, and `Spec.of` uses reflection to look up the `SPEC` field on your class. If the field is missing, the reflective lookup fails immediately with a clear error message:

```
IllegalStateException: Error accessing the Spec for com.example.MyStage. Lucille looks for a public static final Spec SPEC declared on the component class itself, not on its superclasses; a class extending another component must declare one, for example: public static final Spec SPEC = Parent.SPEC;
```

In practice, this is not a significant source of errors. Any unit test that instantiates your component — even the most trivial one — will trigger this failure immediately. You will never get as far as running a pipeline with a missing SPEC; the error is loud and obvious at the earliest possible moment.

**Declare SPEC on each class.** Java lets a subclass see its parent's static `SPEC`, but Lucille's reflective lookup (`Spec.of(Class)`) checks only the component's own class, not its superclasses. So a class that extends another concrete component (for example, `MyCopyFields extends CopyFields`) must declare its own `SPEC`, even if it accepts exactly the same properties: `public static final Spec SPEC = CopyFields.SPEC;`. Redeclaring it makes the choice of accepted properties explicit; if the parent later adds a property, the subclass doesn't silently start accepting a property it may not handle.

Subclassing a built-in component is possible but not recommended. The extension points are the base classes (`Stage`, `AbstractConnector`, `Indexer`, `BaseFileHandler`); a built-in component's internals and properties may change between releases.

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

// For arbitrary config blocks, including nested config objects — no defaults
SpecBuilder.withoutDefaults()
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
// With a Spec describing the object's structure; the first argument is the object's key
.requiredParent("name", myParentSpec)
.optionalParent("name", myParentSpec)

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

### Descriptions

A Spec declares names, types and whether each property is required; it does not carry descriptions. Describe each property in the component's Javadoc "Config Parameters" list instead (see [Javadoc Standards]({{< relref "docs/developer-guide/javadocs" >}})). The admin UI reads those descriptions from the generated Javadoc and merges them into the Spec's JSON.

## How Validation Is Triggered

### Stages

In the `Stage` base class constructor:

```java
public Stage(Config config) {
    this.name = ConfigUtils.getOrDefault(config, "name", null);
    this.config = config;

    // Validate using the subclass's SPEC
    Spec.of(getClass()).validate(config, getDisplayName());

    this.condition = getMergedConditions();
}
```

`Spec.of(Class)` reflectively reads the static `SPEC` field declared on the subclass. It is public, so tooling can read any component's Spec without instantiating it. The `SPEC` field is the only way for a component to declare its Spec.

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
        Spec.of(getClass()).validate(specificImplConfig, indexerConfigKey);
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

A parent Spec describes a nested config block. It is an ordinary Spec, usually built with `SpecBuilder.withoutDefaults()`; the block's key is given where the parent is added, like any other property:

```java
public static final Spec SPEC = SpecBuilder.connector()
    .requiredList("paths", new TypeReference<List<String>>(){})
    .optionalParent("fileOptions", SpecBuilder.withoutDefaults()
        .optionalBoolean("getFileContent", "handleArchivedFiles", "handleCompressedFiles")
        .optionalNumber("maxSizeBytes").build())
    .build();
```

When a parent spec is validated, it checks the properties within that nested config block. The parent's key (e.g., "fileOptions") becomes a legal property, and its children (e.g., "fileOptions.getFileContent") are validated against the parent spec's property set. Because the key is not part of the parent Spec, the same Spec can be used under more than one key.

To share a group of parents across components, put them in one Spec and add it with `include()`. `FileConnector.STORAGE_SPEC` declares the `gcp`, `azure`, and `s3` blocks and `storageClients`; every component that reads files through StorageClients includes it, so they all accept the same storage configuration:

```java
public static final Spec STORAGE_SPEC = SpecBuilder.withoutDefaults()
    .optionalParent("gcp", GCP_PARENT_SPEC)
    .optionalParent("azure", AZURE_PARENT_SPEC)
    .optionalParent("s3", S3_PARENT_SPEC)
    .optionalParent("storageClients", new TypeReference<Map<String, Map<String, Object>>>(){}).build();

// in a Stage that fetches files
public static final Spec SPEC = SpecBuilder.stage()
    .requiredString("dictPath")
    .include(FileConnector.STORAGE_SPEC).build();
```

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

`ConfigValidator.validate()` validates a whole config without running it. The `-validate` flag calls it, the Runner calls it before every run, and the API server calls it before storing a config:

```java
public static Map<String, List<Exception>> validate(Config config) throws Exception {
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

This instantiates every Stage, Connector, and Indexer (triggering their SPEC validation) without actually running any connectors. It also validates the top-level framework blocks (publisher, runner, kafka, etc.) against their Specs; see [Validating Top-Level Framework Blocks](#validating-top-level-framework-blocks).

---

## Default Legal Properties Per Component Type

Each component type gets automatic defaults via its `SpecBuilder` factory:

**Stages** (`SpecBuilder.stage()`):
- `name` (optional string)
- `class` (optional string)
- `enabled` (optional boolean)
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

---

## Validating Top-Level Framework Blocks

Connectors, Stages, and Indexers all declare a `SPEC` in code. When the `-validate` pass instantiates each component, the constructor triggers SPEC validation automatically — the code is the source of truth for what those components accept.

A Lucille config also contains top-level blocks like `publisher`, `runner`, `worker`, `kafka`, `zookeeper`, and `log` that the framework reads directly at runtime. Each of these also has a Spec, declared as a `SPEC` constant on the class that reads the block (`PublisherImpl.SPEC`, `Runner.SPEC`, `Worker.SPEC`, `KafkaUtils.SPEC`, `ZKRetryCounter.SPEC`, `LogUtils.SPEC`). `ConfigValidator.PARENTS_AND_SPECS` pairs each block name with its Spec:

```java
static final List<Pair<String, Spec>> PARENTS_AND_SPECS = List.of(
    Pair.of("publisher", PublisherImpl.SPEC),
    Pair.of("log", LogUtils.SPEC),
    Pair.of("runner", Runner.SPEC),
    Pair.of("kafka", KafkaUtils.SPEC),
    Pair.of("zookeeper", ZKRetryCounter.SPEC),
    Pair.of("worker", Worker.SPEC)
);
```

During the `-validate` pass (and the validation that runs before every run), `validateOtherParents` validates each of these blocks that is present in the config against its Spec, catching unknown properties, missing required ones, and wrong types, the same way component SPECs do. A block that is absent is skipped: all of these blocks are optional at the top level. Because they are ordinary Specs, they can describe nested objects with parent Specs, like any component Spec.

A Spec cannot restrict a property to a fixed set of values, so closed-set values in these blocks are checked separately in `validateOtherParents`. For example, an unknown `runner.metricsLoggingLevel` is reported there with the allowed values.

### Unknown top-level keys

Lucille does not reject top-level keys it doesn't read, because users can define their own top-level keys, for example to reuse a block through HOCON substitution (`csv_connector { ... }` referenced as `${csv_connector}`). Instead, validation logs a warning for an unknown top-level key that is within one or two edits of a key Lucille reads, such as `publsher` for `publisher` or `Indexer` for `indexer`, because that block's settings would otherwise be silently ignored. `ConfigValidator.KNOWN_TOP_LEVEL_KEYS` lists the keys it compares against.

### When to add a block

If you add a new top-level config block to Lucille core that is read directly by the framework (not by a component constructor), declare a `SPEC` for it on the class that reads it and add a pair to `ConfigValidator.PARENTS_AND_SPECS`, so the `-validate` pass can catch configuration errors in it and recognizes the block's name. A new backend block read by an indexer goes in `ConfigValidator.KNOWN_TOP_LEVEL_KEYS` instead. If the block belongs to a component that has a SPEC, no entry is needed — the SPEC handles it.
