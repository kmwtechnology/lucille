---
title: "Coding Conventions"
weight: 20
date: 2025-06-09
description: >
  Formatting, naming, class structure, Javadoc, and test conventions used in the Lucille codebase.
---

Lucille follows conventions broadly consistent with the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html), with some deviations noted below. There is no enforced checkstyle or formatter plugin in the build — conventions are maintained through code review.

---

## Formatting

**Indentation: 2 spaces.** All source files use 2-space indentation, consistent with Google style. No tabs.

**Line length:** No strict enforced limit, but lines are generally kept under 120 characters. Long lines (especially in constructors with many parameters or complex conditionals) are broken at natural points.

**Braces:** Opening braces on the same line as the statement (K&R style). Closing braces on their own line. Single-statement `if` blocks still use braces in most cases, though there are occasional instances of brace-less single-line `if` statements (a minor deviation from strict Google style).

```java
if (doc == null) {
  commitOffsetsAndRemoveCounter(null);
  continue;
}
```

**Blank lines:** One blank line between methods. One blank line between logical sections within a method. No multiple consecutive blank lines (though occasional double blanks appear in older code).

---

## Naming

**Classes:** PascalCase. Stage names describe what they do: `CopyFields`, `DeleteFields`, `RenameFields`, `ChunkText`, `EmitNestedChildren`. Connectors are named for their source: `FileConnector`, `DatabaseConnector`, `SolrConnector`. Indexers are named for their destination: `SolrIndexer`, `OpenSearchIndexer`, `PineconeIndexer`.

**Methods:** camelCase. Standard Java conventions. Getters use `get` prefix (`getId()`, `getString()`). Boolean getters use `is` or `has` prefix (`isDropped()`, `hasChildren()`, `has()`).

**Constants:** UPPER_SNAKE_CASE for `static final` fields that are true constants:

```java
public static final String ID_FIELD = "id";
public static final String RUNID_FIELD = "run_id";
public static final int DEFAULT_BATCH_SIZE = 100;
```

**Instance fields:** camelCase with `private final` where possible. Fields are declared at the top of the class, after constants and the logger:

```java
public class CopyFields extends Stage {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final Spec SPEC = SpecBuilder.stage()...;

  private final Map<String, Object> fieldMapping;
  private final UpdateMode updateMode;
  private final boolean isNested;
```

**Local variables:** camelCase. Short, descriptive names. Loop variables use standard conventions (`i`, `doc`, `node`, `entry`).

**Packages:** All under `com.kmwllc.lucille`. Sub-packages by function: `core`, `stage`, `connector`, `indexer`, `message`, `util`.

---

## Class Structure

The standard ordering within a class is:

1. `static final` constants (UPPER_SNAKE_CASE)
2. Logger declaration
3. `public static final Spec SPEC` declaration
4. Instance fields
5. Constructor
6. `start()` method (for Stages)
7. `processDocument()` or `execute()` (the main logic)
8. `stop()` or `close()` (cleanup)
9. Private helper methods

This ordering is consistent across Stages, Connectors, and Indexers.

---

## Logger Conventions

Two logger patterns are used:

**Standard class logger** — for operational messages:

```java
private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
```

or equivalently:

```java
private static final Logger log = LoggerFactory.getLogger(FileConnector.class);
```

Both patterns appear in the codebase. The `MethodHandles.lookup().lookupClass()` pattern avoids hardcoding the class name (useful when copy-pasting). The explicit class reference is more common in older code.

**DocLogger** — for per-document lifecycle events (used in core components, not in stages):

```java
private static final Logger docLogger = LoggerFactory.getLogger("com.kmwllc.lucille.core.DocLogger");
```

---

## Javadoc

**All public Stages, Connectors, and Indexers have class-level Javadoc** describing what the component does and listing its config parameters in a structured format:

```java
/**
 * Copies values from a source field to a destination field based on the field mapping.
 *
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>fieldMapping (Map&lt;String, Object&gt;) : A mapping of source field names to destination field names.</li>
 *   <li>updateMode (String, Optional) : Determines how writing will be handled. Defaults to 'overwrite'.</li>
 *   <li>isNested (Boolean, Optional) : Sets whether to treat field names as nested json paths. Defaults to false.</li>
 * </ul>
 */
```

This is a project convention: every Stage documents its config parameters in Javadoc using the `Config Parameters -` heading with a `<ul>` list. The format includes the parameter name, its type, whether it's optional, and a description.

**Interface methods have Javadoc.** The `Document`, `Publisher`, `Connector`, and Messenger interfaces are thoroughly documented with `@param`, `@return`, and behavioral descriptions.

**Private methods and implementation details** generally do not have Javadoc, though complex private methods sometimes have inline comments explaining the approach.

---

## Test Conventions

**Test framework: JUnit 4.** The project uses JUnit 4 (`org.junit.Test`, `@Before`, `@After`, etc.), not JUnit 5. This is a notable choice — most new Java projects use JUnit 5, but Lucille has remained on JUnit 4.

**Test class naming:** `{ClassName}Test`. Tests for `CopyFields` are in `CopyFieldsTest`. Tests for `SolrIndexer` are in `SolrIndexerTest`.

**Test method naming:** `test{Behavior}` in camelCase: `testCopyFieldsReplace()`, `testDeleteFields()`, `testGetLegalProperties()`. This follows the older JUnit 4 convention rather than the more descriptive `should_behavior_when_condition` style.

**Test config files:** Stored in `src/test/resources/` in a subdirectory matching the test class name: `src/test/resources/CopyFieldsTest/replace.conf`. Loaded via `StageFactory`:

```java
private StageFactory factory = StageFactory.of(CopyFields.class);
Stage stage = factory.get("CopyFieldsTest/config.conf");
```

**Assertions:** Use JUnit 4 static imports: `assertEquals`, `assertFalse`, `assertNull`, `assertThrows`. Assertion messages are used for complex assertions but often omitted for simple ones.

**Mocking:** Mockito is used for mocking external dependencies (HTTP clients, Kafka consumers, Solr clients). The core Lucille components are generally not mocked — test mode provides the full system running in-memory.

---

## Deviations from Google Java Style

**No enforced formatter.** There is no checkstyle plugin or auto-formatter in the build. Formatting is maintained by convention and code review. This means minor inconsistencies exist (e.g., occasional extra blank lines, slightly varying import ordering).

**Import ordering is not strictly enforced.** Google style prescribes a specific import order (static imports first, then by package). Lucille generally groups imports logically but does not enforce a strict order. IDE auto-import ordering varies between contributors.

**Wildcard imports appear occasionally.** Google style discourages wildcard imports (`import java.util.*`). Lucille generally uses explicit imports but wildcards appear in some files (e.g., `import java.util.*` in Runner.java).

**Line length is relaxed.** Google style enforces 100 characters. Lucille allows longer lines, particularly for log messages, exception messages, and method signatures with many parameters.

**JUnit 4 rather than JUnit 5.** Not a style deviation per se, but notable for developers expecting modern JUnit conventions.

**`private static final` logger named `log` rather than `logger`.** Google style doesn't prescribe logger naming, but many Java projects use `logger`. Lucille consistently uses `log`.

**Field declarations sometimes lack explicit access modifiers in interfaces.** Interface fields in `Document` are implicitly `public static final` without writing it out (e.g., `String ID_FIELD = "id"`). This is valid Java but some style guides prefer explicit modifiers.

---

## Other Conventions

**Config reading pattern:** Required parameters use direct Typesafe Config getters. Optional parameters use `ConfigUtils.getOrDefault`:

```java
this.sourceField = config.getString("sourceField");           // required
this.destField = ConfigUtils.getOrDefault(config, "destField", "output");  // optional with default
```

**Exception handling in stages:** Stages throw `StageException` for errors that should fail the current document. They do not catch and swallow exceptions silently. The Worker handles the exception and routes the document to a failure state.

**Immutable fields where possible:** Constructor-assigned fields are declared `final`. Mutable state is used only when necessary (e.g., counters, caches initialized in `start()`).

**No Lombok.** The project does not use Lombok or other annotation processors for boilerplate reduction. All getters, constructors, and builders are written explicitly.

**Java 17 features:** The project targets Java 17 and uses features like text blocks, `var` (sparingly), `List.of()`, `Map.of()`, and pattern matching in `instanceof` where appropriate.
