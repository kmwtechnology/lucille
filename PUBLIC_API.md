# Lucille Public API

This document states which Java packages make up Lucille's **public API** — the surface that the 1.0
binary-compatibility promise applies to, and that the `japicmp` check (see `lucille-parent/pom.xml`)
guards against accidental binary-incompatible change.

Everything not listed here is internal and may change in any release without notice. Do not build against internal packages.

## Scope and guarantee

- The guarantee is **binary compatibility** for the listed packages across patch and minor releases within the
  same major version (so code compiled against Lucille `1.x` keeps linking against later `1.y`).
- Breaking changes to these packages require a major-version bump.
- `japicmp` compares each release against the `1.0` baseline jar and fails the build on a binary-incompatible
  change to a public package. The baseline is the `1.0` tag; the check is added now and armed at release.

## Public packages

### lucille-core

These are the packages a pipeline/plugin author or an embedder compiles against:

- `com.kmwllc.lucille.core` — the central abstractions: `Document`, `Stage`, `Connector`, `Indexer`,
  `Publisher`, `Runner`, `RunResult`, and the exceptions/enums they expose.
- `com.kmwllc.lucille.core.spec` — the configuration `Spec` / `SpecBuilder` API used to declare and validate
  a component's config.
- `com.kmwllc.lucille.core.filehandler` — the `FileHandler` extension point.
- `com.kmwllc.lucille.connector` — base `Connector` implementations and shared connector support intended for
  reuse (e.g. `AbstractConnector`, `FileConnector`).
- `com.kmwllc.lucille.connector.storageclient` — the `StorageClient` abstraction used by file connectors.
- `com.kmwllc.lucille.indexer` — the `Indexer` base/`IndexerFactory` surface that backend plugins extend.
- `com.kmwllc.lucille.stage` — the built-in `Stage` implementations and the base types stage authors extend.
- `com.kmwllc.lucille.message` — the messaging abstractions (`*Messenger`) used to wire components together.
- `com.kmwllc.lucille.util` — the small set of stable helpers that public types expose in their signatures.

### Component plugins

The public surface of a component plugin is the set of `Indexer` / `Stage` / `Connector` classes it registers
(via `@IndexerType` / `@AutoService` or documented `class:` references) plus their public config. The plugins
are: lucille-solr, lucille-opensearch, lucille-elasticsearch, lucille-pinecone, lucille-weaviate,
lucille-parquet, lucille-tika, lucille-ocr, lucille-jlama, lucille-entity-extraction, and lucille-video.

## JPMS module names (deferred)

Stable `Automatic-Module-Name` manifest entries are **not shipped yet**. Several component plugins depend on
jars that are not JPMS-clean — Tika's parser jars split the package `org.apache.tika.detect`, and
Hadoop's/jlama's native jars derive automatic module names that are not valid Java identifiers
(`...3.25`, `jlama.native`). Until that dependency graph is JPMS-clean, Lucille cannot be cleanly consumed on
the module path, so declaring module names would be partly misleading (and it forces `maven-javadoc-plugin`
into module mode, which fails on those same dependencies). Giving the jars stable module names is deferred to
a follow-up. This does not affect classpath consumers, which is how Lucille is used today.

## Explicitly internal (not covered)

These packages are implementation detail and are excluded from the compatibility check:

- `com.kmwllc.lucille.doclet` — build-time Javadoc tooling.
- `com.kmwllc.lucille.connector.jdbc` — internal JDBC helpers for the DB connector.
- `com.kmwllc.lucille.stage.util`, `com.kmwllc.lucille.stage.dateformatters` — internal helpers for specific
  stages.
- Any package containing `.internal.` or `.impl.` in its name.
- Test sources, shaded `*-plugin.jar` artifacts, and the example modules.

## For embedders

- To embed Lucille as a library, depend on `lucille-core` and add your own SLF4J backend (or depend on
  `lucille-logging` to get Lucille's default logging). `lucille-core` exposes only `slf4j-api` as its logging
  dependency (see DEP-03).
- Build only against the public packages above. If you need something only available in an internal package,
  open an issue so it can be considered for promotion to the public API rather than depended on directly.
