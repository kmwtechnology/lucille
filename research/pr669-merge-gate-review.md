# Merge-gate review: PR #669 (LC-1005 smoketests) merged onto main af96a8715

**Verdict: RED — do not merge until an upgrade note is added (one finding; the fix is documentation, not code).**

## Blocking findings

1. **Silent loss of incremental state on upgrade for every existing FileConnector `state` deployment that uses the default table name.**
   File: `lucille-core/src/main/java/com/kmwllc/lucille/connector/FileConnectorStateManager.java`, constructor (line ~117, `toSqlTableName(connectorName)`) and `init()` (lines ~273-290).
   What breaks: the state manager has shipped since 0.10.0 and, by default, stored state in a quoted table named after the connector (for example `"FILE"` or `"FILE-CONNECTOR-1"`, column `last_published TIMESTAMP WITH TIME ZONE`). After this PR the default table is `STATE_FILE_<8-hex-hash>`. On the first run after upgrading, `init()` looks the new name up, does not find it, creates an empty table, and leaves the old one untouched. The `checkTimestampSchema` guard only fires when the *new* name already exists, so nothing is logged. Consequences for an operator: every file is treated as never published (full republish; `lastPublishedCutoff` becomes a no-op), no tombstones are sent for files that disappeared since the last run (their rows live in the orphaned table), and the old H2/Postgres table stays on disk forever. The same thing happens on PostgreSQL for operators who set an explicit `state.tableName`: the old code created the quoted upper-case table `"MYSTATE"`, the new code addresses unquoted `MYSTATE`, which Postgres folds to `mystate`, so a fresh table is created and the old one orphaned. (On H2 with an explicit `tableName` the behaviour is acceptable: the existing table is found and `init()` fails loudly with "created by an older version of Lucille".)
   No release note or upgrade note accompanies this. The only hint in the diff is the sentence "Tables created by an earlier version of Lucille are not changed." in `doc/site/content/en/docs/ingest-design/connectors/file_connector.md`, inside a paragraph about MySQL collation, which does not say that state is effectively reset.
   Minimal fix: add an explicit upgrade note (in `file_connector.md` under the state section and in the release notes for the next release) stating that the state table name and `last_published` type changed, that an existing state database is not migrated, that the first run after upgrade publishes every file and sends no tombstones for files deleted in between, and that the old table (connector name upper-cased, quoted) can be dropped. Optional hardening if a code change is preferred: in `init()`, when the generated table does not exist, also probe `metadata.getTables(null, null, escapeSearchPattern(connectorName.toUpperCase(), metadata), null)` and fail (or at least `log.warn`) if a legacy table is found.

## Non-blocking observations (considered, not counted)

- `KafkaUtils`: `eventTopicReplicationFactor` defaults to 1 and `min.insync.replicas` is only set when configured, so existing single-broker behaviour is unchanged; both keys are added to the Spec so validation passes. The `(short)` cast has no range check, but a nonsense value just fails topic creation with a broker error.
- `ElasticsearchUtils.disableContentCompression()`: this only stops Apache HttpClient 5 from adding `Accept-Encoding: gzip` and auto-decoding; Rest5Client requests and decodes gzip itself, and the new unit test (`testClientDecodesGzipResponse`, local gzip HTTP server) passes. Non-gzip responses and ES 8 servers are unaffected because a server only gzips when the client asks for it.
- `lucille-core/pom.xml`: postgresql 42.6.0 and mysql-connector-j 9.1.0 are `test` scope only; no runtime classpath change. It does break `mvn -o` for anyone who has never downloaded them, which is expected.
- Connector-name duplicate detection: no production change in `Connector`/`Runner`/`ConfigValidator`; the PR only adds a test asserting the existing exact-match behaviour (`file` and `File` are distinct). The hash suffix on the generated table name keeps their state tables distinct on case-insensitive H2 as well.
- `traversalInstant` is truncated to microseconds and stored as `BIGINT` epoch micros; read and write are symmetric, so no drift. `getLong` + `wasNull` handles the NULL-for-directories case.
- MySQL `utf8mb4_0900_bin` requires MySQL 8.0+; documented in `file_connector.md`.
- `application-example.conf`: new keys commented out, illustrative only.

## Test results (Temurin 21.0.12)

- `mvn -pl lucille-core,lucille-plugins/lucille-elasticsearch -am -DskipTests install` on the merged state: BUILD SUCCESS (offline mode fails only because the two new test-scoped JDBC drivers were not cached; online resolution succeeded). No references to symbols removed by main.
- `mvn -pl lucille-core test -Dtest='FileConnectorStateManager*,TraversalState*,FileConnector*Test,KafkaUtils*,Runner*Test,AbstractConnectorTest'`: 214 run, 0 failures, 0 errors, 8 skipped (the 8 skipped are `FileConnectorStateManagerDatabaseIT` cases that need a Postgres/MySQL URL). BUILD SUCCESS.
- `mvn -pl lucille-plugins/lucille-elasticsearch test -Dtest='Elasticsearch*Test'`: 84 run, 0 failures, 0 errors. BUILD SUCCESS.
- Smoke tests not run (Docker/network).
