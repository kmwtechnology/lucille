package com.kmwllc.lucille.indexer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.connector.jdbc.DBTestHelper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.IndexerContext;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.IndexerFailure;
import com.kmwllc.lucille.core.IndexerRetryableException;
import com.kmwllc.lucille.core.PendingSend;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.h2.api.Trigger;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

public class DatabaseIndexerTest {

  @Rule
  public final DBTestHelper dbHelper = new DBTestHelper("DatabaseIndexerTest/start.sql");

  private final Config config = ConfigFactory.parseResourcesAnySyntax("DatabaseIndexerTest/config.conf").getConfig("indexer");

  // closed after each test, since DBTestHelper fails a test that leaves a connection open
  private final List<DatabaseIndexer> indexers = new ArrayList<>();

  @After
  public void closeIndexers() {
    indexers.forEach(DatabaseIndexer::closeConnection);
  }

  private DatabaseIndexer newIndexer(Config indexerConfig) {
    return newIndexer(indexerConfig, new TestMessenger(), false);
  }

  private DatabaseIndexer newIndexer(Config indexerConfig, TestMessenger messenger, boolean bypass) {
    DatabaseIndexer indexer = new DatabaseIndexer(indexerConfig, new IndexerContext(messenger, bypass, "testing", null));
    indexers.add(indexer);
    return indexer;
  }

  private static Config with(Config indexerConfig, String path, Object value) {
    return indexerConfig.withValue(path, ConfigValueFactory.fromAnyRef(value));
  }

  @Test
  public void testWritesEachFormat() throws Exception {
    Document doc = Document.create("doc1");
    doc.setField("title", "first");
    doc.setField("n", 5);
    doc.setField("meta", new ObjectMapper().readTree("{\"k\":[1,2]}"));
    doc.addToField("tags", "x");
    doc.addToField("tags", "y");
    doc.setField("ts", Instant.parse("2026-01-02T03:04:05Z"));

    List<IndexerFailure> failures = newIndexer(config).sendToIndex(PendingSend.of(List.of(doc)));

    assertEquals(List.of(), failures);
    try (Connection conn = DriverManager.getConnection(DBTestHelper.CONNECTION_STRING);
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM docs WHERE id = 'doc1'")) {
      assertTrue(rs.next());
      assertEquals("first", rs.getString("title"));
      assertEquals(5, rs.getInt("n"));
      assertEquals("{\"k\":[1,2]}", rs.getString("meta"));
      assertArrayEquals(new Object[] {"x", "y"}, (Object[]) rs.getArray("tags").getArray());
      // written in UTC, whatever the JVM's time zone
      assertEquals(Instant.parse("2026-01-02T03:04:05Z"),
          rs.getTimestamp("ts", Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC))).toInstant());
    }
  }

  @Test
  public void testMissingFieldBindsNull() throws Exception {
    Document doc = Document.create("existing");
    doc.setField("n", 3);

    newIndexer(config).sendToIndex(PendingSend.of(List.of(doc)));

    // the MERGE replaces the row, so the title it had is overwritten with NULL
    assertEquals(List.of(Map.of("id", "existing", "n", "3")), rows("SELECT id, title, n FROM docs WHERE id = 'existing'"));
  }

  @Test
  public void testIdOverride() throws Exception {
    Document doc = Document.create("internal");
    doc.setField("otherId", "external");
    doc.setField("title", "overridden");

    newIndexer(with(config, "idOverrideField", "otherId")).sendToIndex(PendingSend.of(List.of(doc)));

    assertEquals(List.of(Map.of("id", "external", "title", "overridden")),
        rows("SELECT id, title FROM docs WHERE title = 'overridden'"));
  }

  @Test
  public void testDeletesApplyInDocumentOrder() throws Exception {
    Document deleteExisting = deletion("existing");
    Document write = Document.create("a");
    write.setField("title", "written then deleted");
    Document deleteWrite = deletion("a");
    Document deleteMissing = deletion("b");
    Document writeAfterDelete = Document.create("b");
    writeAfterDelete.setField("title", "deleted then written");

    List<IndexerFailure> failures = newIndexer(config)
        .sendToIndex(PendingSend.of(List.of(deleteExisting, write, deleteWrite, deleteMissing, writeAfterDelete)));

    assertEquals(List.of(), failures);
    assertEquals(List.of(Map.of("id", "b", "title", "deleted then written")), rows("SELECT id, title FROM docs"));
  }

  @Test
  public void testBindFailureFailsOnlyThatDocument() throws Exception {
    Document good = Document.create("good");
    Document multiValued = Document.create("multi");
    multiValued.addToField("title", "one");
    multiValued.addToField("title", "two");

    List<IndexerFailure> failures = newIndexer(config).sendToIndex(PendingSend.of(List.of(good, multiValued)));

    assertEquals(1, failures.size());
    assertSame(multiValued, failures.get(0).document());
    assertTrue(failures.get(0).cause().getMessage().contains("multi-valued"));
    assertEquals(List.of(Map.of("id", "existing"), Map.of("id", "good")), rows("SELECT id FROM docs ORDER BY id"));
  }

  @Test
  public void testFailingRowsAreIsolated() throws Exception {
    DatabaseIndexer indexer = newIndexer(with(config, "database.sql",
        "INSERT INTO docs (id, title, n, meta, tags, ts) VALUES (?, ?, ?, ? FORMAT JSON, ?, ?)"));
    Document first = Document.create("first");
    Document duplicate = Document.create("existing");
    Document negative = Document.create("negative");
    negative.setField("n", -1);
    Document written = Document.create("written");
    Document deleteWritten = deletion("written");
    Document last = Document.create("last");

    List<IndexerFailure> failures = indexer.sendToIndex(
        PendingSend.of(List.of(first, duplicate, negative, written, deleteWritten, last)));

    assertEquals(2, failures.size());
    assertSame(duplicate, failures.get(0).document());
    assertSame(negative, failures.get(1).document());
    // a constraint violation won't succeed on a retry
    failures.forEach(f -> assertFalse(f.cause() instanceof IndexerRetryableException));
    assertEquals(List.of(Map.of("id", "existing"), Map.of("id", "first"), Map.of("id", "last")),
        rows("SELECT id FROM docs ORDER BY id"));
  }

  /**
   * Fails writes of a document titled "transient" with a serialization failure: after letting passesLeft writes through,
   * while failuresLeft is positive.
   */
  public static class TransientFailureTrigger implements Trigger {
    static final AtomicInteger passesLeft = new AtomicInteger();
    static final AtomicInteger failuresLeft = new AtomicInteger();

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
      if (newRow == null || !"transient".equals(newRow[1]) || passesLeft.getAndDecrement() > 0) {
        return;
      }
      if (failuresLeft.getAndDecrement() > 0) {
        throw new SQLException("simulated serialization failure", "40001");
      }
    }
  }

  private void addTransientFailureTrigger(int passes, int failures) throws SQLException {
    TransientFailureTrigger.passesLeft.set(passes);
    TransientFailureTrigger.failuresLeft.set(failures);
    try (Connection conn = DriverManager.getConnection(DBTestHelper.CONNECTION_STRING)) {
      conn.createStatement().execute("CREATE TRIGGER transient_failure BEFORE INSERT, UPDATE ON docs FOR EACH ROW CALL '"
          + TransientFailureTrigger.class.getName() + "'");
    }
  }

  @Test
  public void testRetryableFailureWhileIsolatingRowsRollsBackTheWholeSend() throws Exception {
    // transientDoc's write succeeds in the batch, and fails once its documents are sent one at a time
    addTransientFailureTrigger(1, Integer.MAX_VALUE);
    Document first = Document.create("first");
    Document transientDoc = Document.create("transientDoc");
    transientDoc.setField("title", "transient");
    // fails the batch with a constraint violation, so the documents are then sent one at a time
    Document negative = Document.create("negative");
    negative.setField("n", -1);

    DatabaseIndexer indexer = newIndexer(config);
    IndexerException e = assertThrows(IndexerException.class,
        () -> indexer.sendToIndex(PendingSend.of(List.of(first, transientDoc, negative))));

    assertTrue(e instanceof IndexerRetryableException);
    // thrown by a single-row send, not by the batch
    assertFalse(e.getCause() instanceof BatchUpdateException);
    // a send that throws must have applied nothing, so first and second were rolled back too
    assertEquals(List.of(Map.of("id", "existing")), rows("SELECT id FROM docs ORDER BY id"));
  }

  @Test
  public void testRetryableFailureIsRetried() throws Exception {
    addTransientFailureTrigger(0, 1);
    TestMessenger messenger = new TestMessenger();
    DatabaseIndexer indexer = newIndexer(with(with(config, "maxRetries", 2), "retryWaitDurationMs", 10), messenger, false);
    Document doc = Document.create("doc1", "run1");
    doc.setField("title", "transient");
    messenger.sendForIndexing(doc);

    indexer.run(1);

    assertEquals(List.of(Event.Type.FINISH), messenger.getSentEvents().stream().map(Event::getType).toList());
    assertEquals(0, Math.max(0, TransientFailureTrigger.failuresLeft.get()));
    assertEquals(List.of(Map.of("id", "doc1", "title", "transient")), rows("SELECT id, title FROM docs WHERE id = 'doc1'"));
  }

  @Test
  public void testRetryableFailureFailsDocumentsWithoutRetries() throws Exception {
    addTransientFailureTrigger(0, 1);
    TestMessenger messenger = new TestMessenger();
    DatabaseIndexer indexer = newIndexer(config, messenger, false);
    Document doc = Document.create("doc1", "run1");
    doc.setField("title", "transient");
    messenger.sendForIndexing(doc);

    indexer.run(1);

    assertEquals(List.of(Event.Type.FAIL), messenger.getSentEvents().stream().map(Event::getType).toList());
  }

  @Test
  public void testReconnectsAfterTheServerClosesTheConnection() throws Exception {
    List<Integer> sessionsBefore = sessionIds();
    DatabaseIndexer indexer = newIndexer(config);
    indexer.sendToIndex(PendingSend.of(List.of(Document.create("before"))));
    List<Integer> indexerSessions = sessionIds();
    indexerSessions.removeAll(sessionsBefore);
    assertEquals(1, indexerSessions.size());
    try (Connection conn = DriverManager.getConnection(DBTestHelper.CONNECTION_STRING)) {
      conn.createStatement().execute("CALL ABORT_SESSION(" + indexerSessions.get(0) + ")");
    }

    // no retries configured, so this only succeeds if the indexer reconnects before sending
    List<IndexerFailure> failures = indexer.sendToIndex(PendingSend.of(List.of(Document.create("after"))));

    assertEquals(List.of(), failures);
    assertEquals(List.of(Map.of("id", "after"), Map.of("id", "before"), Map.of("id", "existing")),
        rows("SELECT id FROM docs ORDER BY id"));
  }

  // the open sessions other than the one running this query
  private static List<Integer> sessionIds() throws SQLException {
    List<Integer> ids = new ArrayList<>();
    try (Connection conn = DriverManager.getConnection(DBTestHelper.CONNECTION_STRING);
        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT SESSION_ID FROM INFORMATION_SCHEMA.SESSIONS WHERE SESSION_ID <> SESSION_ID()")) {
      while (rs.next()) {
        ids.add(rs.getInt(1));
      }
    }
    return ids;
  }

  @Test
  public void testConnectionFailureIsRetryable() {
    DatabaseIndexer indexer = newIndexer(with(config, "database.connectionString", "jdbc:nonexistent:db"));

    IndexerException e = assertThrows(IndexerException.class,
        () -> indexer.sendToIndex(PendingSend.of(List.of(Document.create("doc1")))));

    assertTrue(e instanceof IndexerRetryableException);
  }

  @Test
  public void testRetryableSqlStatesCanBeChanged() {
    DatabaseIndexer indexer = newIndexer(with(with(with(config, "database.connectionString", "jdbc:nonexistent:db"),
        "database.retryableSqlStates", List.of("40")), "maxRetries", 1));

    IndexerException e = assertThrows(IndexerException.class,
        () -> indexer.sendToIndex(PendingSend.of(List.of(Document.create("doc1")))));

    assertFalse(e instanceof IndexerRetryableException);
  }

  @Test
  public void testRunSendsFinishAndFailEvents() throws Exception {
    TestMessenger messenger = new TestMessenger();
    DatabaseIndexer indexer = newIndexer(config, messenger, false);
    Document good = Document.create("good", "run1");
    Document negative = Document.create("negative", "run1");
    negative.setField("n", -1);
    messenger.sendForIndexing(good);
    messenger.sendForIndexing(negative);

    indexer.run(2);

    // the base class sends a batch's FAIL events before its FINISH events
    Map<String, Event.Type> typesById = messenger.getSentEvents().stream()
        .collect(Collectors.toMap(Event::getDocumentId, Event::getType));
    assertEquals(Map.of("good", Event.Type.FINISH, "negative", Event.Type.FAIL), typesById);
  }

  @Test
  public void testCheckConnection() {
    assertTrue(newIndexer(config).validateConnection());
    assertFalse(newIndexer(with(config, "database.validationSql", "SELECT id FROM missing_table")).validateConnection());
    assertFalse(newIndexer(with(config, "database.connectionString", "jdbc:nonexistent:db")).validateConnection());
  }

  @Test
  public void testBypassDoesNotConnect() {
    DatabaseIndexer indexer = newIndexer(config, new TestMessenger(), true);

    assertTrue(indexer.validateConnection());
    assertEquals(1, dbHelper.countConnections());
  }

  @Test
  public void testDeletionMarkerRequiresDeleteSql() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> newIndexer(config.withoutPath("database.deleteSql")));

    assertTrue(e.getMessage().contains("deleteSql"));
  }

  @Test
  public void testInvalidParamsRejected() {
    assertThrows(IllegalArgumentException.class, () -> newIndexer(with(config, "database.params",
        List.of(Map.of("field", "id"), Map.of("field", "tags", "format", "csv")))));
    assertThrows(IllegalArgumentException.class, () -> newIndexer(with(config, "database.params",
        List.of(Map.of("field", "id"), Map.of("field", "tags", "format", "array")))));
    assertThrows(IllegalArgumentException.class, () -> newIndexer(with(config, "database.params",
        List.of(Map.of("field", "id"), Map.of("field", "n", "sqlType", "NOT_A_TYPE")))));
    assertThrows(IllegalArgumentException.class, () -> newIndexer(with(config, "database.params",
        List.of(Map.of("field", "id"), Map.of("field", "title", "arrayType", "text")))));
  }

  @Test
  public void testSqlTypeIsCaseInsensitive() {
    newIndexer(with(with(config, "database.sql", "MERGE INTO docs (id, n) KEY (id) VALUES (?, ?)"), "database.params",
        List.of(Map.of("field", "id"), Map.of("field", "n", "sqlType", "integer"))));
  }

  @Test
  public void testValuesOfTheWrongKindFailOnlyThatDocument() throws Exception {
    DatabaseIndexer indexer = newIndexer(with(with(config, "database.sql", "MERGE INTO docs (id, title, n) KEY (id) VALUES (?, ?, ?)"),
        "database.params", List.of(Map.of("field", "id"), Map.of("field", "title"), Map.of("field", "n", "format", "bytes"))));
    Document object = Document.create("object");
    object.setField("title", new ObjectMapper().readTree("{\"k\": 1}"));
    Document notBase64 = Document.create("notBase64");
    notBase64.setField("n", "not base64!");
    Document good = Document.create("good");

    List<IndexerFailure> failures = indexer.sendToIndex(PendingSend.of(List.of(object, notBase64, good)));

    assertEquals(2, failures.size());
    assertSame(object, failures.get(0).document());
    assertTrue(failures.get(0).cause().getMessage().contains("an object"));
    assertSame(notBase64, failures.get(1).document());
    assertTrue(failures.get(1).cause().getMessage().contains("base64"));
    assertEquals(List.of(Map.of("id", "existing"), Map.of("id", "good")), rows("SELECT id FROM docs ORDER BY id"));
  }

  @Test
  public void testDeleteSettingsRequireDeletionMarker() {
    assertThrows(IllegalArgumentException.class, () -> newIndexer(config.withoutPath("deletionMarker")));
    assertThrows(IllegalArgumentException.class, () -> newIndexer(config.withoutPath("deletionMarker")
        .withoutPath("database.deleteSql").withValue("database.deleteParams", ConfigValueFactory.fromAnyRef(
            List.of(Map.of("field", "id"))))));
  }

  @Test
  public void testRetryableSqlStatesRequireMaxRetries() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> newIndexer(with(config, "database.retryableSqlStates", List.of("40"))));

    assertTrue(e.getMessage().contains("maxRetries"));
  }

  @Test
  public void testPlaceholderCountMustMatchParams() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> newIndexer(with(config, "database.sql",
        "MERGE INTO docs (id, title) KEY (id) VALUES (?, ?)")));
    assertTrue(e.getMessage().contains("2 ? placeholders but indexer.database.params has 6 entries"));

    e = assertThrows(IllegalArgumentException.class, () -> newIndexer(with(config, "database.deleteSql",
        "DELETE FROM docs WHERE id = ? AND title = ?")));
    assertTrue(e.getMessage().contains("indexer.database.deleteSql has 2"));
  }

  @Test
  public void testCountPlaceholders() {
    assertEquals(3, DatabaseIndexer.countPlaceholders("INSERT INTO t (a, b, c) VALUES (?, ?::jsonb, ?::vector)", true));
    assertEquals(1, DatabaseIndexer.countPlaceholders("UPDATE t SET note = 'why?', \"odd?col\" = ? WHERE `x?` = 'it''s?'",
        false));
    assertEquals(1, DatabaseIndexer.countPlaceholders("SELECT ? -- what?\nFROM t /* really? */", false));
    // ?? escapes a literal ? only in the PostgreSQL driver
    assertEquals(1, DatabaseIndexer.countPlaceholders("SELECT * FROM t WHERE meta ?? 'key' AND id = ?", true));
    assertEquals(3, DatabaseIndexer.countPlaceholders("SELECT * FROM t WHERE meta ?? 'key' AND id = ?", false));
    assertEquals(0, DatabaseIndexer.countPlaceholders("SELECT 'unterminated ?", false));
  }

  @Test
  public void testUnsupportedSettingsRejected() {
    assertThrows(IllegalArgumentException.class, () -> newIndexer(with(config, "includeFields", List.of("title"))));
    assertThrows(IllegalArgumentException.class, () -> newIndexer(with(config, "indexOverrideField", "table")));
  }

  private static Document deletion(String id) {
    Document doc = Document.create(id);
    doc.setField("deleted", "true");
    return doc;
  }

  // each row as column name (lower case) to value, leaving out NULLs
  private static List<Map<String, String>> rows(String query) throws SQLException {
    List<Map<String, String>> rows = new ArrayList<>();
    try (Connection conn = DriverManager.getConnection(DBTestHelper.CONNECTION_STRING);
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(query)) {
      int columns = rs.getMetaData().getColumnCount();
      while (rs.next()) {
        Map<String, String> row = new HashMap<>();
        for (int i = 1; i <= columns; i++) {
          if (rs.getString(i) != null) {
            row.put(rs.getMetaData().getColumnLabel(i).toLowerCase(), rs.getString(i));
          }
        }
        rows.add(row);
      }
    }
    return rows;
  }
}
