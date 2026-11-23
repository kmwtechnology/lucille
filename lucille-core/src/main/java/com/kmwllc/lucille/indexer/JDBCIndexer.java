package com.kmwllc.lucille.indexer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerContext;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.IndexerFailure;
import com.kmwllc.lucille.core.IndexerRetryableException;
import com.kmwllc.lucille.core.PendingSend;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes documents to a relational database through JDBC, running a parameterized SQL statement for each document. The
 * statement is written by the user, so database-specific behavior (upserts, casts to JSON or vector types) is expressed
 * in SQL, for example <code>INSERT ... ON CONFLICT (id) DO UPDATE</code> with <code>?::jsonb</code> on PostgreSQL.
 * Additional parameters are made available by the {@link com.kmwllc.lucille.core.Indexer} abstract class.
 * <p>
 * Each send runs in one transaction. Documents are applied in order: runs of consecutive writes and deletes are each
 * sent as a JDBC batch. If a batch fails, the transaction is rolled back and its documents are sent again one at a time,
 * each in its own savepoint, so only the documents that fail are reported as failed. A failure with a retryable SQLState
 * rolls back the whole send instead, so it can be retried. The connection is opened when the indexer
 * connects or first sends documents, not when it is created, so validating a config does not contact the database. It
 * is checked before each send and reopened if it is no longer valid.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>driver (String, Required) : JDBC driver class name; must be on the classpath.</li>
 *   <li>connectionString (String, Required) : JDBC connection URL.</li>
 *   <li>jdbcUser (String, Required) : Username to access the database.</li>
 *   <li>jdbcPassword (String, Required) : Password to access the database.</li>
 *   <li>sql (String, Required) : Statement run for each document, with one <code>?</code> per entry in params; a mismatch
 *   fails at startup. A <code>?</code> in a string literal, quoted identifier or comment isn't counted, nor, with a
 *   PostgreSQL connection string, is <code>??</code>, that driver's escape for a literal <code>?</code>.</li>
 *   <li>params (List&lt;Map&gt;, Required) : How each <code>?</code> in sql is bound, in order. Each entry has:
 *     <ul>
 *       <li>field (String, Required) : Document field to bind. <code>id</code> binds the document's id, or its
 *       indexer.idOverrideField value when that is set.</li>
 *       <li>format (String, Optional) : <code>json</code> binds any value serialized as JSON (also the text format of
 *       pgvector, MySQL and Oracle vectors), <code>array</code> binds a list as a SQL array, <code>timestamp</code> binds
 *       an ISO-8601 instant, <code>bytes</code> binds base64-encoded binary data. Without a format, a single string,
 *       number or boolean is bound with its Java type; a value of the wrong kind for its format fails that document.</li>
 *       <li>arrayType (String, Optional) : Database element type of an array, such as <code>text</code> or
 *       <code>float8</code>. Required when format is <code>array</code>, and not allowed otherwise.</li>
 *       <li>sqlType (String, Optional) : {@link JDBCType} name, such as <code>VARCHAR</code>, used to bind NULL when the
 *       field is missing, for drivers that can't infer the type of a NULL parameter.</li>
 *     </ul>
 *   </li>
 *   <li>deleteSql (String, Optional) : Statement run for documents marked for deletion by indexer.deletionMarker.
 *   Required when indexer.deletionMarker is set, and not allowed otherwise.</li>
 *   <li>deleteParams (List&lt;Map&gt;, Optional) : How each <code>?</code> in deleteSql is bound, as in params. Defaults
 *   to the document id.</li>
 *   <li>validateSql (String, Optional) : Statement run when the indexer connects, which must succeed, for example a
 *   <code>SELECT ... LIMIT 0</code> on the target table.</li>
 *   <li>retryableSqlStates (List&lt;String&gt;, Optional) : SQLState prefixes of failures that may be transient, which are
 *   retried. Requires indexer.maxRetries. Defaults to <code>["08", "40"]</code> (connection exceptions and transaction
 *   rollbacks such as serialization failures and deadlocks).</li>
 * </ul>
 */
@AutoService(Indexer.class)
@IndexerType("JDBC")
public class JDBCIndexer extends Indexer {

  private static final Spec PARAM_SPEC = SpecBuilder.withoutDefaults()
      .requiredString("field")
      .optionalString("format", "arrayType", "sqlType").build();

  public static final Spec SPEC = SpecBuilder.indexer()
      .requiredString("driver", "connectionString", "jdbcUser", "jdbcPassword", "sql")
      .requiredList("params", PARAM_SPEC)
      .optionalString("deleteSql", "validateSql")
      .optionalList("deleteParams", PARAM_SPEC)
      .optionalList("retryableSqlStates", new TypeReference<List<String>>(){}).build();

  private static final Logger log = LoggerFactory.getLogger(JDBCIndexer.class);

  // converts a document's JSON values to the Java types setObject and createArrayOf accept
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final List<String> DEFAULT_RETRYABLE_SQL_STATES = List.of("08", "40");

  private static final int VALID_TIMEOUT_SECONDS = 5;

  private final String driver;
  private final String connectionString;
  private final String jdbcUser;
  private final String jdbcPassword;
  private final String sql;
  private final List<Param> params;
  private final String deleteSql;
  private final List<Param> deleteParams;
  private final String validateSql;
  private final List<String> retryableSqlStates;

  private Connection connection;

  public JDBCIndexer(Config config, IndexerContext context) {
    super(config, context);

    this.driver = config.getString("jdbc.driver");
    this.connectionString = config.getString("jdbc.connectionString");
    this.jdbcUser = config.getString("jdbc.jdbcUser");
    this.jdbcPassword = config.getString("jdbc.jdbcPassword");
    this.sql = config.getString("jdbc.sql");
    this.params = Param.listFromConfig(config.getConfigList("jdbc.params"));
    this.deleteSql = ConfigUtils.getStringOrDefault(config, "jdbc.deleteSql", null);
    this.deleteParams = config.hasPath("jdbc.deleteParams")
        ? Param.listFromConfig(config.getConfigList("jdbc.deleteParams"))
        : List.of(new Param(Document.ID_FIELD, null, null, null));
    this.validateSql = ConfigUtils.getStringOrDefault(config, "jdbc.validateSql", null);
    this.retryableSqlStates = config.hasPath("jdbc.retryableSqlStates")
        ? config.getStringList("jdbc.retryableSqlStates") : DEFAULT_RETRYABLE_SQL_STATES;

    if (getDeletionMarkerField() != null && deleteSql == null) {
      // a document marked for deletion would otherwise be written by sql
      throw new IllegalArgumentException("indexer.jdbc.deleteSql is required when indexer.deletionMarker is set.");
    }
    // settings that would otherwise be silently ignored
    if (getDeletionMarkerField() == null && (deleteSql != null || config.hasPath("jdbc.deleteParams"))) {
      throw new IllegalArgumentException("indexer.jdbc.deleteSql and deleteParams require indexer.deletionMarker to be set.");
    }
    if (config.hasPath("jdbc.retryableSqlStates") && !config.hasPath("maxRetries")) {
      throw new IllegalArgumentException("indexer.jdbc.retryableSqlStates requires indexer.maxRetries to be set.");
    }
    boolean postgres = connectionString.startsWith("jdbc:postgresql:");
    checkPlaceholders("sql", sql, "params", params, postgres);
    if (deleteSql != null) {
      checkPlaceholders("deleteSql", deleteSql, "deleteParams", deleteParams, postgres);
    }
  }

  // a mismatch would otherwise fail every send, or bind fields to the wrong columns
  private static void checkPlaceholders(String sqlName, String statement, String paramsName, List<Param> statementParams,
      boolean postgres) {
    int placeholders = countPlaceholders(statement, postgres);
    if (placeholders != statementParams.size()) {
      throw new IllegalArgumentException("indexer.jdbc." + sqlName + " has " + placeholders + " ? placeholders but indexer.jdbc."
          + paramsName + " has " + statementParams.size() + " entries.");
    }
  }

  /**
   * Counts JDBC parameter markers, skipping string literals, quoted identifiers and comments. On PostgreSQL "??" is the
   * driver's escape for a literal ? (such as the jsonb ? operator), so it isn't counted there. Dollar-quoted strings and
   * backslash escapes inside literals aren't recognized.
   */
  static int countPlaceholders(String statement, boolean postgres) {
    int count = 0;
    int i = 0;
    int length = statement.length();
    while (i < length) {
      char c = statement.charAt(i);
      if (c == '\'' || c == '"' || c == '`') {
        // a doubled quote inside a literal reads as two adjacent literals, which skip the same way
        int end = statement.indexOf(c, i + 1);
        i = end < 0 ? length : end + 1;
      } else if (statement.startsWith("--", i)) {
        int end = statement.indexOf('\n', i);
        i = end < 0 ? length : end + 1;
      } else if (statement.startsWith("/*", i)) {
        int end = statement.indexOf("*/", i + 2);
        i = end < 0 ? length : end + 2;
      } else if (postgres && statement.startsWith("??", i)) {
        i += 2;
      } else {
        if (c == '?') {
          count++;
        }
        i++;
      }
    }
    return count;
  }

  @Override
  protected String getIndexerConfigKey() { return "jdbc"; }

  // params decide what is written, so a field filter has nothing to apply to; a table override and delete-by-query would
  // need SQL the user hasn't written
  @Override
  protected Set<String> getUnsupportedSettings() {
    return Set.of("indexOverrideField", "includeFields", "excludeFields", "deleteByQuery");
  }

  private Connection getOrOpenConnection() throws SQLException {
    // isClosed() stays false when the server or a firewall dropped an idle connection, so check it is still usable
    if (connection != null && !connection.isValid(VALID_TIMEOUT_SECONDS)) {
      closeConnection();
    }
    if (connection == null) {
      try {
        Class.forName(driver);
      } catch (ClassNotFoundException e) {
        throw new SQLException("JDBC driver " + driver + " is not on the classpath.", e);
      }
      connection = DriverManager.getConnection(connectionString, jdbcUser, jdbcPassword);
      connection.setAutoCommit(false);
    }
    return connection;
  }

  @Override
  protected boolean checkConnection() {
    try {
      Connection conn = getOrOpenConnection();
      if (validateSql != null) {
        try (Statement statement = conn.createStatement()) {
          statement.execute(validateSql);
        } finally {
          conn.rollback();
        }
      }
      return conn.isValid(VALID_TIMEOUT_SECONDS);
    } catch (SQLException e) {
      log.error("Could not connect to {} or run validateSql.", connectionString, e);
      return false;
    }
  }

  @Override
  protected List<IndexerFailure> sendToIndex(PendingSend send) throws IndexerException {
    List<Document> documents = send.documents();
    try {
      return sendInBatches(documents);
    } catch (SQLException e) {
      rollbackQuietly();
      if (hasRetryableState(e) || !supportsSavepoints()) {
        throw toIndexerException(e);
      }
      // a batch failure doesn't reliably say which row caused it, so find the rows by sending each on its own
      log.warn("A batch of {} documents failed ({}); sending them one at a time to find the failing ones.",
          documents.size(), messageOf(e));
    }
    try {
      return sendOneByOne(documents);
    } catch (SQLException e) {
      rollbackQuietly();
      throw toIndexerException(e);
    }
  }

  private List<IndexerFailure> sendInBatches(List<Document> documents) throws SQLException {
    List<IndexerFailure> failures = new ArrayList<>();
    Connection conn = getOrOpenConnection();
    try (PreparedStatement write = conn.prepareStatement(sql);
        PreparedStatement delete = deleteSql == null ? null : conn.prepareStatement(deleteSql)) {
      PreparedStatement pending = null;
      for (Document doc : documents) {
        boolean isDelete = isMarkedForDeletion(doc);
        PreparedStatement statement = isDelete ? delete : write;
        // a write and a delete of the same id must apply in document order, so a run ends when the kind changes
        if (pending != null && pending != statement) {
          pending.executeBatch();
        }
        pending = statement;
        try {
          bind(statement, isDelete ? deleteParams : params, doc);
          statement.addBatch();
        } catch (RuntimeException | SQLException e) {
          failures.add(bindFailure(doc, e));
        }
      }
      if (pending != null) {
        pending.executeBatch();
      }
    }
    conn.commit();
    return failures;
  }

  /**
   * Sends each document in its own savepoint within one transaction, so a failing row is rolled back alone: on
   * PostgreSQL a failed statement otherwise aborts the whole transaction. A retryable failure is thrown, so the caller
   * rolls back everything and the send is retried as a whole.
   */
  private List<IndexerFailure> sendOneByOne(List<Document> documents) throws SQLException {
    List<IndexerFailure> failures = new ArrayList<>();
    Connection conn = getOrOpenConnection();
    try (PreparedStatement write = conn.prepareStatement(sql);
        PreparedStatement delete = deleteSql == null ? null : conn.prepareStatement(deleteSql)) {
      for (Document doc : documents) {
        boolean isDelete = isMarkedForDeletion(doc);
        PreparedStatement statement = isDelete ? delete : write;
        try {
          bind(statement, isDelete ? deleteParams : params, doc);
        } catch (RuntimeException | SQLException e) {
          failures.add(bindFailure(doc, e));
          continue;
        }
        Savepoint savepoint = conn.setSavepoint();
        try {
          statement.executeUpdate();
        } catch (SQLException e) {
          if (hasRetryableState(e)) {
            throw e;
          }
          conn.rollback(savepoint);
          failures.add(new IndexerFailure(doc, new IndexerException("Could not send document " + doc.getId() + ": "
              + messageOf(e), e)));
          continue;
        }
        releaseQuietly(conn, savepoint);
      }
    }
    conn.commit();
    return failures;
  }

  // a retryable SQLException while binding (the connection dropped) is rethrown, so the whole send is retried; any other
  // bind failure is the document's own
  private IndexerFailure bindFailure(Document doc, Exception e) throws SQLException {
    if (e instanceof SQLException sqlException && hasRetryableState(sqlException)) {
      throw sqlException;
    }
    return new IndexerFailure(doc, new IndexerException("Could not bind document " + doc.getId() + ": " + messageOf(e), e));
  }

  private boolean supportsSavepoints() {
    try {
      return getOrOpenConnection().getMetaData().supportsSavepoints();
    } catch (SQLException e) {
      return false;
    }
  }

  // some drivers (Oracle, SQL Server) don't support releasing a savepoint, and an unreleased one is discarded at commit
  // anyway, so a failure to release is not the document's
  private static void releaseQuietly(Connection conn, Savepoint savepoint) {
    try {
      conn.releaseSavepoint(savepoint);
    } catch (SQLException e) {
      log.debug("Could not release a savepoint.", e);
    }
  }

  private void bind(PreparedStatement statement, List<Param> statementParams, Document doc) throws SQLException {
    String destinationId = getDestinationId(doc);
    for (int i = 0; i < statementParams.size(); i++) {
      statementParams.get(i).bind(statement, i + 1, doc, destinationId);
    }
  }

  // a failure with a retryable SQLState may be transient; a connection exception also drops the connection so the
  // next attempt reconnects
  private IndexerException toIndexerException(SQLException e) {
    // the connection string is left out, since it can hold credentials and this message reaches FAIL events
    String message = "Error sending documents: " + messageOf(e);
    if (hasRetryableState(e)) {
      if (getSqlState(e).startsWith("08")) {
        closeConnection();
      }
      return new IndexerRetryableException(message, e);
    }
    return new IndexerException(message, e);
  }

  // some drivers throw without a message, such as MySQL's SQLFeatureNotSupportedException from createArrayOf
  private static String messageOf(Exception e) {
    return e.getMessage() != null ? e.getMessage() : e.toString();
  }

  private boolean hasRetryableState(SQLException e) {
    String state = getSqlState(e);
    return state != null && retryableSqlStates.stream().anyMatch(state::startsWith);
  }

  // drivers report a batch failure as a BatchUpdateException whose own SQLState may be null, with the cause chained
  private static String getSqlState(SQLException e) {
    for (SQLException current = e; current != null; current = current.getNextException()) {
      if (current.getSQLState() != null) {
        return current.getSQLState();
      }
    }
    return null;
  }

  private void rollbackQuietly() {
    try {
      if (connection != null && !connection.isClosed()) {
        connection.rollback();
      }
    } catch (SQLException e) {
      log.warn("Error rolling back after a failed send.", e);
    }
  }

  @Override
  public void closeConnection() {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        log.warn("Error closing JDBC connection.", e);
      }
      connection = null;
    }
  }

  private record Param(String field, String format, String arrayType, Integer sqlType) {

    private static List<Param> listFromConfig(List<? extends Config> configs) {
      List<Param> result = new ArrayList<>();
      for (Config c : configs) {
        String field = c.getString("field");
        String format = ConfigUtils.getStringOrDefault(c, "format", null);
        String arrayType = ConfigUtils.getStringOrDefault(c, "arrayType", null);
        String sqlTypeName = ConfigUtils.getStringOrDefault(c, "sqlType", null);
        if (format != null && !Set.of("json", "array", "timestamp", "bytes").contains(format)) {
          throw new IllegalArgumentException("Unknown format \"" + format + "\" for param " + field
              + "; expected json, array, timestamp or bytes.");
        }
        if ("array".equals(format) != (arrayType != null)) {
          throw new IllegalArgumentException("Param " + field + " must set arrayType exactly when its format is array.");
        }
        result.add(new Param(field, format, arrayType, toSqlType(field, sqlTypeName)));
      }
      return result;
    }

    private static Integer toSqlType(String field, String sqlTypeName) {
      if (sqlTypeName == null) {
        return null;
      }
      try {
        return JDBCType.valueOf(sqlTypeName.toUpperCase(Locale.ROOT)).getVendorTypeNumber();
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Param " + field + " has sqlType " + sqlTypeName
            + ", which is not a java.sql.JDBCType name such as VARCHAR or INTEGER.", e);
      }
    }

    private void bind(PreparedStatement statement, int index, Document doc, String destinationId) throws SQLException {
      if (Document.ID_FIELD.equals(field)) {
        statement.setString(index, destinationId);
        return;
      }
      if (!doc.hasNonNull(field)) {
        statement.setNull(index, sqlType == null ? Types.NULL : sqlType);
        return;
      }
      if (format == null) {
        JsonNode node = doc.getJson(field);
        if (node.isContainerNode()) {
          throw new IllegalArgumentException("field " + field + " is " + (node.isArray() ? "multi-valued" : "an object")
              + "; set format to json or array.");
        }
        statement.setObject(index, MAPPER.convertValue(node, Object.class));
        return;
      }
      switch (format) {
        case "json" -> statement.setString(index, doc.getJson(field).toString());
        case "array" -> {
          JsonNode node = doc.getJson(field);
          Object[] values = new Object[node.isArray() ? node.size() : 1];
          for (int i = 0; i < values.length; i++) {
            values[i] = MAPPER.convertValue(node.isArray() ? node.get(i) : node, Object.class);
          }
          statement.setArray(index, statement.getConnection().createArrayOf(arrayType, values));
        }
        case "timestamp" -> {
          Instant instant = doc.getInstant(field);
          // without a calendar the driver writes the JVM's local time into a column without a time zone
          statement.setTimestamp(index, instant == null ? null : Timestamp.from(instant),
              Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC)));
        }
        case "bytes" -> {
          // getBytes returns null for a value that isn't base64, which would otherwise be written as NULL
          byte[] bytes = doc.getBytes(field);
          if (bytes == null) {
            throw new IllegalArgumentException("field " + field + " does not hold base64-encoded bytes.");
          }
          statement.setBytes(index, bytes);
        }
        default -> throw new IllegalStateException("Unknown format " + format);
      }
    }
  }
}
