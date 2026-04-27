package com.kmwllc.lucille.postgres.indexer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.pgvector.PGvector;
import com.typesafe.config.Config;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes Documents to a PostgreSQL table via JDBC, with optional pgvector support.
 *
 * <p>Config (under the <code>postgres</code> key):
 * <ul>
 *   <li><b>jdbcUrl</b> (String, required): JDBC URL, e.g. {@code jdbc:postgresql://host:5432/db}</li>
 *   <li><b>user</b> (String, required)</li>
 *   <li><b>password</b> (String, required — use HOCON env substitution, e.g. {@code ${?PG_PASSWORD}})</li>
 *   <li><b>table</b> (String, required): target table name (will be quoted).</li>
 *   <li><b>idColumn</b> (String, optional, default {@code "id"}): column that stores the document id.</li>
 *   <li><b>upsert</b> (Boolean, optional, default {@code true}): generate
 *       {@code INSERT … ON CONFLICT (idColumn) DO UPDATE}.</li>
 *   <li><b>columns</b> (List&lt;Object&gt;, required): column mappings. Each entry:
 *     <ul>
 *       <li><b>field</b> (String, required): source Document field name</li>
 *       <li><b>column</b> (String, required): destination column name</li>
 *       <li><b>type</b> (String, required): one of
 *           {@code text, varchar, int, bigint, double, float, boolean, timestamp,
 *           jsonb, vector, text[], int[], bigint[], double[]}</li>
 *       <li><b>dim</b> (Int, optional): required when {@code type=vector}, the vector dimension</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * <p>The id column is always written from {@link Document#getId()}. It should <b>not</b> be listed
 * in {@code columns}.
 */
public class PostgresIndexer extends Indexer {

  public static final Spec SPEC = SpecBuilder.indexer()
      .requiredString("jdbcUrl", "user", "password", "table")
      .optionalString("idColumn")
      .optionalBoolean("upsert")
      .requiredList("columns", new TypeReference<List<Map<String, Object>>>() {})
      .build();

  private static final Logger log = LoggerFactory.getLogger(PostgresIndexer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String jdbcUrl;
  private final String user;
  private final String password;
  private final String table;
  private final String idColumn;
  private final boolean upsert;
  private final List<ColumnMapping> columns;
  private final String insertSql;

  private Connection connection;

  public PostgresIndexer(Config config, IndexerMessenger messenger, boolean bypass,
      String metricsPrefix, String localRunId) throws IndexerException {
    super(config, messenger, bypass, metricsPrefix, localRunId);

    this.jdbcUrl = config.getString("postgres.jdbcUrl");
    this.user = config.getString("postgres.user");
    this.password = config.getString("postgres.password");
    this.table = config.getString("postgres.table");
    this.idColumn = config.hasPath("postgres.idColumn") ? config.getString("postgres.idColumn") : "id";
    this.upsert = !config.hasPath("postgres.upsert") || config.getBoolean("postgres.upsert");

    List<? extends Config> rawCols = config.getConfigList("postgres.columns");
    List<ColumnMapping> cm = new ArrayList<>(rawCols.size());
    for (Config c : rawCols) {
      cm.add(ColumnMapping.fromConfig(c));
    }
    this.columns = Collections.unmodifiableList(cm);
    if (this.columns.isEmpty()) {
      throw new IndexerException("postgres.columns must contain at least one entry");
    }
    this.insertSql = buildInsertSql();

    if (!bypass) {
      this.connection = openConnection();
    }
  }

  // Convenience constructor
  public PostgresIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix)
      throws IndexerException {
    this(config, messenger, bypass, metricsPrefix, null);
  }

  private Connection openConnection() throws IndexerException {
    try {
      Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
      conn.setAutoCommit(true);
      PGvector.addVectorType(conn);
      return conn;
    } catch (SQLException e) {
      throw new IndexerException("Could not connect to Postgres at " + jdbcUrl, e);
    }
  }

  @Override
  protected String getIndexerConfigKey() {
    return "postgres";
  }

  @Override
  public boolean validateConnection() {
    if (bypass) {
      return true;
    }
    try {
      if (connection == null || connection.isClosed() || !connection.isValid(5)) {
        return false;
      }
      return true;
    } catch (SQLException e) {
      log.error("Error validating Postgres connection", e);
      return false;
    }
  }

  @Override
  protected Set<Pair<Document, String>> sendToIndex(List<Document> documents) throws Exception {
    Set<Pair<Document, String>> failed = new HashSet<>();
    if (documents.isEmpty()) {
      return failed;
    }
    try (PreparedStatement ps = connection.prepareStatement(insertSql)) {
      List<Document> ordered = new ArrayList<>(documents.size());
      for (Document doc : documents) {
        try {
          bindRow(ps, doc);
          ps.addBatch();
          ordered.add(doc);
        } catch (Exception e) {
          failed.add(Pair.of(doc, "bind error: " + e.getMessage()));
        }
      }
      try {
        ps.executeBatch();
      } catch (SQLException e) {
        // JDBC batch is atomic-per-statement but Postgres stops at first failure;
        // surface the whole batch as failed rather than guessing which row broke.
        throw new IndexerException("Postgres batch insert failed: " + e.getMessage(), e);
      }
    }
    return failed;
  }

  private void bindRow(PreparedStatement ps, Document doc) throws SQLException {
    int idx = 1;
    ps.setString(idx++, doc.getId());
    for (ColumnMapping col : columns) {
      bindValue(ps, idx++, col, doc);
    }
  }

  private void bindValue(PreparedStatement ps, int idx, ColumnMapping col, Document doc) throws SQLException {
    if (!doc.hasNonNull(col.field)) {
      ps.setObject(idx, null);
      return;
    }
    switch (col.type) {
      case "text":
      case "varchar":
        ps.setString(idx, doc.getString(col.field));
        break;
      case "int":
        ps.setInt(idx, doc.getInt(col.field));
        break;
      case "bigint":
        ps.setLong(idx, doc.getLong(col.field));
        break;
      case "double":
      case "float":
        ps.setDouble(idx, doc.getDouble(col.field));
        break;
      case "boolean":
        ps.setBoolean(idx, doc.getBoolean(col.field));
        break;
      case "timestamp": {
        Instant instant = doc.getInstant(col.field);
        if (instant == null) {
          Date d = doc.getDate(col.field);
          instant = d == null ? null : d.toInstant();
        }
        ps.setTimestamp(idx, instant == null ? null : Timestamp.from(instant));
        break;
      }
      case "jsonb": {
        PGobject pg = new PGobject();
        pg.setType("jsonb");
        Object v = doc.asMap().get(col.field);
        try {
          pg.setValue(MAPPER.writeValueAsString(v));
        } catch (Exception e) {
          throw new SQLException("Failed to serialize jsonb for field " + col.field, e);
        }
        ps.setObject(idx, pg);
        break;
      }
      case "vector": {
        List<Float> floats = doc.getFloatList(col.field);
        float[] arr = new float[floats.size()];
        for (int i = 0; i < floats.size(); i++) {
          arr[i] = floats.get(i);
        }
        if (col.dim != null && arr.length != col.dim) {
          throw new SQLException("vector field '" + col.field + "' has length "
              + arr.length + " but column expects dim " + col.dim);
        }
        ps.setObject(idx, new PGvector(arr));
        break;
      }
      case "text[]":
        ps.setArray(idx, connection.createArrayOf("text",
            doc.getStringList(col.field).toArray(new String[0])));
        break;
      case "int[]":
        ps.setArray(idx, connection.createArrayOf("int4",
            doc.getIntList(col.field).toArray(new Integer[0])));
        break;
      case "bigint[]":
        ps.setArray(idx, connection.createArrayOf("int8",
            doc.getLongList(col.field).toArray(new Long[0])));
        break;
      case "double[]":
        ps.setArray(idx, connection.createArrayOf("float8",
            doc.getDoubleList(col.field).toArray(new Double[0])));
        break;
      default:
        throw new SQLException("Unsupported column type: " + col.type);
    }
  }

  /** Visible for testing. */
  String getInsertSql() {
    return insertSql;
  }

  private String buildInsertSql() {
    String cols = columns.stream().map(c -> quoteIdent(c.column)).collect(Collectors.joining(", "));
    String placeholders = columns.stream().map(c -> "?").collect(Collectors.joining(", "));
    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ").append(quoteIdent(table))
        .append(" (").append(quoteIdent(idColumn)).append(", ").append(cols).append(")")
        .append(" VALUES (?, ").append(placeholders).append(")");
    if (upsert) {
      String updates = columns.stream()
          .map(c -> quoteIdent(c.column) + " = EXCLUDED." + quoteIdent(c.column))
          .collect(Collectors.joining(", "));
      sb.append(" ON CONFLICT (").append(quoteIdent(idColumn)).append(") DO UPDATE SET ").append(updates);
    }
    return sb.toString();
  }

  private static String quoteIdent(String ident) {
    if (!ident.matches("[A-Za-z_][A-Za-z0-9_]*")) {
      throw new IllegalArgumentException("Unsafe identifier: " + ident);
    }
    return "\"" + ident + "\"";
  }

  @Override
  public void closeConnection() {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        log.warn("Error closing Postgres connection", e);
      }
    }
  }

  /** A single field→column binding. */
  static final class ColumnMapping {
    final String field;
    final String column;
    final String type;
    final Integer dim;

    ColumnMapping(String field, String column, String type, Integer dim) {
      this.field = field;
      this.column = column;
      this.type = type;
      this.dim = dim;
    }

    static ColumnMapping fromConfig(Config c) {
      String field = c.getString("field");
      String column = c.getString("column");
      String type = c.getString("type").toLowerCase(Locale.ROOT);
      Integer dim = c.hasPath("dim") ? c.getInt("dim") : null;
      if ("vector".equals(type) && dim == null) {
        throw new IllegalArgumentException("vector column '" + column + "' requires 'dim'");
      }
      return new ColumnMapping(field, column, type, dim);
    }
  }
}
