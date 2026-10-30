package com.kmwllc.lucille.connector.jdbc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.connector.AbstractConnector;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.util.JDBCUtils;
import com.typesafe.config.Config;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a <code>SELECT</code> statement and publishes each row as a Document. Column labels are lower-cased to
 * become field names; the <code>idField</code> column supplies the document id.
 *
 * <p>If <code>otherSQLs</code> is set, each of those statements is run on its own connection and merged with the
 * primary result set by join key: every row whose join field equals the current primary row's
 * <code>idField</code> value is attached to the Document as a child Document with id
 * <code>&lt;parentId&gt;_&lt;n&gt;</code>, where <code>n</code> counts the parent's children across all
 * <code>otherSQLs</code> starting at 0. The merge walks all result sets forward in lockstep, so <code>sql</code>
 * and every <code>otherSQLs</code> entry must be ordered by their join key and primary keys must be unique; an
 * out-of-order or duplicate key fails the run rather than silently dropping children. Join keys may be any
 * mutually comparable type (numbers, strings, dates). Child rows whose key is NULL or has no matching primary
 * row are skipped. <code>ignoreColumns</code> applies to the primary result set only; child documents carry
 * every column of their query. {@link com.kmwllc.lucille.stage.CollapseChildrenDocuments} can flatten the
 * children into multi-valued fields.
 *
 * <p>Column values are converted by {@link JDBCUtils}: SQL NULLs are skipped, and a column of an unsupported SQL
 * type fails the run (cast it in SQL or list it in <code>ignoreColumns</code>).
 *
 * <p> Config Parameters:
 * <ul>
 *   <li>driver (String): JDBC driver class name; must be on the classpath</li>
 *   <li>connectionString (String): JDBC connection URL</li>
 *   <li>jdbcUser (String): username to access database</li>
 *   <li>jdbcPassword (String): password to access database</li>
 *   <li>sql (String): SQL statement to query the database</li>
 *   <li>idField (String): column whose value becomes the document id (and the join key when otherSQLs is set)</li>
 *   <li>fetchSize (Integer, Optional): JDBC fetch size hint applied to every query. Drivers differ: MySQL needs
 *   Integer.MIN_VALUE to stream instead of buffering the whole result set; some drivers ignore the hint.</li>
 *   <li>preSQL (String, Optional): statement run before sql, e.g. INSERT, DELETE, UPDATE or DDL. Failures are logged and do not stop the run.</li>
 *   <li>postSQL (String, Optional): statement run after sql, with the same semantics as preSQL.</li>
 *   <li>otherSQLs (List&lt;String&gt;, Optional): additional SELECT statements whose rows are attached as children, as described above</li>
 *   <li>otherJoinFields (List&lt;String&gt;, Optional): the join field of each otherSQLs entry, in the same order. <b>Required if otherSQLs is provided.</b></li>
 *   <li>ignoreColumns (List&lt;String&gt;, Optional): columns of the primary result set to leave off the document (case-insensitive)</li>
 *   <li>connectionRetries (Integer, Optional): number of retries allowed to connect to database, defaults to 1</li>
 *   <li>connectionRetryPause (Integer, Optional): pause between connection retries in milliseconds, defaults to 10000</li>
 * </ul>
 *
 * @author kwatters
 */
public class DatabaseConnector extends AbstractConnector {

  private static final Logger log = LoggerFactory.getLogger(DatabaseConnector.class);

  private final String driver;
  private final String connectionString;
  private final String jdbcUser;
  private final String jdbcPassword;
  private final Integer fetchSize;
  private final String preSql;
  private final String sql;
  private final String postSql;
  private final String idField;
  private final List<String> otherSQLs;
  private final List<String> otherJoinFields;

  // the join key most recently read from each otherSQL result set during execute(); used to detect child
  // result sets that are not ordered by their join field (which would make the merge drop rows silently)
  private final Map<ResultSet, Object> lastOtherJoinIds = new IdentityHashMap<>();
  // the join key of the previous primary row during execute(), for the same ordering check on the parent side
  private Object lastPrimaryJoinId;
  private final Set<String> ignoreColumns;
  private final Integer connectionRetries;
  private final Integer connectionRetryPause;
  // every connection opened during execute() (one for the primary query, one per otherSQL); closed in close()
  private final List<Connection> connections = new ArrayList<>();

  public static final Spec SPEC = SpecBuilder.connector()
      .requiredString("driver", "connectionString", "jdbcUser", "jdbcPassword", "sql", "idField")
      .optionalString("preSQL", "postSQL")
      .optionalNumber("fetchSize", "connectionRetries", "connectionRetryPause")
      .optionalList("otherSQLs", new TypeReference<List<String>>(){})
      .optionalList("otherJoinFields", new TypeReference<List<String>>(){})
      .optionalList("ignoreColumns", new TypeReference<List<String>>(){}).build();

  public DatabaseConnector(Config config) {
    super(config);

    driver = config.getString("driver");
    connectionString = config.getString("connectionString");
    jdbcUser = config.getString("jdbcUser");
    jdbcPassword = config.getString("jdbcPassword");
    sql = config.getString("sql");
    idField = config.getString("idField");

    fetchSize = config.hasPath("fetchSize") ? config.getInt("fetchSize") : null;
    preSql = ConfigUtils.getStringOrDefault(config, "preSQL", null);
    postSql = ConfigUtils.getStringOrDefault(config, "postSQL", null);
    if (config.hasPath("otherSQLs")) {
      otherSQLs = config.getStringList("otherSQLs");
      otherJoinFields = config.getStringList("otherJoinFields");
    } else {
      otherSQLs = new ArrayList<>();
      otherJoinFields = null;
    }
    ignoreColumns = new HashSet<>();
    if (config.hasPath("ignoreColumns")) {
      ignoreColumns.addAll(
          config.getStringList("ignoreColumns")
              .stream()
              .map(String::toLowerCase)
              .collect(Collectors.toSet()));
    }
    connectionRetries = config.hasPath("connectionRetries") && config.getInt("connectionRetries") > 0
        ? config.getInt("connectionRetries") : 1;
    connectionRetryPause = config.hasPath("connectionRetryPause") && config.getInt("connectionRetryPause") > 0
        ? config.getInt("connectionRetryPause") : 10000;
  }

  // Opens a new connection, retrying up to connectionRetries times, and registers it for close().
  private Connection createConnectionWithRetries() throws ClassNotFoundException, SQLException {
    try {
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      log.error("Driver not found {} check classpath to make sure the jdbc driver jar file is there.", driver);
      throw e;
    }

    for (int attempt = 0; attempt <= connectionRetries; attempt++) {
      try {
        Connection connection = DriverManager.getConnection(connectionString, jdbcUser, jdbcPassword);
        connections.add(connection);
        return connection;
      } catch (SQLException e) {
        if (attempt == connectionRetries) {
          log.error("Unable to connect to database {} user:{} after retrying {} time(s).", connectionString, jdbcUser, attempt);
          throw e;
        }
        log.warn("Unable to connect to the database {} user:{} on retry attempt: {}. Retrying...", connectionString, jdbcUser, attempt);
        try {
          TimeUnit.MILLISECONDS.sleep(connectionRetryPause);
        } catch (InterruptedException e2) {
          log.warn("Interrupted while waiting for retry buffer to sleep.");
        }
      }
    }
    // unreachable: the last attempt either returns or rethrows
    throw new SQLException("Failed to connect after " + connectionRetries + " attempts");
  }

  private int getIdColumnIndex(String[] columns) throws ConnectorException {
    for (int i = 0; i < columns.length; i++) {
      if (columns[i].equalsIgnoreCase(idField)) {
        return i + 1;
      }
    }
    throw new ConnectorException("Unable to find id column: " + idField);
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    ResultSet rs = null;
    ArrayList<ResultSet> otherResults = null;
    Connection connection = null;
    Statement statement = null;
    try {
      connection = createConnectionWithRetries();
      runSql(connection, preSql);

      log.info("Running primary sql");
      statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      if (fetchSize != null) {
        statement.setFetchSize(fetchSize);
      }
      rs = statement.executeQuery(sql);

      log.info("Describing primary set...");
      String[] columns = getColumnNames(rs);
      int idColumn = getIdColumnIndex(columns);

      otherResults = new ArrayList<>();
      ArrayList<String[]> otherColumns = new ArrayList<>();
      lastOtherJoinIds.clear();
      lastPrimaryJoinId = null;

      for (String otherSQL : otherSQLs) {
        log.info("Describing other result set... {}", otherSQL);
        ResultSet rs2 = runJoinSQL(otherSQL);
        String[] columns2 = getColumnNames(rs2);
        otherResults.add(rs2);
        otherColumns.add(columns2);
      }
      log.info("Processing rows...");
      long rowNum = 0;
      long skippedRows = 0;
      while (rs.next()) {
        rowNum++;
        String sourceId = rs.getString(idColumn);
        if (!Document.isValidId(sourceId)) {
          // without an id the row cannot be told apart from other such rows, so skip it and carry on with the next one
          skippedRows++;
          log.error("Skipping row {} of the query: its id column \"{}\" is null or blank.", rowNum, columns[idColumn - 1]);
          continue;
        }
        String id = createDocId(sourceId);
        Document doc = Document.create(id);
        for (int i = 1; i <= columns.length; i++) {
          String fieldName = columns[i - 1];
          // an id column literally named "id" is already represented by the document id
          if (i == idColumn && Document.ID_FIELD.equals(fieldName)) {
            continue;
          }
          if (ignoreColumns.contains(fieldName)) {
            continue;
          }
          if (Document.RESERVED_FIELDS.contains(fieldName)) {
            throw new ConnectorException(
                String.format("Field name \"%s\" is reserved, please rename it or add it to the ignore list", fieldName));
          }

          try {
            // skips SQL NULLs; throws UnsupportedSqlTypeException for column types that cannot be represented
            JDBCUtils.parseResultToDoc(doc, rs, fieldName, i);
          } catch(JDBCUtils.UnsupportedSqlTypeException e) {
            // an unsupported column type is a misconfiguration: fail the run rather than drop the field
            throw e;
          } catch(SQLException e) {
            // other per-column read errors leave the field off the document
            log.warn("Error encountered while processing resultSet", e);
          }
        }
        if (!otherResults.isEmpty()) {
          Object joinId = rs.getObject(idColumn);
          // The merge also requires the primary result set to be ordered by the join key and the keys to be
          // unique: a parent whose key is smaller than, or equal to, the previous parent's would silently get no
          // children, because the child cursors have already moved past that key.
          if (lastPrimaryJoinId != null && joinId instanceof Comparable) {
            int order = ((Comparable) joinId).compareTo(lastPrimaryJoinId);
            if (order < 0) {
              throw new ConnectorException(String.format(
                  "primary result set is not ordered by id field %s (%s follows %s); add ORDER BY %s when using otherSQLs",
                  idField, joinId, lastPrimaryJoinId, idField));
            }
            if (order == 0) {
              throw new ConnectorException(String.format(
                  "primary result set has duplicate id field %s value %s; ids must be unique when using otherSQLs",
                  idField, joinId));
            }
          }
          lastPrimaryJoinId = joinId;
          // children are numbered continuously across all otherSQLs; see iterateOtherSQL for the id format
          int nextChildIndex = 0;
          int j = 0;
          for (ResultSet otherResult : otherResults) {
            nextChildIndex = iterateOtherSQL(otherResult, otherColumns.get(j), doc, joinId, nextChildIndex, otherJoinFields.get(j));
            j++;
          }
        }
        publisher.publish(doc);
      }
      if (skippedRows > 0) {
        log.warn("Skipped {} of {} rows because their id was null or blank.", skippedRows, rowNum);
      }
    } catch (Exception e) {
      throw new ConnectorException("Exception caught during connector execution", e);
    } finally {
      // result sets and the primary statement are closed here; the connections (which also own the otherSQL
      // statements) are closed in close()
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
          log.warn("Result set unable to be closed");
        }
      }
      if (otherResults != null) {
        for (ResultSet ors : otherResults) {
          try {
            if (ors != null) {
              ors.close();
            }
          } catch (SQLException e) {
            log.warn("Other result set is unable to be closed");
          }
        }
      }
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException e) {
          log.warn("Unable to close Statement", e);
        }
      }
    }
    runSql(connection, postSql);
  }

  /**
   * Attaches to {@code doc} every row of {@code rs2} whose {@code joinField} equals {@code joinId}, as child
   * documents with ids of the form {@code <parentId>_<n>}, numbering from {@code nextChildIndex}. Returns the
   * next unused index so that children from several otherSQLs are numbered continuously. Rows whose join key
   * is SQL NULL match no parent and are skipped. Both result sets are consumed forward only, so the cursor is
   * left on the first row whose key is greater than {@code joinId} (or past the end) for the next primary row
   * to continue from.
   */
  private int iterateOtherSQL(ResultSet rs2, String[] columns2, Document doc, Object joinId, int nextChildIndex, String joinField) throws Exception {
    // getRow() is 0 when there is no current row: before the first row, after the last row, or for an
    // empty result set (where isBeforeFirst() and isAfterLast() are both false). In all three cases try to
    // advance; if there is no row to advance to, this parent has no children in this result set.
    if (rs2.getRow() == 0 && !rs2.next()) {
      return nextChildIndex;
    }

    while (true) {
      Object otherJoinId = rs2.getObject(joinField);
      if (otherJoinId == null) {
        // a NULL join key (e.g. a nullable foreign key) can match no parent: skip the row
        if (!rs2.next()) {
          return nextChildIndex;
        }
        continue;
      }
      int comparison;
      try {
        comparison = ((Comparable) otherJoinId).compareTo(joinId);
      } catch (NullPointerException e) {
        throw new ConnectorException("Primary id field " + idField + " is null; cannot join otherSQLs.", e);
      } catch (ClassCastException e) {
        throw new ConnectorException("Either otherJoinId or joinId type prevents them from being compared.", e);
      }
      // The merge requires both result sets to be ordered by the join key. A child key smaller than the one
      // seen before it means the child query is not ordered, which would silently drop matching rows.
      Object lastJoinId = lastOtherJoinIds.put(rs2, otherJoinId);
      if (lastJoinId != null && ((Comparable) otherJoinId).compareTo(lastJoinId) < 0) {
        throw new ConnectorException(String.format(
            "otherSQL result set is not ordered by join field %s (%s follows %s); add ORDER BY %s",
            joinField, otherJoinId, lastJoinId, joinField));
      }
      if (comparison < 0) {
        // this child row belongs to no parent in the primary result set (or to one already passed);
        // advance exactly one row, returning if the result set is exhausted.
        if (!rs2.next()) {
          return nextChildIndex;
        }
        continue;
      } else if (comparison > 0) {
        // this child key is past the current parent; leave the cursor here for the next primary row
        return nextChildIndex;
      }

      // join keys match: this row becomes a child document
      Document child = Document.create(doc.getId() + "_" + nextChildIndex);
      nextChildIndex++;
      // columns2 is in column order (see getColumnNames), so index i corresponds to column i + 1
      for (int i = 0; i < columns2.length; i++) {
        // same type handling as the primary result set (nulls skipped, unsupported types fail the run)
        JDBCUtils.parseResultToDoc(child, rs2, columns2[i].trim(), i + 1);
      }
      doc.addChild(child);
      if (!rs2.next()) {
        return nextChildIndex;
      }
    }
  }

  // Opens an otherSQL result set on its own connection: the primary and child result sets are all forward-only
  // and consumed concurrently, which a single connection cannot support on every driver.
  private ResultSet runJoinSQL(String sql) throws SQLException {
    ResultSet rs2;
    try {
      log.info("Running other sql");
      Connection connection = null;
      try {
        connection = createConnectionWithRetries();
      } catch (ClassNotFoundException e) {
        throw new SQLException("Database driver could not be loaded: " + driver, e);
      }
      Statement state2 = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      if (fetchSize != null) {
        state2.setFetchSize(fetchSize);
      }
      rs2 = state2.executeQuery(sql);
    } catch (SQLException e) {
      log.error("Error executing SQL", e);
      throw e;
    }
    log.info("Other SQL Executed.");
    return rs2;
  }

  /**
   * Returns the result set's column labels in column order. Labels are lower-cased because drivers differ in
   * the case they report them in; the lower-cased label is the document field name.
   */
  private String[] getColumnNames(ResultSet rs) throws SQLException {
    ResultSetMetaData meta = rs.getMetaData();
    String[] names = new String[meta.getColumnCount()];
    for (int i = 0; i < names.length; i++) {
      names[i] = meta.getColumnLabel(i + 1).toLowerCase();
      log.info("column {} ", names[i]);
    }
    return names;
  }

  // Runs preSQL / postSQL. A failure is logged but does not stop the run.
  private void runSql(Connection connection, String sql) {
    if (!StringUtils.isEmpty(sql)) {
      try (Statement state = connection.createStatement()) {
        state.executeUpdate(sql);
      } catch (SQLException e) {
        log.error("Error running Update SQL {}", sql, e);
      }
    }
  }

  @Override
  public void close() throws ConnectorException {
    for (Connection connection : connections) {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          log.warn("Connection could not be closed", e);
        }
      }
    }
    connections.clear();
  }

  /** Returns true if every connection this connector opened has been closed. Intended for tests. */
  public boolean isClosed() {
    if (connections.isEmpty()) {
      return true;
    }
    for (Connection connection : connections) {
      try {
        if (!connection.isClosed()) {
          return false;
        }
      } catch (SQLException e) {
        log.error("Unable to check if connection was closed", e);
        return false;
      }
    }
    return true;
  }
}
