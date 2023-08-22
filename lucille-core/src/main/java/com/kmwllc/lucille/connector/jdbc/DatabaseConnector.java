package com.kmwllc.lucille.connector.jdbc;

import com.kmwllc.lucille.connector.AbstractConnector;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Database Connector - This connector can run a select statement and return the rows
 * from the database as documents which are published to a topic for processing.
 * If "otherSQLs" are set, the sql and otherSQLs must all be ordered by their join key
 * and the otherJoinFields must be populated.  If those parameters are populated
 * this connector will run the otherSQL statements in parallel and flatten the rows from
 * the otherSQL statements onto the Document as a child document
 * <p>
 * Note: currently this connector with otherSQL statements only supports integers as a
 * join key.
 *
 * @author kwatters
 */
public class DatabaseConnector extends AbstractConnector {

  private static final Logger log = LogManager.getLogger(DatabaseConnector.class);

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
  private final Set<String> ignoreColumns;
  private final List<Connection> connections = new ArrayList<>();
  // TODO: consider moving this down to the base connector class.
  //  private ConnectorState state = null;

  // The constructor that takes the config.
  public DatabaseConnector(Config config) {
    super(config);

    // required config
    driver = config.getString("driver");
    connectionString = config.getString("connectionString");
    jdbcUser = config.getString("jdbcUser");
    jdbcPassword = config.getString("jdbcPassword");
    sql = config.getString("sql");
    idField = config.getString("idField");

    // optional config

    // For MYSQL this should be set to Integer.MIN_VALUE to avoid buffering the full resultset in memory.
    // The behavior of this parameter varies from driver to driver, often it defaults to 0.
    fetchSize = config.hasPath("fetchSize") ? config.getInt("fetchSize") : null;
    preSql = ConfigUtils.getOrDefault(config, "preSql", null);
    postSql = ConfigUtils.getOrDefault(config, "postSql", null);
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
  }

  // create a jdbc connection
  private Connection createConnection() throws ClassNotFoundException, SQLException {
    Connection connection;
    try {
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      log.error("Driver not found {} check classpath to make sure the jdbc driver jar file is there.", driver);
      throw e;
    }
    try {
      connection = DriverManager.getConnection(connectionString, jdbcUser, jdbcPassword);
    } catch (SQLException e) {
      log.error("Unable to connect to database {} user:{}", connectionString, jdbcUser);
      throw e;
    }
    connections.add(connection);
    return connection;
  }

  private int getIdColumnIndex(String[] columns) throws ConnectorException {
    for (int i = 0; i < columns.length; i++) {
      if (columns[i].equalsIgnoreCase(idField)) {
        return i + 1;
      }
    }

    // throw an exception if unable to find id column
    throw new ConnectorException("Unable to find id column: " + idField);
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {

    try {
      // connect to the database.
      Connection connection = createConnection();
      // run the pre-sql (if specified)
      runSql(connection, preSql);
      // TODO: make sure we cleanup result set/statement/connections properly.
      ResultSet rs;
      log.info("Running primary sql");
      try {
        Statement state = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (fetchSize != null) {
          state.setFetchSize(fetchSize);
        }
        rs = state.executeQuery(sql);
      } catch (SQLException e) {
        throw e;
      }
      log.info("Describing primary set...");
      String[] columns = getColumnNames(rs);
      int idColumn = getIdColumnIndex(columns);

      ArrayList<ResultSet> otherResults = new ArrayList<>();
      ArrayList<String[]> otherColumns = new ArrayList<>();
      for (String otherSQL : otherSQLs) {
        log.info("Describing other result set... {}", otherSQL);
        // prepare the other sql query
        // TODO: run all sql statements in parallel.
        ResultSet rs2 = runJoinSQL(otherSQL);
        String[] columns2 = getColumnNames(rs2);
        otherResults.add(rs2);
        otherColumns.add(columns2);
      }
      log.info("Processing rows...");
      while (rs.next()) {
        // Need the ID column from the RS.
        String id = createDocId(rs.getString(idColumn));
        Document doc = Document.create(id);
        // Add each column / field name to the doc
        for (int i = 1; i <= columns.length; i++) {
          // TODO: how do we normalize our column names?  (lowercase is probably ok and likely desirable as
          // sometimes databases return columns in upper/lower case depending on which db you talk to.)
          String fieldName = columns[i - 1].toLowerCase();

          // continue if it is an id column and has the same name as the id field
          if (i == idColumn && Document.ID_FIELD.equals(fieldName)) {
            // we already have this column because it's the id column.
            continue;
          }

          // continue if field is in the ignore set
          if (ignoreColumns.contains(fieldName)) {
            continue;
          }

          // throw an exception if field is in the reserved set
          if (Document.RESERVED_FIELDS.contains(fieldName)) {
            throw new ConnectorException(
                String.format("Field name \"%s\" is reserved, please rename it or add it to the ignore list", fieldName));
          }

          String fieldValue = rs.getString(i);
          // log.info("Add Field {} Value {} -- Doc {}", fieldName, fieldValue, doc);
          if (fieldValue != null) {
            doc.setOrAdd(fieldName, fieldValue);
          }
        }
        if (!otherResults.isEmpty()) {
          // this is the primary key that the result set is ordered by.
          Integer joinId = rs.getInt(idField);
          int childId = -1;
          int j = 0;
          for (ResultSet otherResult : otherResults) {
            iterateOtherSQL(otherResult, otherColumns.get(j), doc, joinId, childId, otherJoinFields.get(j));
            j++;
          }
        }
        // feed the accumulated document.
        publisher.publish(doc);
      }

      // close all results
      rs.close();
      for (ResultSet ors : otherResults) {
        ors.close();
      }
      // the post sql.
      runSql(connection, postSql);
      flush();
    } catch (Exception e) {
      throw new ConnectorException("Exception caught during connector execution", e);
    }
  }

  private void flush() {
    // TODO: possibly move to base class / interface
    // lifecycle to be called after the last doc is processed..
    // in case the connector is doing some batching to make sure it flushes the last batch.
    // System.err.println("No Op flush for now.");
  }

  private void iterateOtherSQL(ResultSet rs2, String[] columns2, Document doc, Integer joinId, int childId, String joinField) throws SQLException {
    // Test if we need to advance or if we should read the current row ...
    if (rs2.isBeforeFirst()) {
      // we need to at least advance to the first row.
      rs2.next();
    }
    // Test if this result set is already exhausted.
    if (rs2.isAfterLast()) {
      // um... why is this getting called?  if it is?
      return;
    }

    do {
      // Convert to do-while I think we can avoid the rs2.previous() call.

      // TODO: support non INT primary key
      int otherJoinId = rs2.getInt(joinField);
      if (otherJoinId < joinId) {
        // advance until we get to the id on the right side that we want.
        rs2.next();
        continue;
      }

      if (otherJoinId > joinId) {
        // we've gone too far... lets back up and break out , move forward the primary result set.
        // we should leave the cursor here, so we can test again when the primary result set is advanced.
        return;
      }

      // here we have a match for the join keys... let's create the child doc for this joined row.
      childId++;
      Document child = Document.create(Integer.toString(childId));
      for (String c : columns2) {
        String fieldName = c.trim().toLowerCase();
        String fieldValue = rs2.getString(c);
        if (fieldValue != null) {
          child.setOrAdd(fieldName, fieldValue);
        }
      }
      // add the accumulated child doc.

      // add the accumulated rows to the document.
      doc.addChild(child);
      // Ok... so now we need to advance this cursor and see if there is another row to collapse into a child.
    } while (rs2.next());

  }

  // TODO: can we remove this method and just use runSql instead?
  private ResultSet runJoinSQL(String sql) throws SQLException {
    ResultSet rs2;
    try {
      // Running the sql
      log.info("Running other sql");
      // create a new connection instead of re-using this one because we're using forward only result sets
      Connection connection = null;
      try {
        // TODO: clean up this connection .. we need to hold onto a handle of it
        connection = createConnection();
      } catch (ClassNotFoundException e) {
        log.error("Error creating connection.", e);
      }
      Statement state2 = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      // Statement state2 = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      // make sure we stream the results instead of buffering in memory.
      // TODO: this doesn't work for h2 db..  it does work for mysql..  *shrug*
      // Mysql needs this hint so it the mysql driver doesn't try to buffer the entire resultset in memory.
      if (fetchSize != null) {
        state2.setFetchSize(fetchSize);
      }
      rs2 = state2.executeQuery(sql);
    } catch (SQLException e) {
      e.printStackTrace();
      throw e;
    }
    log.info("Other SQL Executed.");
    return rs2;
  }

  /**
   * Return an array of column names.
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

  private void runSql(Connection connection, String sql) {
    if (!StringUtils.isEmpty(sql)) {
      try (Statement state = connection.createStatement()) {
        state.executeUpdate(sql);
      } catch (SQLException e) {
        log.error("Error running Update SQL {}", sql, e);
        // TODO: maybe we should throw here?
      }
    }
  }

  //@Override
  public void stop() {
    // TODO: move this to a base class..
  }

  @Override
  public void close() throws ConnectorException {
    for (Connection connection : connections) {
      if (connection == null) {
        // no-op
        continue;
      }
      try {
        connection.close();
      } catch (SQLException e) {
        // We don't care if we can't close the connection.
        continue;
      }
    }
    // empty out the collections
    connections.clear();
  }

  // for testing purposes
  // return true if any connection is open to the database
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
