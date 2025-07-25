package com.kmwllc.lucille.connector.jdbc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.connector.AbstractConnector;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.util.JDBCUtils;
import com.typesafe.config.Config;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Database Connector - This connector can run a <code>SELECT</code> statement and return the rows from the database as published Documents.
 * If <code>otherSQLs</code> is set, the <code>sql</code> and <code>otherSQLs</code> must all be ordered by their join key, and
 * <code>otherJoinFields</code> must be populated. If those parameters are populated, this connector will run the <code>otherSQL</code>
 * statements in parallel, flattening the rows from the <code>otherSQL</code> statements onto the Document as a child document.
 * <p> <b>Note:</b> With <code>otherSQL</code> statements, the Connector only supports integer join keys.
 *
 * <p> Config Parameters:
 * <ul>
 *   <li>driver (String): Driver used for creating a connection to database</li>
 *   <li>connectionString (String): used for establishing a connection to the right database</li>
 *   <li>jdbcUser (String): username to access database</li>
 *   <li>jdbcPassword (String): password to access database</li>
 *   <li>sql (String): SQL statement to query the database.</li>
 *   <li>idField (String): column name used for id in the database</li>
 *   <li>fetchSize (Integer, Optional): returns the desired resultSet size if set</li>
 *   <li>preSQL (String, Optional): SQL statement that returns nothing. Performed before sql is executed. e.g. INSERT, DELETE, UPDATE, SQL DDL statement.</li>
 *   <li>postSQL (String, Optional): SQL statement that returns nothing. Performed after sql is executed. e.g. INSERT, DELETE, UPDATE, SQL DDL statement.</li>
 *   <li>otherSQLs (List&lt;String&gt;, Optional): list of SQL statements to query and retrieve another result set of size fetchSize if set. For joining result sets.</li>
 *   <li>otherJoinFields (String, Optional): join field used for other result sets retrieved from otherSQLs. <b>Required if otherSQL is provided.</b></li>
 *   <li>ignoreColumns (List&lt;String&gt;, Optional): list of columns to ignore when populating Lucille document from sql result set.</li>
 *   <li>connectionRetries (Integer, Optional): number of retries allowed to connect to database, defaults to 1</li>
 *   <li>connectionRetryPause (Integer, Optional): duration of pause between retries in milliseconds, defaults to 10000 or 10 seconds</li>
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
  private final Set<String> ignoreColumns;
  private final Integer connectionRetries;
  private final Integer connectionRetryPause;
  private final List<Connection> connections = new ArrayList<>();
  // TODO: consider moving this down to the base connector class.
  //  private ConnectorState state = null;

  public static final Spec SPEC = Spec.connector()
      .requiredString("driver", "connectionString", "jdbcUser", "jdbcPassword", "sql", "idField")
      .optionalString("preSQL", "postSQL")
      .optionalNumber("fetchSize", "connectionRetries", "connectionRetryPause")
      .optionalList("otherSQLs", new TypeReference<List<String>>(){})
      .optionalList("otherJoinFields", new TypeReference<List<String>>(){})
      .optionalList("ignoreColumns", new TypeReference<List<String>>(){});

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
    preSql = ConfigUtils.getOrDefault(config, "preSQL", null);
    postSql = ConfigUtils.getOrDefault(config, "postSQL", null);
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

  // create a jdbc connection
  private Connection createConnectionWithRetries() throws ClassNotFoundException, SQLException {
    try {
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      log.error("Driver not found {} check classpath to make sure the jdbc driver jar file is there.", driver);
      throw e;
    }

    // try to get connection, and if fails, retry up to "connectionRetries"
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
    // should never reach this line
    throw new SQLException("Failed to connect after " + connectionRetries + " attempts");
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
    ResultSet rs = null;
    ArrayList<ResultSet> otherResults = null;
    Connection connection = null;
    Statement statement = null;
    try {
      connection = createConnectionWithRetries();
      // run the pre-sql (if specified)
      runSql(connection, preSql);
      // TODO: make sure we cleanup result set/statement/connections properly.

      log.info("Running primary sql");

      try {
        // state is closed at the end as it is still being used while gathering result sets
        statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (fetchSize != null) {
          statement.setFetchSize(fetchSize);
        }
        rs = statement.executeQuery(sql);
      } catch (SQLException e) {
        throw e;
      }

      log.info("Describing primary set...");
      String[] columns = getColumnNames(rs);
      int idColumn = getIdColumnIndex(columns);

      otherResults = new ArrayList<>();
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

          try {
            // parse result into document
            // can throw SQL Exception if column cannot be found or resultSet is closed
            // will not add to document if fieldValue is null or if field value is unsupported type
            JDBCUtils.parseResultToDoc(doc, rs, fieldName, i);
          } catch(SQLException e) {
            log.warn("Error encountered while processing resultSet", e);
          }
        }
        if (!otherResults.isEmpty()) {
          // this is the primary key that the result set is ordered by.
          Object joinId = rs.getObject(idField);
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
    } catch (Exception e) {
      throw new ConnectorException("Exception caught during connector execution", e);
    } finally {
      // we are closing the result sets
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
    // the post sql.
    runSql(connection, postSql);
  }

  private void iterateOtherSQL(ResultSet rs2, String[] columns2, Document doc, Object joinId, int childId, String joinField) throws Exception {
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
      Object otherJoinId = rs2.getObject(joinField);
      int comparison;
      try {
        comparison = ((Comparable) otherJoinId).compareTo(joinId);
      } catch (NullPointerException e) {
        throw new ConnectorException("Either otherJoinId or joinId is null.", e);
      } catch (ClassCastException e) {
        throw new ConnectorException("Either otherJoinId or joinId type prevents them from being compared.", e);
      }
      if (comparison < 0) {
        // advance until we get to the id on the right side that we want.
        rs2.next();
        continue;
      } else if (comparison > 0) {
        // we've gone too far... lets back up and break out , move forward the primary result set.
        // we should leave the cursor here, so we can test again when the primary result set is advanced.
        return;
      }

      // here we have a match for the join keys... let's create the child doc for this joined row.
      childId++;
      Document child = Document.create(Integer.toString(childId));
      for (String c : columns2) {
        String fieldName = c.trim().toLowerCase();
        Object fieldValue = rs2.getObject(c);
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
        connection = createConnectionWithRetries();
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
      log.error("Error executing SQL", e);
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

  public void stop() {
    // TODO: move this to a base class..
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
