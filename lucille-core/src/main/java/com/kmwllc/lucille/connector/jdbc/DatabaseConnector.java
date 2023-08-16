package com.kmwllc.lucille.connector.jdbc;

import com.kmwllc.lucille.connector.AbstractConnector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Database Connector - This connector can run a select statement and return the rows 
 * from the database as documents which are published to a topic for processing.
 * If "otherSQLs" are set, the sql and otherSQLs must all be ordered by their join key
 * and the otherJoinFields must be populated.  If those parameters are populated
 * this connector will run the otherSQL statements in parallel and flatten the rows from
 * the otherSQL statements onto the Document as a child document
 *
 * @author kwatters
 *
 */
public class DatabaseConnector extends AbstractConnector {

  private static final Logger log = LogManager.getLogger(DatabaseConnector.class);

  private String driver;
  private String connectionString;
  private String jdbcUser;
  private String jdbcPassword;
  private String preSql;
  private String sql;
  private String postSql;
  private String idField;
  private List<String> otherSQLs = new ArrayList<String>();
  private List<String> otherJoinFields;
  private Connection connection = null;

  // The constructor that takes the config.
  public DatabaseConnector(Config config) {
    super(config);
    // TODO: move to base class functionality
    // docIdPrefix = config.getString("docIdPrefix");
    driver = config.getString("driver");
    connectionString = config.getString("connectionString");
    jdbcUser = config.getString("jdbcUser");
    jdbcPassword = config.getString("jdbcPassword");
    sql = config.getString("sql");
    idField = config.getString("idField");
    if (config.hasPath("preSql")) {
      preSql = config.getString("preSql");
    }
    if (config.hasPath("postSql")) {
      postSql = config.getString("postSql");
    }
    if (config.hasPath("otherSQLs")) {
      otherSQLs = config.getStringList("otherSQLs");
      otherJoinFields = config.getStringList("otherJoinFields");
    }
  }

  // create a jdbc connection
  private void createConnection() throws ClassNotFoundException, SQLException {
    try {
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      // TODO better error handling/logging
      e.printStackTrace();
      setState(ConnectorState.ERROR);
      throw (e);
    }
    try {
      connection = DriverManager.getConnection(connectionString, jdbcUser, jdbcPassword);
    } catch (SQLException e) {
      // TODO better logging/error handling.
      e.printStackTrace();
      setState(ConnectorState.ERROR);
      throw (e);
    }
  }

  private void setState(ConnectorState error) {
    // TODO: update the State here.
    // System.err.println("Implement Me: ");
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {

    try {
      setState(ConnectorState.RUNNING);
      // connect to the database.
      createConnection();
      // run the pre-sql (if specified)
      runSql(preSql);

      ResultSet rs = null;
      try {
        log.info("Running primary sql");
        Statement state = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        rs = state.executeQuery(sql);
      } catch (SQLException e) {
        e.printStackTrace();
        setState(ConnectorState.ERROR);
        throw (e);
      }

      log.info("Describing primary set...");
      String[] columns = getColumnNames(rs);
      int idColumn = -1;
      for (int i = 0; i < columns.length; i++) {
        if (columns[i].equalsIgnoreCase(idField)) {
          idColumn = i + 1;
          break;
        }
      }

      ArrayList<ResultSet> otherResults = new ArrayList<ResultSet>();
      ArrayList<String[]> otherColumns = new ArrayList<String[]>();
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
          if (i == idColumn && Document.ID_FIELD.equals(fieldName)) {
            // we already have this column because it's the id column.
            continue;
          }
          // log.info("Add Field {} Value {} -- Doc {}", columns[i-1].toLowerCase(), rs.getString(i), doc);
          String fieldValue = rs.getString(i);
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

        // Fix the duplicate id field value that occurs
        // doc.setField("id", id);
        // feed the accumulated document.
        publisher.publish(doc);
      }

      // close all results
      rs.close();
      for (ResultSet ors : otherResults) {
        ors.close();
      }
      // the post sql.
      runSql(postSql);
      flush();
      setState(ConnectorState.STOPPED);
    } catch (Exception e) {
      setState(ConnectorState.ERROR);
      throw new ConnectorException("Exception caught during connector execution", e);
    }
  }

  private void flush() {
    // TODO: possibly move to base class / interface
    // lifecycle to be called after the last doc is processed..
    // in case the connector is doing some batching to make sure it flushes the last batch.
    // System.err.println("No Op flush for now.");
  }

  private void iterateOtherSQL(ResultSet rs2, String[] columns2, Document doc, Integer joinId, int childId, String joinField)
      throws SQLException {
    while (rs2.next()) {
      // TODO: support non INT primary key
      Integer otherJoinId = rs2.getInt(joinField);

      if (otherJoinId < joinId) {
        // advance until we get to the id on the right side that we want.
        continue;
      }
      if (otherJoinId > joinId) {
        // we've gone too far.. lets back up and break out , move forward the primary result set.
        rs2.previous();
        break;
      }
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
    }

    // TODO: can we remove this?
    if (!rs2.isLast()) {
      rs2.previous();
    }

  }

  private ResultSet runJoinSQL(String sql) throws SQLException {
    ResultSet rs2 = null;
    try {
      // TODO: can we do this with forward only ?
      log.info("Running other sql");
      Statement state2 = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      rs2 = state2.executeQuery(sql);
    } catch (SQLException e) {
      e.printStackTrace();
      setState(ConnectorState.ERROR);
      throw (e);
    }
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

  private void runSql(String sql) {
    if (!StringUtils.isEmpty(sql)) {
      try {
        Statement state = connection.createStatement();
        state.executeUpdate(sql);
        state.close();
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  //@Override
  public void stop() {
    // TODO: move this to a base class..
    setState(ConnectorState.STOPPED);
  }

  public String getDriver() {
    return driver;
  }

  public void setDriver(String driver) {
    this.driver = driver;
  }

  public String getConnectionString() {
    return connectionString;
  }

  public void setConnectionString(String connectionString) {
    this.connectionString = connectionString;
  }

  public String getJdbcUser() {
    return jdbcUser;
  }

  public void setJdbcUser(String jdbcUser) {
    this.jdbcUser = jdbcUser;
  }

  public String getJdbcPassword() {
    return jdbcPassword;
  }

  public void setJdbcPassword(String jdbcPassword) {
    this.jdbcPassword = jdbcPassword;
  }

  public String getPreSql() {
    return preSql;
  }

  public void setPreSql(String preSql) {
    this.preSql = preSql;
  }

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public String getPostSql() {
    return postSql;
  }

  public void setPostSql(String postSql) {
    this.postSql = postSql;
  }

  public String getIdField() {
    return idField;
  }

  public void setIdField(String idField) {
    this.idField = idField;
  }

  @Override
  public void close() throws ConnectorException {
    if (connection == null) {
      // no-op
      return;
    }
    try {
      connection.close();
    } catch (SQLException e) {
      throw new ConnectorException("Error closing the connection", e);
    }
  }

  // for testing purposes
  Connection getConnection() {
    return this.connection;
  }
}
