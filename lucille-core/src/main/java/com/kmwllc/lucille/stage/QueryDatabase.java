package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.JDBCUtils;
import com.typesafe.config.Config;
import java.util.concurrent.TimeUnit;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Stage runs a prepared SQL statement on keyfields in a document and places the results in fields of choice.
 * This stage should try and reconnect to the database in the future.
 *
 * Config Parameters:
 * - driver (String) : Driver used for creating a connection to database
 * - connectionString (String) : used for establishing a connection to the right database
 * - jdbcUser (String) : username to access database
 * - jdbcPassword (String) : password to access database
 * - sql (String, Optional) : SQL statement that would be requested to the database. Allow for "?" character for keyFields configuration
 * - keyFields (List&lt;String&gt;) : Strings to replace ? in the statement made in sql
 *   e.g. keyFields : ["123"]
 *        sql: SELECT name FROM meal WHERE id = ?
 * - inputTypes (List&lt;String&gt;) : Each input type of each of the keyField
 * - fieldMapping (Map&lt;String, String&gt;) : map of columns retrieved from result set to the name of the field in the Lucille document
 *   it will populate with.
 * - connectionRetries (Integer, Optional) : number of retries allowed to connect to database, defaults to 1
 * - connectionRetryPause (Integer, Optional) : duration of pause between retries in milliseconds, defaults to 10000 or 10 seconds
 */
public class QueryDatabase extends Stage {

  public static final Spec SPEC = Spec.stage()
      .reqStr("driver", "connectionString", "jdbcUser", "jdbcPassword")
      .reqList("keyFields", new TypeReference<List<String>>(){})
      .reqList("inputTypes", new TypeReference<List<String>>(){})
      .optStr("sql")
      .optNum("connectionRetries", "connectionRetryPause")
      .reqParent("fieldMapping", new TypeReference<Map<String, String>>(){});

  private String driver;
  private String connectionString;
  private String jdbcUser;
  private String jdbcPassword;
  private String sql;
  private List<String> keyFields;
  private List<PreparedStatementParameterType> inputTypes;
  private Map<String, Object> fieldMapping;
  protected Connection connection = null;
  private PreparedStatement preparedStatement;
  private Integer connectionRetries;
  private Integer connectionRetryPause;
  private static final Logger log = LoggerFactory.getLogger(QueryDatabase.class);

  public QueryDatabase(Config config) {
    super(config);

    driver = config.getString("driver");
    connectionString = config.getString("connectionString");
    jdbcUser = config.getString("jdbcUser");
    jdbcPassword = config.getString("jdbcPassword");
    keyFields = config.getStringList("keyFields");
    sql = config.hasPath("sql") ? config.getString("sql") : null;
    fieldMapping = config.getConfig("fieldMapping").root().unwrapped();
    List<String> inputTypeList = config.getStringList("inputTypes");
    inputTypes = new ArrayList<>();
    for (String type : inputTypeList) {
      inputTypes.add(PreparedStatementParameterType.getType(type));
    }
    connectionRetries = config.hasPath("connectionRetries") && config.getInt("connectionRetries") > 0
        ? config.getInt("connectionRetries") : 1;
    connectionRetryPause = config.hasPath("connectionRetryPause") && config.getInt("connectionRetryPause") > 0
        ? config.getInt("connectionRetryPause") : 10000;
  }

  @Override
  public void start() throws StageException {
    createConnectionWithRetry();
    try {
      preparedStatement = connection.prepareStatement(sql);
    } catch (Exception e) {
      throw new StageException("Not a valid SQL statement", e);
    }

    if (inputTypes.size() != keyFields.size()) {
      throw new StageException("mismatch between types provided and keyfields provided");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    try {
      preparedStatement.clearParameters();
    } catch (SQLException e) {
      throw new StageException("parameters in prepared statement cannot be cleared", e);
    }
    try {
      for (int i = 0; i < keyFields.size(); i++) {
        String field = keyFields.get(i);

        if (!doc.has(field)) {
          throw new StageException("document does not have field " + field);
        }

        PreparedStatementParameterType t = inputTypes.get(i);
        switch (t) {
          case STRING:
            preparedStatement.setString(i + 1, doc.getString(field));
            break;
          case INTEGER:
            preparedStatement.setInt(i + 1, doc.getInt(field));
            break;
          case LONG:
            preparedStatement.setLong(i + 1, doc.getLong(field));
            break;
          case DOUBLE:
            preparedStatement.setDouble(i + 1, doc.getDouble(field));
            break;
          case BOOLEAN:
            preparedStatement.setBoolean(i + 1, doc.getBoolean(field));
            break;
          case DATE:
            Instant inst = doc.getInstant(field);
            Date date = (Date) Date.from(inst);
            preparedStatement.setDate(i + 1, date);
          default:
            throw new StageException("Type " + t + " not recognized");
        }
      }
      ResultSet result = preparedStatement.executeQuery();
      // now we need to iterate the results
      while (result.next()) {
        // parse result into document
        // can throw SQL Exception if column cannot be found or resultSet is closed
        // will not add to document if fieldValue is null or if field value is unsupported type
        JDBCUtils.parseResultToDoc(doc, result, fieldMapping);
      }
    } catch (SQLException e) {
      throw new StageException("Error handling SQL statements", e);
    }
    return null;
  }


  private void createConnectionWithRetry() throws StageException {
    try {
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      throw new StageException("Database driver could not be loaded.", e);
    }
    // try to get connection, and if fails, retry "connectionRetries" amount of times
    for (int attempt = 0; attempt <= connectionRetries; attempt++) {
      try {
        connection = DriverManager.getConnection(connectionString, jdbcUser, jdbcPassword);
        break;
      } catch (SQLException e) {
        if (attempt == connectionRetries) {
          log.error("Unable to connect to database {} user:{} after retrying {} time(s).", connectionString, jdbcUser, attempt);
          throw new StageException("Unable to connect to database " + connectionString, e);
        }
        log.warn("Unable to connect to the database {} user:{} on retry attempt: {}. Retrying...", connectionString, jdbcUser, attempt);
        try {
          TimeUnit.MILLISECONDS.sleep(connectionRetryPause);
        } catch (InterruptedException e2) {
          log.warn("Interrupted while waiting for retry buffer to sleep.");
        }
      }
    }
  }

  @Override
  public void stop() throws StageException {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        throw new StageException("Error closing connection.", e);
      }
    }
    if (preparedStatement != null) {
      try {
        preparedStatement.close();
      } catch (SQLException e) {
        throw new StageException("Error closing prepared statement", e);
      }
    }
  }
}

