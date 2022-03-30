package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This Stage runs a prepared SQL statement on keyfields in a document and places the results in fields of choice.
 * Currently, this stage treats all fields as Strings, so it may be important to support more types in the future.
 * Additionally, this stage should try and reconnect to the database in the future.
 */
public class QueryDatabase extends Stage {
  private String driver;
  private String connectionString;
  private String jdbcUser;
  private String jdbcPassword;
  private String sql;
  private List<String> keyFields;
  private Map<String, Object> fieldMapping;
  protected Connection connection = null;
  PreparedStatement preparedStatement;
  private int replacements;
  private static final Logger log = LogManager.getLogger(QueryDatabase.class);

  public QueryDatabase(Config config) {
    super(config);
    driver = config.getString("driver");
    connectionString = config.getString("connectionString");
    jdbcUser = config.getString("jdbcUser");
    jdbcPassword = config.getString("jdbcPassword");
    keyFields = config.getStringList("keyFields");
    sql = config.hasPath("sql") ? config.getString("sql") : null;
    fieldMapping = config.getConfig("fieldMapping").root().unwrapped();
  }

  @Override
  public void start() throws StageException {
    createConnection();
    try {
      preparedStatement = connection.prepareStatement(sql);
    } catch (Exception e) {
      throw new StageException("Not a valid SQL statement", e);
    }
    try {
      replacements = preparedStatement.getParameterMetaData().getParameterCount();
    } catch (Exception e) {
      throw new StageException("Parameter metadata could not be accessed", e);
    }
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {

    List<String> subs = new ArrayList<String>();
    for (String field : keyFields) {
      if (!doc.has(field)) {
        throw new StageException("document does not have field " + field);
      }
      subs.add(doc.getString(field));
    }

    if (replacements != subs.size()) {
      throw new StageException("mismatch between replacements needed and provided");
    }

    // no need to close result as closing the statement automatically closes the ResultSet
    try {
      preparedStatement.clearParameters();
      for (int i = 0; i < subs.size(); i++) {
        String s = subs.get(i);
        if (StringUtils.isEmpty(s)) {
          continue;
        }
        preparedStatement.setString(i + 1, s);
      }

      ResultSet result = preparedStatement.executeQuery();

      // now we need to iterate the results
      while (result.next()) {

        // Need the ID column from the RS.
        for (String key : fieldMapping.keySet()) {
          String value = result.getString(key);
          String field = (String) fieldMapping.get(key);
          doc.addToField(field, value);
        }
      }
    } catch (SQLException e) {
      throw new StageException("Error handling SQL statements", e);
    }
    return null;
  }

  private void createConnection() throws StageException {
    try {
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      throw new StageException("Database driver could not be loaded.", e);
    }
    try {
      connection = DriverManager.getConnection(connectionString, jdbcUser, jdbcPassword);
    } catch (SQLException e) {
      throw new StageException("Error creating connection to database", e);
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
