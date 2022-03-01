package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.List;
import java.util.Map;

public class QueryDatabase extends Stage {
  private String driver;
  private String connectionString;
  private String jdbcUser;
  private String jdbcPassword;
  private String sql;
  private String keyField;
  private Map<String, Object> fieldMapping;
  protected Connection connection = null;
  private static final Logger log = LogManager.getLogger(QueryDatabase.class);

  public QueryDatabase(Config config) {
    super(config);
    driver = config.getString("driver");
    connectionString = config.getString("connectionString");
    jdbcUser = config.getString("jdbcUser");
    jdbcPassword = config.getString("jdbcPassword");
    keyField = config.getString("keyField");
    sql = config.hasPath("sql") ? config.getString("sql") : null;
    fieldMapping = config.getConfig("fieldMapping").root().unwrapped();
  }

  @Override
  public void start() throws StageException {
    createConnection();
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(keyField)) {
      return null;
    }

    // no need to close result as closing the statement automatically closes the ResultSet
    try (PreparedStatement preparedStatement = connection.prepareStatement(sql);) {
      int replacements = preparedStatement.getParameterMetaData().getParameterCount();
      List<String> subs = doc.getStringList(keyField);

      if (replacements != subs.size()) {
        throw new StageException("mismatch between replacements needed and provided");
      }

      for (int i = 1; i < subs.size() + 1; i++) {
        String s = subs.get(i - 1);
        if (StringUtils.isEmpty(s)) {
          continue;
        }
        preparedStatement.setString(i, s);
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
      throw new StageException("Error creating connection to database", e);
    }
    try {
      connection = DriverManager.getConnection(connectionString, jdbcUser, jdbcPassword);
    } catch (SQLException e) {
      throw new StageException("Error creating connection to database", e);
    }
  }
}
