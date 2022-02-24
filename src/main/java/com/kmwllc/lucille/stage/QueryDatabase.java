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
    createConnection();
  }

  private void createConnection() {
    try {
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      log.error("Error creating connection", e);
    }
    try {
      connection = DriverManager.getConnection(connectionString, jdbcUser, jdbcPassword);
    } catch (SQLException e) {
      log.error("Error creating connection", e);
    }
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(keyField)) {
      return null;
    }

    try {
      PreparedStatement preparedStatement = connection.prepareStatement(sql);

      for (String s : doc.getStringList(keyField)) {
        if (StringUtils.isEmpty(s)) {
          continue;
        }

        preparedStatement.setObject(1, s);
        preparedStatement.setString(1, s);
        ResultSet result = preparedStatement.executeQuery();

        // now we need to iterate the results
        while (result.next()) {
          // Need the ID column from the RS.
          for (String field : fieldMapping.keySet()) {
            String value = result.getString(field);
            doc.addToField(field, value);
          }
        }
        result.close();
      }
      preparedStatement.close();
    } catch (SQLException e) {
      throw new StageException("Error handling SQL statements", e);
    }
    return null;

  }
}
