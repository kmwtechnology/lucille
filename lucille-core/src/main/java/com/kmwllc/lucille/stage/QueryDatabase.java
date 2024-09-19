package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.JDBCUtils;
import com.typesafe.config.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This Stage runs a prepared SQL statement on keyfields in a document and places the results in fields of choice.
 * This stage should try and reconnect to the database in the future.
 */
public class QueryDatabase extends Stage {

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
  private static final Logger log = LogManager.getLogger(QueryDatabase.class);

  public QueryDatabase(Config config) {
    super(config, new StageSpec()
        .withOptionalProperties("sql")
        .withRequiredParents("fieldMapping")
        .withRequiredProperties("driver", "connectionString", "jdbcUser", "jdbcPassword",
            "keyFields", "inputTypes"));

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
  }

  @Override
  public void start() throws StageException {
    createConnection();
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

