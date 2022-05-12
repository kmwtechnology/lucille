package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
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
  private List<PreparedStatementParameterType> returnTypes;
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
    List<String> inputTypeList = config.getStringList("inputTypes");
    inputTypes = new ArrayList<>();
    for (String type : inputTypeList) {
      inputTypes.add(PreparedStatementParameterType.getType(type));
    }
    List<String> returnTypeList = config.getStringList("returnTypes");
    returnTypes = new ArrayList<>();
    for (String type : returnTypeList) {
      returnTypes.add(PreparedStatementParameterType.getType(type));
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
    try {
      replacements = preparedStatement.getParameterMetaData().getParameterCount();
    } catch (Exception e) {
      throw new StageException("Parameter metadata could not be accessed", e);
    }

    if (inputTypes.size() != keyFields.size()) {
      throw new StageException("mismatch between types provided and keyfields provided");
    }

    if (returnTypes.size() != fieldMapping.size()) {
      throw new StageException("mismatch between return types provided and field mapping provided");
    }
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {

    List<Object> subs = new ArrayList<Object>();

    for (int i = 0; i < keyFields.size(); i++) {
      String field = keyFields.get(i);
      if (!doc.has(field)) {
        throw new StageException("document does not have field " + field);
      }

      PreparedStatementParameterType t = inputTypes.get(i);
      switch (t) {
        case STRING:
          subs.add(doc.getString(field));
          break;
        case INTEGER:
          subs.add(doc.getInt(field));
          break;
        case LONG:
          subs.add(doc.getLong(field));
          break;
        case DOUBLE:
          subs.add(doc.getDouble(field));
          break;
        case BOOLEAN:
          subs.add(doc.getBoolean(field));
          break;
        case DATE:
          subs.add(doc.getInstant(field));
        default:
          throw new StageException("Type " + t + " not recognized");
      }
    }

    if (replacements != subs.size()) {
      throw new StageException("mismatch between replacements needed and provided");
    }
    // no need to close result as closing the statement automatically closes the ResultSet
    try {
      preparedStatement.clearParameters();
      for (int i = 0; i < subs.size(); i++) {
        PreparedStatementParameterType t = inputTypes.get(i);
        switch (t) {
          case STRING:
            String str = (String) subs.get(i);
            preparedStatement.setString(i + 1, str);
            break;
          case INTEGER:
            int integer = (int) subs.get(i);
            preparedStatement.setInt(i + 1, integer);
            break;
          case LONG:
            long l = (long) subs.get(i);
            preparedStatement.setLong(i + 1, l);
            break;
          case DOUBLE:
            double d = (double) subs.get(i);
            preparedStatement.setDouble(i + 1, d);
            break;
          case BOOLEAN:
            boolean b = (boolean) subs.get(i);
            preparedStatement.setBoolean(i + 1, b);
            break;
          case DATE:
            Instant inst = (Instant) subs.get(i);
            Date date = (Date) Date.from(inst);
            preparedStatement.setDate(i + 1, date);
            break;
          default:
            throw new StageException("Type " + t + " not recognized");
        }
      }


      ResultSet result = preparedStatement.executeQuery();
      // now we need to iterate the results
      while (result.next()) {

        // Need the ID column from the RS.
        int index = 0;
        for (String key : fieldMapping.keySet()) {
          String field = (String) fieldMapping.get(key);
          PreparedStatementParameterType t = returnTypes.get(index);
          switch (t) {
            case STRING:
              String str = result.getString(key);
              doc.addToField(field, str);
              break;
            case INTEGER:
              int i = result.getInt(key);
              doc.addToField(field, i);
              break;
            case LONG:
              long l = result.getLong(key);
              doc.addToField(field, l);
              break;
            case DOUBLE:
              double d = result.getDouble(key);
              doc.addToField(field, d);
              break;
            case BOOLEAN:
              boolean b = result.getBoolean(key);
              doc.addToField(field, b);
              break;
            case DATE:
              Date date = result.getDate(key);
              doc.addToField(field, date.toInstant());
              break;
            default:
              throw new StageException("Type " + t + " not recognized");
          }
          index++;
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

