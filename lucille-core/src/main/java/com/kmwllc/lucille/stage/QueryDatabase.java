package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.databind.node.NullNode;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.io.IOUtils;
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
        // get the metadata of the results
        ResultSetMetaData metadata = result.getMetaData();
        for (String key : fieldMapping.keySet()) {
          // get the column type of that field
          String field = (String) fieldMapping.get(key);
          int columnIndex = result.findColumn(key);
          int columnType = metadata.getColumnType(columnIndex);
          // used this instead of passing object into doc via doc.setField(Object) because Object
          // could be Date type, which needs to be converted into Instant type. Rather modify stage than
          // document interface
          // mapping of sql types to java
          switch (columnType) {
            case Types.CLOB:
            case Types.NCLOB:
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
              doc.addToField(field, result.getString(key));
              break;
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
              doc.addToField(field, result.getInt(key));
              break;
            case Types.BIGINT:
              doc.addToField(field, result.getLong(key));
              break;
            case Types.DECIMAL:
            case Types.NUMERIC:
            case Types.DOUBLE:
              doc.addToField(field, result.getDouble(key));
              break;
            case Types.FLOAT:
            case Types.REAL:
              doc.addToField(field, result.getFloat(key));
              break;
            case Types.BOOLEAN: // if Boolean is null, then the field would be false by result.getBoolean(key)
            case Types.BIT:
              doc.addToField(field, result.getBoolean(key));
              break;
            case Types.DATE:
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
              // DATE: sets nanoseconds & time to 0, then checks either JVM or DB server location, and converts to UTC
              // - e.g. JVM or DB server is UTC-4, will convert to UTC by adding 4 hours.
              // TIMESTAMP_WITH_TIMEZONE: converts it to UTC timing.
              Timestamp timestamp = result.getTimestamp(key);
              if (timestamp != null) {
                doc.addToField(field, timestamp.toInstant());
              }
              break;
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
              doc.addToField(field, result.getBytes(key));
              break;
            default:
              // for now jsonNode is not supported
              throw new StageException("SQL Type " + columnType + " not recognized in documents");
          }
        }
      }
    } catch (SQLException e) {
      log.info(e.getMessage());
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

