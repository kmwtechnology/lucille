package com.kmwllc.lucille.util;

import com.kmwllc.lucille.core.Document;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCUtils {

  private static final Logger log = LoggerFactory.getLogger(JDBCUtils.class);

  public static void parseResultToDoc(Document doc, ResultSet rs, Map<String, Object> fieldMapping) throws SQLException {
    ResultSetMetaData metadata = rs.getMetaData();
    for (String key : fieldMapping.keySet()) {
      String field = (String) fieldMapping.get(key);
      int columnIndex = rs.findColumn(key);
      int columnType = metadata.getColumnType(columnIndex);
      typeHandling(doc, rs, field, columnIndex, columnType);
    }
  }

  public static void parseResultToDoc(Document doc, ResultSet rs, String fieldName)
      throws SQLException {
    int columnIndex = rs.findColumn(fieldName);
    parseResultToDoc(doc, rs, fieldName, columnIndex);
  }

  public static void parseResultToDoc(Document doc, ResultSet rs, String fieldName, int columnIndex)
      throws SQLException {
    ResultSetMetaData metadata = rs.getMetaData();
    typeHandling(doc, rs, fieldName, columnIndex, metadata.getColumnType(columnIndex));
  }

  private static void typeHandling(Document doc, ResultSet rs, String fieldName, int columnIndex, int columnType)
      throws SQLException {
      switch (columnType) {
        case Types.CLOB:
        case Types.NCLOB:
        case Types.VARCHAR:
        case Types.CHAR:
        case Types.LONGVARCHAR:
        case Types.NCHAR:
        case Types.NVARCHAR:
        case Types.LONGNVARCHAR:
          String str = rs.getString(columnIndex);
          if (str != null) {
            doc.setOrAdd(fieldName, str);
          }
          break;
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
          int integer = rs.getInt(columnIndex);
          // rs.getInt would return 0 if SQL value is null. This would to check that the field value is not null
          if (!rs.wasNull()) {
            doc.setOrAdd(fieldName, integer);
          }
          break;
        case Types.BIGINT:
          long longVal = rs.getLong(columnIndex);
          if (!rs.wasNull()) {
            doc.setOrAdd(fieldName,longVal);
          }
          break;
        case Types.DECIMAL:
        case Types.NUMERIC:
        case Types.DOUBLE:
          double doubleVal = rs.getDouble(columnIndex);
          // rs.getDouble would return 0 if SQL value is null. This would to check that the field value is not null
          if (!rs.wasNull()) {
            doc.setOrAdd(fieldName, doubleVal);
          }
          break;
        case Types.FLOAT:
        case Types.REAL:
          float floatVal = rs.getFloat(columnIndex);
          // rs.getFloat would return 0 if SQL value is null. This would to check that the field value is not null
          if (!rs.wasNull()) {
            doc.setOrAdd(fieldName, floatVal);
          }
          break;
        case Types.BOOLEAN:
        case Types.BIT:
          boolean boolVal = rs.getBoolean(columnIndex);
          // rs.getBoolean would return false if SQL value is null. This would to check that the field value is not null
          if (!rs.wasNull()) {
            doc.setOrAdd(fieldName, boolVal);
          }
          break;
        case Types.DATE:
          Date dateVal = rs.getDate(columnIndex);
          if (dateVal != null) {
            doc.setOrAdd(fieldName, dateVal);
          }
          break;
        case Types.TIMESTAMP:
          Timestamp timestamp = rs.getTimestamp(columnIndex);
          if (timestamp != null) {
            doc.setOrAdd(fieldName, timestamp);
          }
          break;
        // case Types.TIMESTAMP_WITH_TIMEZONE:
        // if TIMESTAMP_WITH_TIMEZONE = 2023-01-01T00:00:00-4, will be retrieved as 2023-01-01T04:00:00Z in getTimestamp()
        // better not save it if it would be converted
        case Types.BLOB:
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
          byte[] bytes = rs.getBytes(columnIndex);
          if (bytes != null) {
            doc.setOrAdd(fieldName, bytes);
          }
          break;
        default:
          log.warn("SQL Type {} ({}) not supported", JDBCType.valueOf(columnType).getName(), columnType);
      }
  }
}
