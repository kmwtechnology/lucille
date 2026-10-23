package com.kmwllc.lucille.util;

import com.kmwllc.lucille.core.Document;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;

public class JDBCUtils {

  /**
   * Thrown when a result set column has a SQL type that Lucille cannot represent faithfully on a Document.
   * An unsupported type is treated as a misconfiguration (select a different column or cast it in SQL) and
   * fails the run rather than silently dropping or converting the value.
   */
  public static class UnsupportedSqlTypeException extends SQLException {

    public UnsupportedSqlTypeException(String fieldName, String typeName, int typeCode) {
      super(String.format("Unsupported SQL type %s (%d) for column %s; cast it in SQL or exclude the column",
          typeName, typeCode, fieldName));
    }
  }

  // The column's SQL type name as reported by the driver, or "unknown" if the driver cannot provide it.
  private static String columnTypeName(ResultSet rs, int columnIndex) {
    try {
      String name = rs.getMetaData().getColumnTypeName(columnIndex);
      return name == null ? "unknown" : name;
    } catch (Exception e) {
      return "unknown";
    }
  }

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
        // DATE and TIMESTAMP carry no zone. Reading them as LocalDate/LocalDateTime keeps the stored wall-clock
        // value regardless of the JVM's default zone (getDate/getTimestamp would shift it by that zone's offset);
        // the value is then labelled UTC so that Document.getInstant/getDate/getTimestamp keep working.
        case Types.DATE:
          LocalDate dateVal = rs.getObject(columnIndex, LocalDate.class);
          if (dateVal != null) {
            doc.setOrAdd(fieldName, dateVal.atStartOfDay(ZoneOffset.UTC).toInstant());
          }
          break;
        case Types.TIMESTAMP:
          LocalDateTime timestamp = rs.getObject(columnIndex, LocalDateTime.class);
          if (timestamp != null) {
            doc.setOrAdd(fieldName, timestamp.toInstant(ZoneOffset.UTC));
          }
          break;
        // case Types.TIMESTAMP_WITH_TIMEZONE:
        // if TIMESTAMP_WITH_TIMEZONE = 2023-01-01T00:00:00-4, will be retrieved as 2023-01-01T04:00:00Z in getTimestamp()
        // better not save it if it would be converted
        case Types.TIME:
          // a zone-less time of day round-trips losslessly as the driver's string form ("10:15:30")
          String timeStr = rs.getString(columnIndex);
          if (timeStr != null) {
            doc.setOrAdd(fieldName, timeStr);
          }
          break;
        case Types.BLOB:
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
          // some drivers (H2) report UUID columns as BINARY; storing the 16 raw bytes would misrepresent the value
          if ("UUID".equalsIgnoreCase(columnTypeName(rs, columnIndex))) {
            throw new UnsupportedSqlTypeException(fieldName, "UUID", columnType);
          }
          byte[] bytes = rs.getBytes(columnIndex);
          if (bytes != null) {
            doc.setOrAdd(fieldName, bytes);
          }
          break;
        default:
          // includes the zone-aware temporal types (TIME/TIMESTAMP WITH TIME ZONE), ARRAY, STRUCT, OTHER
          // (JSON, geometry, interval, enum) and vendor-specific type codes outside java.sql.Types
          throw new UnsupportedSqlTypeException(fieldName, columnTypeName(rs, columnIndex), columnType);
      }
  }
}
