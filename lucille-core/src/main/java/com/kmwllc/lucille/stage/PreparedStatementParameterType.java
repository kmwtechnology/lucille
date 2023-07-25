package com.kmwllc.lucille.stage;

/**
 * An enum to represent the parameter types which can be used by the Prepared Statement in the QueryDatabase stage.
 */
public enum PreparedStatementParameterType {
  STRING,
  INTEGER,
  LONG,
  DOUBLE,
  BOOLEAN,
  DATE;

  public static PreparedStatementParameterType getType(String type)
      throws IllegalArgumentException {
    switch (type) {
      case "String":
        return STRING;
      case "Integer":
        return INTEGER;
      case "Long":
        return LONG;
      case "Double":
        return DOUBLE;
      case "Boolean":
        return BOOLEAN;
      case "Date":
        return DATE;
      default:
        throw new IllegalArgumentException(
            "type " + type + " is not a valid prepared statement parameter.");
    }
  }
}
