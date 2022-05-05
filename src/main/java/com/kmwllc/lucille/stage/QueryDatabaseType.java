package com.kmwllc.lucille.stage;

public enum QueryDatabaseType {
  STRING, INTEGER, LONG, DOUBLE, BOOLEAN;

  public static QueryDatabaseType getType(String type) {
    if (type.equals("String")) {
      return STRING;
    } else if (type.equals("Integer")) {
      return INTEGER;
    } else if (type.equals("Long")) {
      return LONG;
    } else if (type.equals("Double")) {
      return DOUBLE;
    } else if (type.equals("Boolean")) {
      return BOOLEAN;
    } else {
      return null;
    }
  }
}
