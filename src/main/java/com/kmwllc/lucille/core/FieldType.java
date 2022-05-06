package com.kmwllc.lucille.core;

public enum FieldType {
  STRING, INTEGER, LONG, DOUBLE, BOOLEAN;

  public static FieldType getType(String type) {
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
