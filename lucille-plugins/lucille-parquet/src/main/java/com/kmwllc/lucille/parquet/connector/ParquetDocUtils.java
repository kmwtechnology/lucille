package com.kmwllc.lucille.parquet.connector;

import com.kmwllc.lucille.core.Document;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.Type;

public class ParquetDocUtils {

  private ParquetDocUtils() { }

  public static void setDocField(Document doc, Type field, SimpleGroup simpleGroup, int j) {
    // check if we have a field value before setting it.
    if (simpleGroup.getFieldRepetitionCount(j) == 0) {
      return;
    }

    String fieldName = field.getName();

    switch (field.asPrimitiveType().getPrimitiveTypeName()) {
      case BINARY:
        doc.setField(fieldName, simpleGroup.getString(j, 0));
        break;
      case FLOAT:
        doc.setField(fieldName, simpleGroup.getFloat(j, 0));
        break;
      case INT32:
        doc.setField(fieldName, simpleGroup.getInteger(j, 0));
        break;
      case INT64:
        doc.setField(fieldName, simpleGroup.getLong(j, 0));
        break;
      case DOUBLE:
        doc.setField(fieldName, simpleGroup.getDouble(j, 0));
        break;
      // todo consider adding a default case
    }
  }

  public static void addToField(Document doc, String fieldName, Type type, Group group) {
    switch (type.asPrimitiveType().getPrimitiveTypeName()) {
      case BINARY:
        doc.addToField(fieldName, group.getString(0, 0));
        break;
      case FLOAT:
        doc.addToField(fieldName, group.getFloat(0, 0));
        break;
      case INT32:
        doc.addToField(fieldName, group.getInteger(0, 0));
        break;
      case INT64:
        doc.addToField(fieldName, group.getLong(0, 0));
        break;
      case DOUBLE:
        doc.addToField(fieldName, group.getDouble(0, 0));
        break;
      // todo consider adding a default case
    }
  }
}
