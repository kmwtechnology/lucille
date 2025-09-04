package com.kmwllc.lucille.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;

public class JSONUtils {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Adds a value to a given JsonNode at a nested key array where "a.b.c.d" would be ["a", "b", "c", "d"].
   * For ArrayNodes, it is expecting the field to be a.b.2.c where 'b' is an ArrayNode
   *
   * @param node : source json to put nested field on
   * @param nestedKeys : nested String field split up into a list
   * @param value : JsonNode value to put in place of the given field
   * @param startVal : int start value for the nestedKeys list when looping through it
   * @throws IOException
   */
  public static void putNestedFieldValue(JsonNode node, String[] nestedKeys, JsonNode value, int startVal) throws IOException {
    if (startVal > nestedKeys.length - 1) {
      throw new IOException("The startVal cannot be greater than the length of the nestedKeys array.");
    }
    for (int i = startVal; i < nestedKeys.length; i++) {
      String curKey = nestedKeys[i];
      // if gotten to last node, put the value on the field
      if (i == nestedKeys.length - 1) {
        if (isInteger(curKey)) {
          int curKeyInt = Integer.parseInt(curKey);
          ((ArrayNode) node).insert(curKeyInt, value);
        } else {
          ((ObjectNode) node).set(curKey, value);
        }
        break;
      }

      if (isInteger(curKey)) {
        int curKeyInt = Integer.parseInt(curKey);
        if (node == null) {
          node = MAPPER.createArrayNode();
        }
        if (!node.has(curKeyInt)) {
          ((ArrayNode) node).insert(curKeyInt, MAPPER.createObjectNode());
        }
        node = node.get(curKeyInt);
      } else {
        if (node == null) {
          node = MAPPER.createObjectNode();
        }
        if (!node.has(curKey)) {
          ((ObjectNode) node).set(curKey, MAPPER.createObjectNode());
        }
        node = node.get(curKey);
      }
    }
  }

  public static void putNestedFieldValue(JsonNode node, String[] nestedKeys, JsonNode value) throws IOException {
    putNestedFieldValue(node, nestedKeys, value, 0);
  }

  // gets the JsonNode at a nested path or key like "a.b.c.d"
  public static JsonNode getNestedValue(String[] nestedFields, JsonNode node) throws IOException {
    return getNestedValue(nestedFields, node, 0);
  }

  public static JsonNode getNestedValue(String[] nestedFields, JsonNode node, int startVal) throws IOException {
    if (startVal > nestedFields.length - 1) {
      throw new IOException("The startVal cannot be greater than the length of the nestedKeys array.");
    }
    JsonNode value = null;
    for (int i = startVal; i < nestedFields.length; i++) {
      String curKey = nestedFields[i];
      // for first in list, get values from node
      if (i == startVal) {
        if (node == null) {
          return null;
        }
        if (isInteger(curKey)) {
          int curKeyInt = Integer.parseInt(curKey);
          value = node.get(curKeyInt);
        } else {
          value = node.get(curKey);
        }

        continue;
      }

      if (value == null) {
        return null;
      }
      if (isInteger(curKey)) {
        int curKeyInt = Integer.parseInt(curKey);
        value = value.get(curKeyInt);
      } else {
        value = value.get(curKey);
      }
    }
    return value;
  }

  private static boolean isInteger(String str) {
    try {
      Integer.parseInt(str);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }
}
