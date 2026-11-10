package com.kmwllc.lucille.salesforce.connector;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

/**
 * Converts a Salesforce JSON value into a value {@link com.kmwllc.lucille.core.Document} can store. Datetime fields
 * become {@link Instant}s. Date and time fields stay strings, because Lucille has no date-only type and a date should
 * not be shifted to midnight UTC.
 */
final class SalesforceValues {

  private SalesforceValues() {
  }

  static Object toFieldValue(JsonNode node, String salesforceType) {
    if (node == null || node.isNull() || node.isMissingNode()) {
      return null;
    }
    if (node.isBoolean()) {
      return node.booleanValue();
    }
    if (node.isIntegralNumber()) {
      return node.canConvertToLong() ? node.longValue() : node.asText();
    }
    if (node.isFloatingPointNumber()) {
      return node.doubleValue();
    }
    if (node.isTextual()) {
      String text = node.asText();
      if ("datetime".equalsIgnoreCase(salesforceType)) {
        Instant instant = parseDateTime(text);
        if (instant != null) {
          return instant;
        }
      }
      return text;
    }
    return node;
  }

  /**
   * Parses a Salesforce datetime. The REST API emits {@code 2024-01-15T13:45:30.000+0000}; {@code Z} and
   * {@code +00:00} are accepted as well. A value that is not a datetime returns null.
   */
  static Instant parseDateTime(String text) {
    if (text == null || text.length() < 20 || text.charAt(10) != 'T') {
      return null;
    }
    try {
      return OffsetDateTime.parse(toIsoOffset(text)).toInstant();
    } catch (DateTimeParseException e) {
      return null;
    }
  }

  /** Turns a trailing {@code +0000} offset into the {@code +00:00} form {@link OffsetDateTime} parses. */
  private static String toIsoOffset(String text) {
    if (text.endsWith("Z")) {
      return text;
    }
    int length = text.length();
    if (length < 5) {
      return text;
    }
    char sign = text.charAt(length - 5);
    if (sign != '+' && sign != '-') {
      return text;
    }
    String zone = text.substring(length - 4);
    if (zone.chars().allMatch(Character::isDigit)) {
      return text.substring(0, length - 2) + ":" + text.substring(length - 2);
    }
    return text;
  }
}
