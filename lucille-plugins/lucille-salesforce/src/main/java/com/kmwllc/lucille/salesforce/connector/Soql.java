package com.kmwllc.lucille.salesforce.connector;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Builds SOQL for one Salesforce object. A field list that would make a long request is split so each statement stays
 * within {@link #MAX_QUERY_LENGTH} characters; the connector merges the pieces on {@code Id}.
 */
final class Soql {

  /**
   * Raw statement length kept in reserve under a typical Salesforce URL limit. Encoding a comma as {@code %2C} makes
   * the request longer than the statement, so the raw cap sits well under 16 KB.
   */
  static final int MAX_QUERY_LENGTH = 8000;

  /**
   * Characters left free in a follow-up {@code Id IN (...)} query so the field list still has room for a batch of ids.
   */
  static final int ID_CLAUSE_BUDGET = 2000;

  private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z][A-Za-z0-9_]*");
  private static final Pattern FROM_OBJECT = Pattern.compile("(?i)\\bFROM\\s+([A-Za-z][A-Za-z0-9_]*)");
  private static final Pattern LEADING_WHERE = Pattern.compile("(?i)^where\\s+");
  private static final Pattern CLAUSE_TAIL = Pattern.compile("(?is)\\b(order\\s+by|group\\s+by|having|limit|offset)\\b");

  private Soql() {
  }

  static boolean isIdentifier(String name) {
    return name != null && IDENTIFIER.matcher(name).matches();
  }

  /** The object named by the first FROM clause. */
  static String referencedObject(String soql) {
    Matcher matcher = FROM_OBJECT.matcher(soql);
    if (!matcher.find()) {
      throw new IllegalArgumentException("SOQL has no FROM clause.");
    }
    return matcher.group(1);
  }

  /**
   * Returns the condition with a leading WHERE removed, or null when there is nothing to add. A condition that
   * contains ORDER BY, LIMIT, or another statement is rejected. Words inside SOQL string literals are ignored, so a
   * value such as {@code 'order by'} is allowed.
   */
  static String normalizeWhere(String where) {
    if (where == null || where.isBlank()) {
      return null;
    }
    String trimmed = where.trim();
    if (trimmed.indexOf(';') >= 0) {
      throw new IllegalArgumentException("where must be a single SOQL condition and must not contain ';'.");
    }
    String condition = LEADING_WHERE.matcher(trimmed).replaceFirst("");
    if (condition.isBlank()) {
      throw new IllegalArgumentException("where is blank.");
    }
    if (CLAUSE_TAIL.matcher(stripStringLiterals(condition)).find()) {
      throw new IllegalArgumentException(
          "where must be only the condition. Put ORDER BY, LIMIT, or a full statement in queries.");
    }
    return condition;
  }

  static String select(String objectName, List<String> fields, String where, Integer limit) {
    if (fields == null || fields.isEmpty()) {
      throw new IllegalArgumentException("A SOQL statement needs at least one field.");
    }
    StringBuilder sql = new StringBuilder();
    sql.append("SELECT ");
    appendFields(sql, fields);
    sql.append(" FROM ").append(objectName);
    if (where != null && !where.isBlank()) {
      sql.append(" WHERE ").append(where);
    }
    sql.append(" ORDER BY Id");
    if (limit != null) {
      sql.append(" LIMIT ").append(limit);
    }
    return sql.toString();
  }

  static String selectByIds(String objectName, List<String> fields, List<String> ids) {
    if (ids == null || ids.isEmpty()) {
      throw new IllegalArgumentException("An Id IN query needs at least one id.");
    }
    StringBuilder sql = new StringBuilder();
    sql.append("SELECT ");
    appendFields(sql, fields);
    sql.append(" FROM ").append(objectName).append(" WHERE Id IN (");
    for (int i = 0; i < ids.size(); i++) {
      if (i > 0) {
        sql.append(", ");
      }
      sql.append('\'').append(escapeLiteral(ids.get(i))).append('\'');
    }
    sql.append(')');
    return sql.toString();
  }

  static String escapeLiteral(String value) {
    return value.replace("\\", "\\\\").replace("'", "\\'");
  }

  /**
   * Splits {@code fields} into SELECT lists. Every list starts with {@code Id}. The first list is the paged query and
   * carries the WHERE, ORDER BY, and LIMIT. Later lists are fetched with {@code Id IN} and leave
   * {@code idClauseBudget} characters free for that predicate.
   */
  static List<List<String>> chunkFields(String objectName, List<String> fields, String where, Integer limit,
      int maxLength, int idClauseBudget) {
    if (fields == null || fields.isEmpty() || !"Id".equals(fields.get(0))) {
      throw new IllegalArgumentException("The first selected field must be Id.");
    }
    if (maxLength < 1) {
      throw new IllegalArgumentException("maxLength must be positive.");
    }
    List<List<String>> chunks = new ArrayList<>();
    List<String> current = new ArrayList<>();
    current.add("Id");
    boolean pagingChunk = true;
    for (int i = 1; i < fields.size(); i++) {
      String field = fields.get(i);
      if (lengthWith(objectName, current, field, where, limit, pagingChunk, idClauseBudget) <= maxLength) {
        current.add(field);
        continue;
      }
      if (current.size() == 1) {
        throw new IllegalArgumentException("SOQL for " + objectName + " does not fit in " + maxLength
            + " characters even with only Id and " + field + ".");
      }
      chunks.add(List.copyOf(current));
      current = new ArrayList<>();
      current.add("Id");
      pagingChunk = false;
      if (lengthWith(objectName, current, field, where, limit, false, idClauseBudget) > maxLength) {
        throw new IllegalArgumentException("Field " + field + " on " + objectName
            + " does not fit in a SOQL query of " + maxLength + " characters.");
      }
      current.add(field);
    }
    chunks.add(List.copyOf(current));
    return chunks;
  }

  /** Splits an id list into {@code Id IN} queries that each fit in {@code maxLength}. */
  static List<String> queriesForIds(String objectName, List<String> fields, List<String> ids, int maxLength) {
    List<String> queries = new ArrayList<>();
    List<String> window = new ArrayList<>();
    for (String id : ids) {
      window.add(id);
      if (selectByIds(objectName, fields, window).length() <= maxLength) {
        continue;
      }
      window.remove(window.size() - 1);
      if (window.isEmpty()) {
        throw new IllegalArgumentException(
            "SOQL Id filter for " + objectName + " does not fit in " + maxLength + " characters.");
      }
      queries.add(selectByIds(objectName, fields, List.copyOf(window)));
      window = new ArrayList<>();
      window.add(id);
      if (selectByIds(objectName, fields, window).length() > maxLength) {
        throw new IllegalArgumentException(
            "SOQL Id filter for " + objectName + " does not fit in " + maxLength + " characters.");
      }
    }
    if (!window.isEmpty()) {
      queries.add(selectByIds(objectName, fields, List.copyOf(window)));
    }
    return queries;
  }

  private static int lengthWith(String objectName, List<String> current, String extra, String where, Integer limit,
      boolean pagingChunk, int idClauseBudget) {
    List<String> fields = new ArrayList<>(current.size() + 1);
    fields.addAll(current);
    fields.add(extra);
    if (pagingChunk) {
      return select(objectName, fields, where, limit).length();
    }
    return selectByIds(objectName, fields, List.of("000000000000000AAA")).length() + idClauseBudget;
  }

  private static void appendFields(StringBuilder sql, List<String> fields) {
    for (int i = 0; i < fields.size(); i++) {
      if (i > 0) {
        sql.append(", ");
      }
      sql.append(fields.get(i));
    }
  }

  /** Drops SOQL string literals so keyword checks do not trip on a quoted value. */
  private static String stripStringLiterals(String value) {
    StringBuilder out = new StringBuilder(value.length());
    boolean inString = false;
    for (int i = 0; i < value.length(); i++) {
      char current = value.charAt(i);
      if (current == '\\' && inString && i + 1 < value.length()) {
        i++;
        continue;
      }
      if (current == '\'') {
        inString = !inString;
        continue;
      }
      if (!inString) {
        out.append(current);
      }
    }
    return out.toString();
  }
}
