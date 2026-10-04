package com.kmwllc.lucille.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;

import java.util.Collections;
import java.util.List;

/**
 * Resolved, immutable configuration controlling how much information about a source Document is embedded
 * in the lifecycle {@link Event}s that Lucille publishes to the Kafka event topic.
 *
 * <p>By default no source-document fields are embedded, so an {@link Event}'s serialized JSON is unchanged.
 * When {@code kafka.event.includeFields} is configured, the named fields are copied from the source Document
 * into a nested {@code documentFields} object on the Event. The copied values preserve the exact JSON shape
 * that the fields have in the Document itself (single value vs. array, and value type), because the raw
 * {@link JsonNode} for each field is copied.
 *
 * <p>Config (all under the {@code kafka.event} block, all optional):
 * <ul>
 *   <li>{@code includeFields} (string list): names of source-document fields to embed in every event.
 *       Omit or leave empty to preserve the current behavior (no {@code documentFields} in the event JSON).</li>
 *   <li>{@code maxValuesPerField} (number): cap on how many values are copied from a multi-valued field.
 *       Omit for no cap.</li>
 * </ul>
 */
public class EventFieldConfig {

  private static final JsonNodeFactory NODE_FACTORY = JsonNodeFactory.instance;

  /** Shared instance representing "no enrichment"; used when config is absent or empty. */
  public static final EventFieldConfig DISABLED = new EventFieldConfig(Collections.emptyList(), -1);

  private final List<String> includeFields;
  private final int maxValuesPerField;

  private EventFieldConfig(List<String> includeFields, int maxValuesPerField) {
    this.includeFields = includeFields;
    this.maxValuesPerField = maxValuesPerField;
  }

  /**
   * Builds an {@link EventFieldConfig} from the given (typically pipeline/run) Config, reading the optional
   * {@code kafka.event.includeFields} and {@code kafka.event.maxValuesPerField} keys. Returns {@link #DISABLED}
   * when the config is null, the keys are absent, or {@code includeFields} is empty.
   */
  public static EventFieldConfig fromConfig(Config config) {
    if (config == null || !config.hasPath("kafka.event.includeFields")) {
      return DISABLED;
    }

    List<String> fields = config.getStringList("kafka.event.includeFields");
    if (fields == null || fields.isEmpty()) {
      return DISABLED;
    }

    int max = config.hasPath("kafka.event.maxValuesPerField")
        ? config.getInt("kafka.event.maxValuesPerField")
        : -1;

    return new EventFieldConfig(Collections.unmodifiableList(fields), max);
  }

  /** Returns true if no source-document fields should be embedded. */
  public boolean isEmpty() {
    return includeFields.isEmpty();
  }

  /**
   * Extracts the configured fields from the given Document into a new {@link ObjectNode}, copying the raw
   * value node for each field so that its JSON representation matches how it appears in the Document itself.
   * Fields that are absent from the Document are skipped. When a value limit is configured, multi-valued
   * fields are truncated to that many values.
   *
   * @return an ObjectNode containing the requested, present fields, or {@code null} if enrichment is disabled
   *     or none of the requested fields are present (so the Event omits {@code documentFields} entirely).
   */
  public ObjectNode extract(Document document) {
    if (isEmpty() || document == null) {
      return null;
    }

    ObjectNode result = NODE_FACTORY.objectNode();
    for (String fieldName : includeFields) {
      if (!document.has(fieldName)) {
        continue;
      }

      JsonNode value = document.getJson(fieldName);
      if (value == null) {
        continue;
      }

      result.set(fieldName, truncate(value));
    }

    return result.isEmpty() ? null : result;
  }

  /**
   * Returns a deep copy of the value, truncated to {@code maxValuesPerField} elements if it is an array and a
   * cap is configured. Deep-copying prevents the Event from aliasing the live Document's nodes.
   */
  private JsonNode truncate(JsonNode value) {
    if (maxValuesPerField < 0 || !value.isArray() || value.size() <= maxValuesPerField) {
      return value.deepCopy();
    }

    ArrayNode truncated = NODE_FACTORY.arrayNode();
    for (int i = 0; i < maxValuesPerField; i++) {
      truncated.add(value.get(i).deepCopy());
    }
    return truncated;
  }
}
