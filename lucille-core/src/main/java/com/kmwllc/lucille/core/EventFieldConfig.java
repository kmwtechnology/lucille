package com.kmwllc.lucille.core;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *   <li>{@code maxValuesPerField} (number, at least 1): cap on how many values are copied from a multi-valued
 *       field; extra values are dropped. Omit for no cap.</li>
 *   <li>{@code maxValueLength} (number, at least 1): maximum length in characters of any copied string value.
 *       Defaults to {@value #DEFAULT_MAX_VALUE_LENGTH}.</li>
 * </ul>
 *
 * <p>Only flat data is copied: scalars (string, number, boolean) and arrays of scalars. A field holding an
 * object, a nested array, or an array containing such values, or any string longer than {@code maxValueLength},
 * is skipped entirely (never truncated or partially copied), and a warning is logged the first time each field
 * is skipped. Null-valued fields are skipped silently.
 */
public class EventFieldConfig {

  private static final Logger log = LoggerFactory.getLogger(EventFieldConfig.class);

  private static final String EVENT_PATH = "kafka.event";

  /**
   * Spec for the {@code kafka.event} block; plugged into the Kafka spec so that unknown keys and wrongly typed
   * values are rejected when the config is validated at startup.
   */
  public static final Spec SPEC = SpecBuilder.parent("event")
      .optionalList("includeFields", new TypeReference<List<String>>(){})
      .optionalNumber("maxValuesPerField", "maxValueLength")
      .build();

  public static final int DEFAULT_MAX_VALUE_LENGTH = 1024;

  // marks maxValuesPerField as unset (no cap on array length)
  private static final int NO_LIMIT = -1;

  private static final JsonNodeFactory NODE_FACTORY = JsonNodeFactory.instance;

  /** Shared instance representing "no enrichment"; used when config is absent or empty. */
  public static final EventFieldConfig DISABLED =
      new EventFieldConfig(Collections.emptyList(), NO_LIMIT, DEFAULT_MAX_VALUE_LENGTH);

  private final List<String> includeFields;
  private final int maxValuesPerField;
  private final int maxValueLength;

  // fields we have already warned about, so a consistently skipped field does not log once per event
  private final Set<String> warnedFields = ConcurrentHashMap.newKeySet();

  private EventFieldConfig(List<String> includeFields, int maxValuesPerField, int maxValueLength) {
    this.includeFields = includeFields;
    this.maxValuesPerField = maxValuesPerField;
    this.maxValueLength = maxValueLength;
  }

  /**
   * Builds an {@link EventFieldConfig} from the given (typically pipeline/run) Config, reading the optional
   * {@code kafka.event} block. Returns {@link #DISABLED} when the config is null, the block or
   * {@code includeFields} is absent, or {@code includeFields} is empty.
   *
   * @throws IllegalArgumentException if a numeric setting is less than 1 (key names and types are checked
   *     separately, by {@link #SPEC})
   */
  public static EventFieldConfig fromConfig(Config config) {
    if (config == null || !config.hasPath(EVENT_PATH)) {
      return DISABLED;
    }

    if (!config.hasPath(EVENT_PATH + ".includeFields")) {
      return DISABLED;
    }

    List<String> fields = config.getStringList(EVENT_PATH + ".includeFields");
    if (fields.isEmpty()) {
      return DISABLED;
    }

    int maxValues = getPositiveIntOrDefault(config, "maxValuesPerField", NO_LIMIT);
    int maxLength = getPositiveIntOrDefault(config, "maxValueLength", DEFAULT_MAX_VALUE_LENGTH);

    return new EventFieldConfig(Collections.unmodifiableList(fields), maxValues, maxLength);
  }

  private static int getPositiveIntOrDefault(Config config, String key, int defaultValue) {
    String path = EVENT_PATH + "." + key;
    if (!config.hasPath(path)) {
      return defaultValue;
    }
    int value = config.getInt(path);
    if (value < 1) {
      throw new IllegalArgumentException(path + " must be at least 1, but was " + value);
    }
    return value;
  }

  /** Returns true if no source-document fields should be embedded. */
  public boolean isDisabled() {
    return includeFields.isEmpty();
  }

  /**
   * Extracts the configured fields from the given Document into a new {@link ObjectNode}, copying each value
   * so that its JSON representation matches how it appears in the Document itself. Fields that are absent or
   * null are skipped, as are fields that are not flat or exceed the configured limits (see the class
   * documentation). Arrays longer than {@code maxValuesPerField} are cut to that many values.
   *
   * @return an ObjectNode containing the requested, eligible fields, or {@code null} if enrichment is disabled
   *     or none of the requested fields qualify (so the Event omits {@code documentFields} entirely).
   */
  public ObjectNode extract(Document document) {
    if (isDisabled() || document == null) {
      return null;
    }

    ObjectNode result = NODE_FACTORY.objectNode();
    for (String fieldName : includeFields) {
      if (!document.has(fieldName)) {
        continue;
      }

      JsonNode value = document.getJson(fieldName);
      if (value == null || value.isNull()) {
        continue;
      }

      JsonNode copy = copyOrNull(fieldName, value);
      if (copy != null) {
        result.set(fieldName, copy);
      }
    }

    return result.isEmpty() ? null : result;
  }

  /** Returns a copy of a scalar or array of scalars (cut to the value cap), or null if the field must be skipped. */
  private JsonNode copyOrNull(String fieldName, JsonNode value) {
    if (!value.isArray()) {
      String skipReason = getSkipReason(value);
      if (skipReason != null) {
        warnSkippedOnce(fieldName, skipReason);
        return null;
      }
      return value.deepCopy();
    }

    int count = maxValuesPerField == NO_LIMIT ? value.size() : Math.min(value.size(), maxValuesPerField);
    ArrayNode copy = NODE_FACTORY.arrayNode(count);
    for (int i = 0; i < count; i++) {
      JsonNode element = value.get(i);
      String skipReason = getSkipReason(element);
      if (skipReason != null) {
        warnSkippedOnce(fieldName, skipReason);
        return null;
      }
      copy.add(element.deepCopy());
    }
    return copy;
  }

  /** Returns why the given value cannot be copied onto an event, or null if it can. */
  private String getSkipReason(JsonNode value) {
    if (value.isContainerNode()) {
      return "it is, or contains, an object or nested array";
    }
    if (value.isTextual() && value.textValue().length() > maxValueLength) {
      return "a value exceeds " + EVENT_PATH + ".maxValueLength (" + maxValueLength + ")";
    }
    return null;
  }

  private void warnSkippedOnce(String fieldName, String reason) {
    if (warnedFields.add(fieldName)) {
      log.warn("Not copying field '{}' onto events because {}. Further skips of this field are not logged.",
          fieldName, reason);
    }
  }
}
