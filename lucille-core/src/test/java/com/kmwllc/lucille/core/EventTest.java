package com.kmwllc.lucille.core;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.time.Instant;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class EventTest {

  @Test
  public void testCreateEqualsHashcodeDeserialize() throws Exception {
    Document doc1 = Document.create("id1", "runId1");
    Event event1 = new Event(doc1, "test message", Event.Type.FINISH);
    Event event1B = Event.fromJsonString(event1.toString());
    assertTrue(event1.equals(event1));
    assertTrue(event1.equals(event1B));
    assertTrue(event1B.equals(event1));
    assertEquals(event1.hashCode(), event1B.hashCode());

    KafkaDocument doc2 = new KafkaDocument(doc1, "t1", 5, 3, "testKey");
    Instant beforeEvent2 = Instant.now();
    Event event2 = new Event(doc2, "test message", Event.Type.FINISH);
    Instant afterEvent2 = Instant.now();
    assertFalse(event1.equals(event2));
    assertFalse(event2.equals(event1));
    assertNotEquals(event1.hashCode(), event2.hashCode());
    Event event2B = Event.fromJsonString(event2.toString());
    assertTrue(event2.equals(event2));
    assertTrue(event2.equals(event2B));
    assertTrue(event2B.equals(event2));
    assertEquals(event2.hashCode(), event2B.hashCode());

    assertEquals("id1", event2B.getDocumentId());
    assertEquals("runId1", event2B.getRunId());
    assertEquals("test message", event2B.getMessage());
    assertEquals(Event.Type.FINISH, event2B.getType());
    assertFalse(beforeEvent2.isAfter(event2.getInstant()));
    assertFalse(afterEvent2.isBefore(event2.getInstant()));
    assertEquals(event2.getInstant(), event2B.getInstant());
    assertEquals("t1", event2B.getTopic());
    assertEquals(Integer.valueOf(5), event2B.getPartition());
    assertEquals("testKey", event2B.getKey());
  }

  @Test
  public void testIsCreate() throws Exception {
    Document doc1 = Document.create("id1", "runId1");
    Event event1 = new Event(doc1, "test message", Event.Type.CREATE);
    Event event2 = new Event(doc1, "test message", Event.Type.FINISH);
    Event event3 = new Event(doc1, "test message", Event.Type.FAIL);
    Event event4 = new Event(doc1, "test message", Event.Type.DROP);
    assertTrue(event1.isCreate());
    assertFalse(event2.isCreate());
    assertFalse(event3.isCreate());
    assertFalse(event4.isCreate());
  }

  @Test
  public void testNoEnrichmentByDefault() throws Exception {
    Document doc = Document.create("id1", "runId1");
    doc.setField("source", "s3://bucket/key");

    // both the un-enriched constructor and a null field config leave documentFields absent
    Event event = new Event(doc, "msg", Event.Type.FINISH);
    Event eventNullConfig = new Event(doc, "msg", Event.Type.FINISH, null);
    Event eventDisabled = new Event(doc, "msg", Event.Type.FINISH, EventFieldConfig.DISABLED);

    assertNull(event.getDocumentFields());
    assertNull(eventNullConfig.getDocumentFields());
    assertNull(eventDisabled.getDocumentFields());
    assertFalse(event.toString().contains("documentFields"));
  }

  @Test
  public void testEnrichmentIncludesRequestedFields() throws Exception {
    Document doc = Document.create("id1", "runId1");
    doc.setField("source", "s3://bucket/key");
    doc.setField("mimeType", "application/pdf");
    doc.setField("unwanted", "should not appear");

    EventFieldConfig fieldConfig = configFor(List.of("source", "mimeType"), null);
    Event event = new Event(doc, "msg", Event.Type.FINISH, fieldConfig);

    ObjectNode fields = event.getDocumentFields();
    assertEquals("s3://bucket/key", fields.get("source").asText());
    assertEquals("application/pdf", fields.get("mimeType").asText());
    assertFalse(fields.has("unwanted"));

    // survives JSON round-trip
    Event roundTripped = Event.fromJsonString(event.toString());
    assertEquals(event, roundTripped);
    assertEquals("s3://bucket/key", roundTripped.getDocumentFields().get("source").asText());
  }

  @Test
  public void testEnrichmentPreservesMultiValuedShape() throws Exception {
    Document doc = Document.create("id1", "runId1");
    doc.addToField("tags", "a");
    doc.addToField("tags", "b");
    doc.setField("single", "only");

    Event event = new Event(doc, "msg", Event.Type.CREATE, configFor(List.of("tags", "single"), null));
    ObjectNode fields = event.getDocumentFields();

    // multi-valued field stays an array, single-valued stays a scalar, matching the document's own JSON
    assertTrue(fields.get("tags").isArray());
    assertEquals(2, fields.get("tags").size());
    assertEquals("a", fields.get("tags").get(0).asText());
    assertFalse(fields.get("single").isArray());
    assertEquals("only", fields.get("single").asText());
  }

  @Test
  public void testEnrichmentSkipsAbsentFields() throws Exception {
    Document doc = Document.create("id1", "runId1");
    doc.setField("present", "yes");

    // "missing" is absent from the doc; requesting only absent fields yields no documentFields at all
    Event withOnePresent = new Event(doc, "msg", Event.Type.FINISH, configFor(List.of("present", "missing"), null));
    assertTrue(withOnePresent.getDocumentFields().has("present"));
    assertFalse(withOnePresent.getDocumentFields().has("missing"));

    Event withNonePresent = new Event(doc, "msg", Event.Type.FINISH, configFor(List.of("missing"), null));
    assertNull(withNonePresent.getDocumentFields());
  }

  @Test
  public void testMaxValuesPerFieldTruncates() throws Exception {
    Document doc = Document.create("id1", "runId1");
    doc.addToField("tags", "a");
    doc.addToField("tags", "b");
    doc.addToField("tags", "c");

    Event event = new Event(doc, "msg", Event.Type.FINISH, configFor(List.of("tags"), 2));
    ObjectNode fields = event.getDocumentFields();
    assertEquals(2, fields.get("tags").size());
    assertEquals("a", fields.get("tags").get(0).asText());
    assertEquals("b", fields.get("tags").get(1).asText());
  }

  @Test
  public void testMaxValuesPerFieldEqualToSizeKeepsAll() throws Exception {
    Document doc = Document.create("id1", "runId1");
    doc.addToField("tags", "a");
    doc.addToField("tags", "b");

    // cap equal to the number of values: nothing is truncated (boundary of value.size() <= max)
    Event event = new Event(doc, "msg", Event.Type.FINISH, configFor(List.of("tags"), 2));
    ObjectNode fields = event.getDocumentFields();
    assertTrue(fields.get("tags").isArray());
    assertEquals(2, fields.get("tags").size());
    assertEquals("a", fields.get("tags").get(0).asText());
    assertEquals("b", fields.get("tags").get(1).asText());
  }

  @Test
  public void testMaxValuesPerFieldZeroYieldsEmptyArray() throws Exception {
    Document doc = Document.create("id1", "runId1");
    doc.addToField("tags", "a");
    doc.addToField("tags", "b");

    // a cap of 0 truncates every value out, leaving an empty array (not null; the field is still present)
    Event event = new Event(doc, "msg", Event.Type.FINISH, configFor(List.of("tags"), 0));
    ObjectNode fields = event.getDocumentFields();
    assertTrue(fields.has("tags"));
    assertTrue(fields.get("tags").isArray());
    assertEquals(0, fields.get("tags").size());
  }

  @Test
  public void testMaxValuesPerFieldDoesNotAffectSingleValuedField() throws Exception {
    Document doc = Document.create("id1", "runId1");
    doc.setField("single", "only");

    // a single-valued (scalar) field is never truncated regardless of the cap; it stays a scalar
    Event event = new Event(doc, "msg", Event.Type.FINISH, configFor(List.of("single"), 1));
    ObjectNode fields = event.getDocumentFields();
    assertFalse(fields.get("single").isArray());
    assertEquals("only", fields.get("single").asText());
  }

  @Test
  public void testEnrichmentPreservesNonStringValueTypes() throws Exception {
    Document doc = Document.create("id1", "runId1");
    doc.setField("count", 42);
    doc.setField("ratio", 3.5);
    doc.setField("flag", true);

    Event event = new Event(doc, "msg", Event.Type.FINISH, configFor(List.of("count", "ratio", "flag"), null));
    ObjectNode fields = event.getDocumentFields();

    // raw JsonNode copy preserves the JSON type of each value, not just its string form
    assertTrue(fields.get("count").isInt());
    assertEquals(42, fields.get("count").asInt());
    assertTrue(fields.get("ratio").isNumber());
    assertEquals(3.5, fields.get("ratio").asDouble(), 0.0);
    assertTrue(fields.get("flag").isBoolean());
    assertTrue(fields.get("flag").asBoolean());

    // and the types survive a JSON round-trip through the event topic
    Event roundTripped = Event.fromJsonString(event.toString());
    ObjectNode rtFields = roundTripped.getDocumentFields();
    assertTrue(rtFields.get("count").isInt());
    assertTrue(rtFields.get("flag").isBoolean());
  }

  @Test
  public void testEnrichmentDeepCopiesSoLaterDocumentMutationDoesNotLeak() throws Exception {
    Document doc = Document.create("id1", "runId1");
    doc.addToField("tags", "a");
    doc.addToField("tags", "b");

    Event event = new Event(doc, "msg", Event.Type.FINISH, configFor(List.of("tags"), null));
    ObjectNode fields = event.getDocumentFields();
    assertEquals(2, fields.get("tags").size());

    // mutating the source document after the event is built must not alter the event's captured copy
    doc.addToField("tags", "c");

    assertEquals("event must hold a deep copy independent of the live document",
        2, event.getDocumentFields().get("tags").size());
  }

  @Test
  public void testMaxValuesPerFieldParsedFromConfig() {
    Config config = ConfigFactory.parseMap(Map.of(
        "kafka.event.includeFields", List.of("tags"),
        "kafka.event.maxValuesPerField", 1));
    EventFieldConfig fieldConfig = EventFieldConfig.fromConfig(config);

    Document doc = Document.create("id1", "runId1");
    doc.addToField("tags", "a");
    doc.addToField("tags", "b");

    // confirms fromConfig actually reads maxValuesPerField (not just the test helper constructing it)
    ObjectNode fields = fieldConfig.extract(doc);
    assertEquals(1, fields.get("tags").size());
    assertEquals("a", fields.get("tags").get(0).asText());
  }

  @Test
  public void testFromConfigDisabledWhenAbsentOrEmpty() {
    assertTrue(EventFieldConfig.fromConfig(ConfigFactory.empty()).isEmpty());
    assertTrue(EventFieldConfig.fromConfig(null).isEmpty());

    Config emptyList = ConfigFactory.parseMap(Map.of("kafka.event.includeFields", List.of()));
    assertTrue(EventFieldConfig.fromConfig(emptyList).isEmpty());
  }

  private static EventFieldConfig configFor(List<String> fields, Integer maxValues) {
    java.util.Map<String, Object> map = new java.util.HashMap<>();
    map.put("kafka.event.includeFields", fields);
    if (maxValues != null) {
      map.put("kafka.event.maxValuesPerField", maxValues);
    }
    return EventFieldConfig.fromConfig(ConfigFactory.parseMap(map));
  }
}
