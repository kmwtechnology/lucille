package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.junit.Test;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;

public class ApplyJSONataTest {

  private final StageFactory factory = StageFactory.of(ApplyJSONata.class);
  private final ObjectMapper mapper = new ObjectMapper();

  @Test 
  public void testConfig() {
    assertThrows(StageException.class, () -> factory.get("ApplyJSONataTest/noExpression.conf"));
  }

  @Test 
  public void testInvalidExpression() {
    assertThrows(StageException.class, () -> factory.get("ApplyJSONataTest/invalidExpression.conf"));
  }

  // Expression: {"id": id, "keys": $keys()}
  @Test
  public void testApplyToFullDocument() throws StageException {
    Stage invalidStage = factory.get("ApplyJSONataTest/fullInvalid.conf");
    Stage validStage = factory.get("ApplyJSONataTest/fullValid.conf");

    Document doc = Document.create("abc123");
    doc.setField("foo", "bar");

    invalidStage.processDocument(doc);
    assertEquals(2, doc.getFieldNames().size());
    assertEquals("abc123", doc.getId());
    assertEquals("bar", doc.getString("foo"));

    validStage.processDocument(doc);
    assertEquals(2, doc.getFieldNames().size());
    assertEquals("abc123", doc.getId());
    assertEquals(List.of("id", "foo"), doc.getStringList("keys"));
  }

  // Expression: "foo" (which just returns, again, "foo")
  @Test 
  public void testApplyToField() throws StageException {
    Stage stageWithDest = factory.get("ApplyJSONataTest/withDest.conf");
    Stage stageWithoutDest = factory.get("ApplyJSONataTest/withoutDest.conf");

    JsonNode bar = mapper.convertValue("bar", JsonNode.class);
    JsonNode foo = mapper.convertValue("foo", JsonNode.class);
    Document doc = Document.create("id");
    doc.setField("source", bar);

    Document doc2 = Document.create("id");
    doc2.setField("source", bar);

    stageWithDest.processDocument(doc);
    assertEquals(3, doc.getFieldNames().size());
    assertEquals("id", doc.getId());
    assertEquals(bar, doc.getJson("source"));
    assertEquals(foo, doc.getJson("destination"));

    stageWithoutDest.processDocument(doc2);
    assertEquals(2, doc2.getFieldNames().size());
    assertEquals("id", doc2.getId());
    assertEquals(foo, doc2.getJson("source"));

    Document docWithoutSource = Document.create("no_source");
    docWithoutSource.setField("something_else", 12345);
    stageWithDest.processDocument(docWithoutSource);
    assertEquals("no_source", docWithoutSource.getId());
    assertEquals((Integer) 12345, docWithoutSource.getInt("something_else"));
  }

  // **NOTE**: For the following three tests, the expression is field.value.
  // We want to make sure we can correctly return ObjectNodes that are both list and objects,
  // as well as "raw" values.
  @Test
  public void testAccessValue() throws StageException {
    Stage stage = factory.get("ApplyJSONataTest/fieldAccessValue.conf");

    Document doc = Document.create("id");
    doc.setField("source", mapper.createObjectNode()
        .set("field", mapper.createObjectNode()
            .put("value", "8.2")));

    stage.processDocument(doc);
    assertEquals(8.2, doc.getDouble("dest"), 0.0001);
  }

  @Test
  public void testAccessJson() throws StageException {
    Stage stage = factory.get("ApplyJSONataTest/fieldAccessValue.conf");

    Document doc = Document.create("id");
    doc.setField("source", mapper.createObjectNode()
        .set("field", mapper.createObjectNode()
            .set("value", mapper.createObjectNode()
                // (AQID)
                .put("a", new byte[] {1, 2, 3}))));

    stage.processDocument(doc);
    assertEquals("{\"a\":\"AQID\"}", doc.getJson("dest").toString());
  }

  @Test
  public void testAccessList() throws StageException {
    Stage stage = factory.get("ApplyJSONataTest/fieldAccessValue.conf");

    Document doc = Document.create("id");
    doc.setField("source", mapper.createObjectNode()
        .set("field", mapper.createObjectNode()
            .set("value", mapper.createArrayNode().add(0).add(1).add(2))));

    stage.processDocument(doc);
    assertEquals("[0,1,2]", doc.getJson("dest").toString());
  }

  // **NOTE**: For the following three tests, each one hasa different type for "source" - a raw object, a JSON object,
  // and a JSON array. The expression used is just $string(), which can be easily applied to all of these.
  @Test
  public void testSourceValue() throws StageException {
    Stage stage = factory.get("ApplyJSONataTest/sourceToString.conf");

    Document doc = Document.create("id");
    doc.setField("source", 1);

    stage.processDocument(doc);
    assertTrue(doc.getJson("dest").isTextual());
    assertEquals("\"1\"", doc.getJson("dest").toString());
  }

  @Test
  public void testSourceJson() throws StageException {
    Stage stage = factory.get("ApplyJSONataTest/sourceToString.conf");

    Document doc = Document.create("id");
    // source: {"a": "b"}
    doc.setField("source", mapper.createObjectNode().put("a", "b"));

    stage.processDocument(doc);
    assertTrue(doc.getJson("dest").isTextual());
    // yes, those quotes are returned as escaped by $string()
    assertEquals("\"{\\\"a\\\":\\\"b\\\"}\"", doc.getJson("dest").toString());
  }

  @Test
  public void testSourceList() throws StageException {
    Stage stage = factory.get("ApplyJSONataTest/sourceToString.conf");

    Document doc = Document.create("id");
    doc.setField("source", mapper.createArrayNode().add(1).add(2).add(3));

    stage.processDocument(doc);
    assertTrue(doc.getJson("dest").isTextual());
    assertEquals("\"[1,2,3]\"", doc.getJson("dest").toString());
  }

  // Expression: $exists(field.value) ? $number(field.value) : null
  @Test
  public void testAccessFieldFunction() throws StageException {
    Stage stage = factory.get("ApplyJSONataTest/conditionallyAccessFieldValue.conf");

    Document hasValue = Document.create("id");
    hasValue.setField("source", mapper.createObjectNode()
        .set("field", mapper.createObjectNode()
            .put("value", 1)));

    stage.processDocument(hasValue);
    assertEquals((Integer) 1, hasValue.getInt("dest"));

    Document doesntHaveValue = Document.create("noVal");
    doesntHaveValue.setField("source", mapper.createObjectNode()
        .put("field", "text"));

    assertNull(doesntHaveValue.getInt("dest"));
  }

  @Test
  public void testAccessFieldTransform() throws StageException {
    Stage stage = factory.get("ApplyJSONataTest/fieldAccessValueTransformation.conf");

    Document doc = Document.create("123");

    /*
    Expression: {"id": id, "value": field.value} (an in-place transformation, no source field)
    {
      "id": "123",
      "field": {
        "value": {
          "a": "b"
        }
      }
    }
     */
    doc.setField("field", mapper.createObjectNode()
        .set("value", mapper.createObjectNode()
            .put("a", "b")));

    stage.processDocument(doc);

    assertEquals("123", doc.getId());
    // should no longer have "field".
    assertFalse(doc.has("field"));
    assertEquals("{\"a\":\"b\"}", doc.getJson("value").toString());
  }
}
