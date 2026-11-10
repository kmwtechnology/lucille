package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.junit.Test;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.dashjoin.jsonata.Jsonata;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.kmwllc.lucille.core.DocumentException;
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
    assertEquals(List.of("foo", "id"), doc.getStringList("keys"));
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

  // **NOTE**: For the following three tests, each one has a different type for "source" - a raw object, a JSON object,
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

  @Test
  public void testExceptionDuringTransformation() throws StageException {
    Stage stage = factory.get("ApplyJSONataTest/badEvaluation.conf");
    // Expression: $invalidFunction(field)
    Document doc = Document.create("doc1");

    doc.setField("source", mapper.createObjectNode()
        .set("value", mapper.createObjectNode()
            .put("a", "b")));

    // should not throw an exception, but should log a warning and not make any changes to the document.
    stage.processDocument(doc);

    assertEquals("doc1", doc.getId());
    assertNull(doc.getJson("destination"));
  }

  @Test
  public void testApplyReservedFields() throws StageException {
    // We have updated the reserved fields to use underscores instead (ex. ___chlidren vs .children), because if not handled correctly,
    //  it would break this use case where we want to use the ___children field with nested funtionality.

    Stage stage = factory.get("ApplyJSONataTest/reservedFields.conf");
    // Expression: $[0].enabled ? $[0].enabled : false
    Document child0 = Document.create("child0");
    child0.setField("enabled", true);
    Document child1 = Document.create("child1");
    child1.setField("enabled", false);
    Document doc = Document.create("doc1");
    doc.addChild(child0);
    doc.addChild(child1);

    stage.processDocument(doc);

    assertEquals(true, child0.getBoolean("enabled"));
    assertEquals(true, doc.getBoolean("isEnabled"));
  }

  @Test
  public void testTransform() throws Exception {
    Document doc = Document.createFromJson("{\"id\":\"id\",\"foo\": \"bar\"}");
    doc.setField("bytes", new byte[]{1, 2, 3});

    // test mutating a reserved field
    Jsonata mutateReservedExpr = Jsonata.jsonata("{\"id\":\"diff\",\"foo\": \"bar\"}");
    assertThrows(DocumentException.class, () -> ApplyJSONata.transform(doc, mutateReservedExpr));
    assertEquals(3, doc.getFieldNames().size());
    assertEquals("id", doc.getId());
    assertEquals("bar", doc.getString("foo"));
    assertArrayEquals(new byte[]{1, 2, 3}, doc.getBytes("bytes"));

    // test mutation does not create object
    Jsonata mutateIntoArray = Jsonata.jsonata("[1, 2, 3]");
    Jsonata mutateIntoNum = Jsonata.jsonata("3");
    assertThrows(DocumentException.class, () -> ApplyJSONata.transform(doc, mutateIntoArray));
    assertThrows(DocumentException.class, () -> ApplyJSONata.transform(doc, mutateIntoNum));
    assertEquals(3, doc.getFieldNames().size());
    assertEquals("id", doc.getId());
    assertEquals("bar", doc.getString("foo"));
    assertArrayEquals(new byte[]{1, 2, 3}, doc.getBytes("bytes"));

    // test valid mutation
    Jsonata mutateFoo = Jsonata.jsonata("$merge([$, {\"foo\": $substring(foo, 2)}])");
    ApplyJSONata.transform(doc, mutateFoo);
    assertEquals(3, doc.getFieldNames().size());
    assertEquals("id", doc.getId());
    assertEquals("r", doc.getString("foo"));
    assertArrayEquals(new byte[]{1, 2, 3}, doc.getBytes("bytes"));
  }

  @Test
  public void testTransformNestedJson() throws Exception {
    Document doc = Document.createFromJson("{\"id\":\"id\",\"foo\": \"bar\"}");
    doc.setField("nested", mapper.createObjectNode()
        .put("bytes", new byte[]{1, 2, 3})
        .put("int", 1)
        .put("string", "text, with; delimiters"));
    doc.setNestedJson("nested.bool", BooleanNode.getTrue());

    // test valid mutation with new destination
    Jsonata mutateString = Jsonata.jsonata("$trim($split(string, /[,;、，،؛；]/)[1])");
    ApplyJSONata.transform(doc, mutateString, "nested", "nested.newString");
    assertEquals(3, doc.getFieldNames().size());
    assertEquals("id", doc.getId());
    assertEquals("text, with; delimiters", doc.getNestedJson("nested.string").asText());
    assertEquals("with", doc.getNestedJson("nested.newString").asText());
    assertArrayEquals(new byte[]{1, 2, 3}, doc.getNestedJson("nested.bytes").binaryValue());
    assertEquals(1, doc.getNestedJson("nested.int").asInt());

    // test null source node
    assertThrows(DocumentException.class, () ->  ApplyJSONata.transform(doc, mutateString, "nested.nonexistent", "nested.null"));
    doc.setNestedJson("nested.null", mapper.nullNode());
    assertThrows(DocumentException.class, () ->  ApplyJSONata.transform(doc, mutateString, "nested.null", "nested.null"));

    // test valid mutation with same destination
    Jsonata mutateInt = Jsonata.jsonata("int + 10");
    ApplyJSONata.transform(doc, mutateInt, "nested", "nested.int");
    assertEquals(3, doc.getFieldNames().size());
    assertEquals("id", doc.getId());
    assertEquals("text, with; delimiters", doc.getNestedJson("nested.string").asText());
    assertEquals("with", doc.getNestedJson("nested.newString").asText());
    assertArrayEquals(new byte[]{1, 2, 3}, doc.getNestedJson("nested.bytes").binaryValue());
    assertEquals(11, doc.getNestedJson("nested.int").asInt());

    // test null destination node
    ApplyJSONata.transform(doc, mutateString, "nested", null);
    assertEquals("with", doc.getString("nested"));
    assertEquals(3, doc.getFieldNames().size());
    assertEquals("id", doc.getId());
    assertNull(doc.getNestedJson("nested.string"));
    assertNull(doc.getNestedJson("nested.newString"));
    assertNull(doc.getNestedJson("nested.bytes"));
    assertNull(doc.getNestedJson("nested.int"));
  }

  @Test
  public void testTransformCannotAddReservedOrEmptyField() throws Exception {
    Document doc = Document.create("id");
    doc.setField("foo", "bar");

    Jsonata addDropped = Jsonata.jsonata("$merge([$, {\"___dropped\": true}])");
    assertThrows(DocumentException.class, () -> ApplyJSONata.transform(doc, addDropped));
    Jsonata addEmptyName = Jsonata.jsonata("$merge([$, {\"\": 1}])");
    assertThrows(DocumentException.class, () -> ApplyJSONata.transform(doc, addEmptyName));

    // the document is unchanged
    assertFalse(doc.isDropped());
    assertEquals(List.of("id", "foo"), List.copyOf(doc.getFieldNames()));
    assertEquals("bar", doc.getString("foo"));
  }
}
