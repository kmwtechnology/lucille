package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import java.util.List;
import org.junit.Test;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;

public class ApplyJSONNataTest {

  private final StageFactory factory = StageFactory.of(ApplyJSONNata.class);
  private final ObjectMapper mapper = new ObjectMapper();

  @Test 
  public void testConfig() {
    assertThrows(StageException.class, () -> factory.get("ApplyJSONNataTest/noExpression.conf"));
  }

  @Test 
  public void testInvalidExpression() {
    assertThrows(StageException.class, () -> factory.get("ApplyJSONNataTest/invalidExpression.conf"));
  }

  // Expression: {"id": "id", "keys": $keys()}
  @Test
  public void testApplyToFullDocument() throws StageException {
    Stage invalidStage = factory.get("ApplyJSONNataTest/fullInvalid.conf");
    Stage validStage = factory.get("ApplyJSONNataTest/fullValid.conf");

    Document doc = Document.create("id");
    doc.setField("foo", "bar");

    invalidStage.processDocument(doc);
    assertEquals(2, doc.getFieldNames().size());
    assertEquals("id", doc.getId());
    assertEquals("bar", doc.getString("foo"));

    validStage.processDocument(doc);
    assertEquals(2, doc.getFieldNames().size());
    assertEquals("id", doc.getId());
    assertEquals(List.of("id", "foo"), doc.getStringList("keys"));
  }

  // Expression: "foo" (which just returns, again, "foo")
  @Test 
  public void testApplyToField() throws StageException {
    Stage stageWithDest = factory.get("ApplyJSONNataTest/withDest.conf");
    Stage stageWithoutDest = factory.get("ApplyJSONNataTest/withoutDest.conf");

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

  // Expression: field.value
  @Test
  public void testJsonataFieldAccessForValue() throws StageException {
    Stage stage = factory.get("ApplyJSONNataTest/fieldAccessValue.conf");

    Document doc = Document.create("id");
    doc.setField("source", mapper.createObjectNode()
        .set("field", mapper.createObjectNode()
            .put("value", "8.2")));

    stage.processDocument(doc);
    assertEquals(8.2, doc.getDouble("dest"), 0.0001);
  }

  // Expression: field
  @Test
  public void testJsonataFieldAccessForJson() throws StageException {
    Stage stage = factory.get("ApplyJSONNataTest/fieldAccessJson.conf");

    Document doc = Document.create("id");
    doc.setField("source", mapper.createObjectNode()
        .set("field", mapper.createObjectNode()
            .put("value", "8.2")));

    stage.processDocument(doc);

    JsonNode destNode = doc.getJson("dest");
    assertEquals("{\"value\":\"8.2\"}", destNode.toString());
  }
}
