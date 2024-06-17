package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import java.util.List;
import org.junit.Test;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

    StageWithDest.processDocument(doc);
    assertEquals(3, doc.getFieldNames().size());
    assertEquals("id", doc.getId());
    assertEquals(bar, doc.getJson("source"));
    assertEquals(foo, doc.getJson("destination"));

    stageWithoutDest.processDocument(doc2);
    System.out.println(doc2.toString());
    assertEquals(2, doc2.getFieldNames().size());
    assertEquals("id", doc2.getId());
    assertEquals(foo, doc2.getJson("source"));
  }
}
