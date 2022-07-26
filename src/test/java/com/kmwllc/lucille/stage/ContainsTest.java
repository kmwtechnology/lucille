package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;
import static org.junit.Assert.*;

public class ContainsTest {

  private StageFactory factory = StageFactory.of(Contains.class);

  @Test
  public void testContains() throws StageException {
    Stage stage = factory.get("ContainsTest/config.conf");

    JsonDocument doc1 = new JsonDocument("doc1");
    doc1.setField("input1", "contains a MATCH");
    stage.processDocument(doc1);
    assertEquals("FOUND", doc1.getString("output"));

    JsonDocument doc2 = new JsonDocument("doc2");
    doc2.setField("input1", "nothing here!");
    doc2.setField("input2", "test, 123, found one!");
    stage.processDocument(doc2);
    assertEquals("FOUND", doc2.getString("output"));

    JsonDocument doc3 = new JsonDocument("doc3");
    doc3.setField("input1", "nothing here!");
    doc3.addToField("input1", "here's a match");
    stage.processDocument(doc3);
    assertEquals("FOUND", doc3.getString("output"));

    JsonDocument doc4 = new JsonDocument("doc4");
    doc4.setField("notChecked", "this has a match");
    doc4.setField("input1", "nothing here!");
    stage.processDocument(doc4);
    assertFalse(doc4.has("output"));

    JsonDocument doc5 = new JsonDocument("doc5");
    doc4.setField("input1", "nothing here!");
    stage.processDocument(doc5);
    assertFalse(doc5.has("output"));
  }


}
