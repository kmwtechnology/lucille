package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ContainsTest {

  private StageFactory factory = StageFactory.of(Contains.class);

  @Test
  public void testContains() throws StageException {
    Stage stage = factory.get("ContainsTest/config.conf");

    Document doc1 = Document.create("doc1");
    doc1.setField("input1", "contains a MATCH");
    stage.processDocument(doc1);
    assertEquals("FOUND", doc1.getString("output"));

    Document doc2 = Document.create("doc2");
    doc2.setField("input1", "nothing here!");
    doc2.setField("input2", "test, 123, found one!");
    stage.processDocument(doc2);
    assertEquals("FOUND", doc2.getString("output"));

    Document doc3 = Document.create("doc3");
    doc3.setField("input1", "nothing here!");
    doc3.addToField("input1", "here's a match");
    stage.processDocument(doc3);
    assertEquals("FOUND", doc3.getString("output"));

    Document doc4 = Document.create("doc4");
    doc4.setField("notChecked", "this has a match");
    doc4.setField("input1", "nothing here!");
    stage.processDocument(doc4);
    assertFalse(doc4.has("output"));

    Document doc5 = Document.create("doc5");
    doc4.setField("input1", "nothing here!");
    stage.processDocument(doc5);
    assertFalse(doc5.has("output"));
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("ContainsTest/config.conf");
    assertEquals(
        Set.of(
            "output", "contains", "ignoreCase", "name", "fields", "conditions", "value", "class", "conditionPolicy"),
        stage.getLegalProperties());
  }
}
