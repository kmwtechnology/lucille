package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;
import static org.junit.Assert.*;

public class ContainsTest {

  private StageFactory factory = StageFactory.of(Contains.class);

  @Test
  public void testContains() throws StageException {
    Stage stage = factory.get("ContainsTest/config.conf");

    Document doc1 = new Document("doc1");
    doc1.setField("input1", "contains a MATCH");
    stage.processDocument(doc1);
    assertEquals("FOUND", doc1.getString("output"));

    Document doc2 = new Document("doc2");
    doc2.setField("input1", "nothing here!");
    doc2.setField("input2", "test, 123, found one!");
    stage.processDocument(doc2);
    assertEquals("FOUND", doc2.getString("output"));

    Document doc3 = new Document("doc3");
    doc3.setField("input1", "nothing here!");
    doc3.addToField("input1", "here's a match");
    stage.processDocument(doc3);
    assertEquals("FOUND", doc3.getString("output"));
  }


}
