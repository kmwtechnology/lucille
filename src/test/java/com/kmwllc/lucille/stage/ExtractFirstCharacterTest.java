package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;
import static org.junit.Assert.*;

public class ExtractFirstCharacterTest {

  StageFactory factory = StageFactory.of(ExtractFirstCharacter.class);

  @Test
  public void testExtractFirstCharacter() throws StageException {
    Stage stage = factory.get("ExtractFirstCharacterTest/config.conf");

    Document doc1 = new Document("doc1");
    doc1.setField("input1", "this is an input");
    stage.processDocument(doc1);
    assertEquals("t", doc1.getString("output1"));

    Document doc2 = new Document("doc2");
    doc2.setField("input1", "1234567");
    doc2.setField("input2", "Hello, this is valid");
    stage.processDocument(doc2);
    assertEquals("nonalpha", doc2.getString("output1"));
    assertEquals("H", doc2.getString("output2"));
    assertFalse(doc2.has("output3"));
  }

}
