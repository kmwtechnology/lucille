package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;
import static org.junit.Assert.*;

public class ExtractFirstCharacterTest {

  StageFactory factory = StageFactory.of(ExtractFirstCharacter.class);

  @Test
  public void testExtractFirstCharacter() throws StageException {
    Stage stage = factory.get("ExtractFirstCharacterTest/config.conf");

    JsonDocument doc1 = new JsonDocument("doc1");
    doc1.setField("input1", "this is an input");
    stage.processDocument(doc1);
    assertEquals("t", doc1.getString("output1"));

    JsonDocument doc2 = new JsonDocument("doc2");
    doc2.setField("input1", "1234567");
    doc2.setField("input2", "Hello, this is valid");
    stage.processDocument(doc2);
    assertEquals("nonalpha", doc2.getString("output1"));
    assertEquals("H", doc2.getString("output2"));
    assertFalse(doc2.has("output3"));
  }

  @Test
  public void testReplacement() throws StageException {
   Stage stage = factory.get("ExtractFirstCharacterTest/replacement.conf");

   JsonDocument doc = new JsonDocument("doc");
   doc.setField("input1", "12345");
   doc.setField("input2", "valid");
   stage.processDocument(doc);
   assertEquals("not a letter", doc.getString("output1"));
   assertEquals("v", doc.getString("output2"));
  }

  @Test
  public void testSkip() throws StageException {
    Stage stage = factory.get("ExtractFirstCharacterTest/skip.conf");

    JsonDocument doc = new JsonDocument("doc");
    doc.setField("input1", "testing");
    doc.setField("input2", "500 bottles of beer on the wall");
    stage.processDocument(doc);
    assertFalse(doc.has("output2"));
    assertEquals("t", doc.getString("output1"));
  }

}
