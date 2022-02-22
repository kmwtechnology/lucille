package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TextExtractorTest {

  private StageFactory factory = StageFactory.of(TextExtractor.class);

  @Test
  public void testTextExtractor() throws StageException {
    Stage stage = factory.get("TextExtractorTest/config.conf");

    Document doc = new Document("doc1");
    doc.setField("path", "src/test/resources/TextExtractorTest/tika.txt");
    stage.processDocument(doc);

    System.out.println(doc.toString());

    assertEquals("Hi There!\n", doc.getString("text"));
  }
}
