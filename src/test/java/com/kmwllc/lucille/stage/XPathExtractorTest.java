package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class XPathExtractorTest {

  private StageFactory factory = StageFactory.of(XPathExtractor.class);

  @Test
  public void testXPathExtractor() throws StageException {
    Stage stage = factory.get("XPathExtractorTest/config.conf");

    Document doc1 = new Document("doc1");
    doc1.setField("xml", "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "\n" +
      "<bookstore>\n" +
      "\n" +
      "<book>\n" +
      "  <title lang=\"en\">Harry Potter</title>\n" +
      "  <price>29.99</price>\n" +
      "</book>\n" +
      "\n" +
      "<book>\n" +
      "  <title lang=\"en\">Learning XML</title>\n" +
      "  <price>39.95</price>\n" +
      "</book>\n" +
      "\n" +
      "</bookstore>");

    stage.processDocument(doc1);

    assertEquals("Harry Potter\n" +
      "  29.99\n" +
      "\n" +
      "\n" +
      "\n" +
      "  Learning XML\n" +
      "  39.95", doc1.getString("output1"));
  }
}
