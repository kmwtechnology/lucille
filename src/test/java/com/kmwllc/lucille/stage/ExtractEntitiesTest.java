package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class ExtractEntitiesTest {

  @Test
  public void testExtractEntities() throws Exception {
    Config config = ConfigFactory.load("ExtractEntitiesTest/config.conf");
    ExtractEntities stage = new ExtractEntities(config);
    stage.start();

    Document doc = new Document("doc");
    doc.setField("input1", "I live in the United States.");
    stage.processDocument(doc);
    assertEquals("Country name should be extracted from input1", "United States",
        doc.getStringList("output").get(0));

    Document doc2 = new Document("doc2");
    doc2.setField("input1", "I live in China but am from taiwan");
    doc2.setField("input2", "I live in Canada");
    doc2.setField("input3", "I live in USSR");
    stage.processDocument(doc2);
    List<String> tokens = doc2.getStringList("output");
    assertEquals("Country names from input1 should be extracted to output", "China", tokens.get(0));
    assertEquals("Country names from input1 should be extracted to output", "Taiwan", tokens.get(1));
    assertEquals("Country names from input2 should be extracted to output", "Canada", tokens.get(2));
    assertEquals("Country names from input3 should be extracted to output", "Russia", tokens.get(3));
  }

}
