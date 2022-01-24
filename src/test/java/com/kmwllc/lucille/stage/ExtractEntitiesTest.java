package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class ExtractEntitiesTest {

  private StageFactory factory = StageFactory.of(ExtractEntities.class);

  @Test
  public void testExtractEntities() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/config.conf");

    // Ensure that keywords from the dictionary are correctly extracted
    Document doc = new Document("doc");
    doc.setField("input1", "I live in the United States.");
    stage.processDocument(doc);
    assertEquals("Country name should be extracted from input1", "United States",
        doc.getStringList("output").get(0));

    // Ensure that several fields can be extracted and that payloads work as expected
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
  
  @Test
  public void testExtractEntitiesAndPayloads() throws Exception {
    Document doc = new Document("doc");
    doc.setField("input1", "I live in the United States.");
    Stage stage = factory.get("ExtractEntitiesTest/configEntityAndPayload.conf");
    stage.processDocument(doc);
    assertEquals("North America", doc.getString("payload"));
    assertEquals("United States", doc.getString("entity"));
  }
  
}
