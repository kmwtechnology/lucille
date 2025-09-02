package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class ExtractEntitiesFSTTest {

  private StageFactory factory = StageFactory.of(com.kmwllc.lucille.stage.ExtractEntitiesFST.class);

  @Test
  public void testExtractEntities() throws Exception {
    Stage stage = factory.get("ExtractEntitiesFSTTest/config.conf");

    Document doc = Document.create("doc");
    doc.setField("input1", "I live in the United States.");
    stage.processDocument(doc);
    assertEquals("Country payload should be extracted from input1", "North America",
        doc.getStringList("output").get(0));

    Document doc2 = Document.create("doc2");
    doc2.setField("input1", "I live in China but am from taiwan");
    doc2.setField("input2", "I live in Canada");
    doc2.setField("input3", "I live in USSR");
    stage.processDocument(doc2);
    List<String> payloads = doc2.getStringList("output");
    assertEquals("Asia", payloads.get(0));
    assertEquals("Asia", payloads.get(1));
    assertEquals("North America", payloads.get(2));
    assertEquals("Russia", payloads.get(3));
  }

  @Test
  public void testExtractEntitiesAndPayloads() throws Exception {
    Document doc = Document.create("doc");
    doc.setField("input1", "I live in the United States.");
    Stage stage = factory.get("ExtractEntitiesFSTTest/configEntityAndPayload.conf");
    stage.processDocument(doc);
    assertEquals("North America", doc.getString("payload"));
    assertEquals("united states", doc.getString("entity"));
  }


  @Test
  public void testCaseInsensitivity() throws StageException {
    Document capitalized = Document.create("uppercased");
    capitalized.setField("input1", "I live in the United States.");

    Document lowercased = Document.create("lowercased");
    lowercased.setField("input1", "i live in the united states.");

    Stage stage = factory.get("ExtractEntitiesFSTTest/config.conf");
    stage.processDocument(capitalized);
    stage.processDocument(lowercased);

    assertEquals("North America", capitalized.getString("output"));
    assertEquals("North America", lowercased.getString("output"));
  }


  @Test
  public void testExtractEntitiesSkipsMalformedLine() throws Exception {
    Document doc = Document.create("doc");
    doc.setField("input1", "I live in the United States.");

    // the dictionary file has "United States�,North America" which won’t match "United States"
    // Ensure we don’t get an entity/payload from that line, but later entries (e.g., Canada) still work.
    Stage stage = factory.get("ExtractEntitiesFSTTest/configForMalformedDictionary.conf");
    stage.processDocument(doc);

    assertFalse(doc.has("payload"));
    assertFalse(doc.has("entity"));

    Document doc2 = Document.create("doc2");
    doc2.setField("input1", "I live in Canada.");
    stage.processDocument(doc2);

    assertEquals("canada", doc2.getString("entity"));
    assertEquals("North America", doc2.getString("payload"));
  }


  @Test
  public void testMultipleSourceDestFields() throws StageException {
    Document doc = Document.create("doc");
    doc.setField("input1", "I live in the United States.");
    doc.setField("input2", "I live in Taiwan.");
    doc.setField("input3", "I live in Canada.");

    Stage stage = factory.get("ExtractEntitiesFSTTest/multipleFields.conf");
    stage.processDocument(doc);

    assertEquals("North America", doc.getString("output1"));
    assertEquals("Asia",          doc.getString("output2"));
    assertEquals("North America", doc.getString("output3"));
  }

  // When usePayloads = false, the destField will be populated with a list of the keywords,
  // even if there are payloads in the dictionary.
  @Test
  public void testDontUsePayloads() throws StageException {
    Stage stage = factory.get("ExtractEntitiesFSTTest/dontUsePayloads.conf");

    Document doc = Document.create("doc");
    doc.setField("source", "The United States had elections in 2024; Canada had elections in 2025.");

    stage.processDocument(doc);

    List<String> terms = doc.getStringList("dest");
    assertTrue(terms.contains("united states"));
    assertTrue(terms.contains("canada"));
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("ExtractEntitiesFSTTest/config.conf");
    assertEquals(
        Set.of(
            "ignore_overlaps",
            "entity_field",
            "source",
            "dest",
            "use_payloads",
            "update_mode",
            "stop_on_hit",
            "name",
            "conditions",
            "class",
            "dictionaries",
            "conditionPolicy",
            "gcp", "azure", "s3"
        ),
        stage.getLegalProperties()
    );
  }

  @Test
  public void testBadDictionaryPath() {
    assertThrows(StageException.class,
        () -> factory.get("ExtractEntitiesFSTTest/badDictionaryFilePath.conf"));
  }
}