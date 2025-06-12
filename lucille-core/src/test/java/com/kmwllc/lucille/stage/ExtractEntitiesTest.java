package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ExtractEntitiesTest {

  private StageFactory factory = StageFactory.of(ExtractEntities.class);

  @Test
  public void testExtractEntities() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/config.conf");

    // Ensure that keywords from the dictionary are correctly extracted
    Document doc = Document.create("doc");
    doc.setField("input1", "I live in the United States.");
    stage.processDocument(doc);
    assertEquals("Country name should be extracted from input1", "United States",
        doc.getStringList("output").get(0));

    // Ensure that several fields can be extracted and that payloads work as expected
    Document doc2 = Document.create("doc2");
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
    Document doc = Document.create("doc");
    doc.setField("input1", "I live in the United States.");
    Stage stage = factory.get("ExtractEntitiesTest/configEntityAndPayload.conf");
    stage.processDocument(doc);
    assertEquals("North America", doc.getString("payload"));
    assertEquals("United States", doc.getString("entity"));
  }

  @Test
  public void testDontIgnoreCase() throws StageException {
    Document capitalized = Document.create("uppercased");
    capitalized.setField("input1", "I live in the United States.");

    Document lowercased = Document.create("lowercased");
    lowercased.setField("input1", "I live in the united states.");

    Stage stage = factory.get("ExtractEntitiesTest/dontIgnoreCase.conf");

    stage.processDocument(capitalized);
    stage.processDocument(lowercased);

    assertEquals("United States", capitalized.getString("output"));
    assertFalse(lowercased.has("entity"));
  }

  @Test
  public void testExtractEntitiesSkipsMalformedLine() throws Exception {
    Document doc = Document.create("doc");
    doc.setField("input1", "I live in the United States.");

    // the dictionary file has the "replacement" unicode character which causes the entry to be safely skipped.
    // also, the dictionary file has many empty lines before "Canada", so we want to make sure that is handled appropriately.
    Stage stage = factory.get("ExtractEntitiesTest/configForMalformedDictionary.conf");
    stage.processDocument(doc);

    assertFalse(doc.has("payload"));
    assertFalse(doc.has("entity"));

    Document doc2 = Document.create("doc2");
    doc2.setField("input1", "I live in Canada.");

    stage.processDocument(doc2);

    assertEquals("Canada", doc2.getString("entity"));
    assertEquals("North America", doc2.getString("payload"));
  }

  @Test
  public void testMultipleSourceDestFields() throws StageException {
    Document doc = Document.create("uppercased");
    doc.setField("input1", "I live in the United States.");
    doc.setField("input2", "I live in Taiwan.");
    doc.setField("input3", "I live in Canada.");

    Stage stage = factory.get("ExtractEntitiesTest/multipleFields.conf");
    stage.processDocument(doc);

    assertEquals("United States", doc.getString("output1"));
    assertEquals("Taiwan", doc.getString("output2"));
    assertEquals("Canada", doc.getString("output3"));
  }

  // When usePayloads = false, the destField will be populated with a list of the keywords,
  // even if there are payloads in the dictionary.
  @Test
  public void testDontUsePayloads() throws StageException {
    Stage stage = factory.get("ExtractEntitiesTest/dontUsePayloads.conf");

    Document doc = Document.create("doc");
    doc.setField("source", "The United States had elections in 2024; Canada had elections in 2025.");

    stage.processDocument(doc);

    assertTrue(doc.getStringList("dest").contains("United States"));
    assertTrue(doc.getStringList("dest").contains("Canada"));
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("ExtractEntitiesTest/config.conf");
    assertEquals(
        Set.of(
            "ignore_overlaps",
            "entity_field",
            "source",
            "dest",
            "ignore_case",
            "use_payloads",
            "only_whitespace_separated",
            "update_mode",
            "stop_on_hit",
            "name",
            "only_whole_words",
            "conditions",
            "class",
            "dictionaries",
            "conditionPolicy",
            "gcp", "azure", "s3"),
        stage.getLegalProperties());
  }

  @Test
  public void testBadDictionaryPath() {
    // bad path to a dictionary, causes an error building the trie
    assertThrows(StageException.class,
        () -> factory.get("ExtractEntitiesTest/badDictionaryFilePath.conf"));
  }
}
