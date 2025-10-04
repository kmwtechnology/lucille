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
    doc2.setField("input1", "I live in China but am from Taiwan");
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
    assertEquals("United States", doc.getString("entity"));
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

    assertEquals("Canada", doc2.getString("entity"));
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
    assertTrue(terms.contains("United States"));
    assertTrue(terms.contains("Canada"));
  }

  @Test
  public void testBadDictionaryPath() {
    assertThrows(StageException.class,
        () -> factory.get("ExtractEntitiesFSTTest/badDictionaryFilePath.conf"));
  }

  @Test
  public void testIgnoreOverlapsFalseEmitsAll() throws Exception {
    Stage stage = factory.get("ExtractEntitiesFSTTest/configOverlapFalse.conf");
    Document doc = Document.create("doc");
    doc.setField("text", "I moved to New York City last year.");
    stage.processDocument(doc);
    List<String> out = doc.getStringList("out");
    assertTrue(out.contains("NY"));
    assertTrue(out.contains("NYC"));
  }

  @Test
  public void testIgnoreOverlapsTrueEmitsLongest() throws Exception {
    Stage stage = factory.get("ExtractEntitiesFSTTest/configOverlapTrue.conf");
    Document doc = Document.create("doc");
    doc.setField("text", "I moved to New York City last year.");
    stage.processDocument(doc);
    List<String> out = doc.getStringList("out");
    assertEquals(1, out.size());
    assertEquals("NYC", out.get(0));
  }

  @Test
  public void testStopOnHitOnlyFirst() throws Exception {
    Stage stage = factory.get("ExtractEntitiesFSTTest/configStopOnHit.conf");
    Document doc = Document.create("doc");
    doc.setField("input1", "United States and Canada are neighbors.");
    stage.processDocument(doc);
    List<String> out = doc.getStringList("output");
    assertEquals(1, out.size());
    assertEquals("North America", out.get(0));
  }

  @Test
  public void testWhitespace() throws Exception {
    Stage stage = factory.get("ExtractEntitiesFSTTest/configOverlapTrue.conf");
    Document doc = Document.create("doc");
    doc.setField("text", "I moved to         New      York      City      last year.");
    stage.processDocument(doc);
    List<String> out = doc.getStringList("out");
    assertEquals(1, out.size());
    assertEquals("NYC", out.get(0));
  }

  @Test
  public void testNearMatch() throws Exception {
    Stage stage = factory.get("ExtractEntitiesFSTTest/configOverlapTrue.conf");
    Document doc = Document.create("doc");
    doc.setField("text", "New Yorkk City New City York Neww York City Neww York City New NewYork NewYorkCity NewYork City");
    stage.processDocument(doc);
    assertFalse(doc.has("out"));
  }

  @Test
  public void testMultipleSameMatches() throws Exception {
    Stage stage = factory.get("ExtractEntitiesFSTTest/configOverlapTrue.conf");
    Document doc = Document.create("doc");
    doc.setField("text", "New York New York New York New York");
    stage.processDocument(doc);
    List<String> out = doc.getStringList("out");
    assertEquals(4, out.size());
  }

  @Test
  public void testPayloadFallbackWhenMissingPayload() throws Exception {
    Stage stage = factory.get("ExtractEntitiesFSTTest/configMissingPayloadFallback.conf");

    Document doc = Document.create("doc");
    doc.setField("input1", "I live in Taiwan.");
    stage.processDocument(doc);

    assertEquals("Taiwan", doc.getString("payload"));
    assertEquals("Taiwan", doc.getString("entity"));
  }

  @Test
  public void testDuplicateKeysAcrossMultipleDictionariesDeduped() throws Exception {
    Stage stage = factory.get("ExtractEntitiesFSTTest/configDupAcrossDicts.conf");

    Document doc = Document.create("doc");
    doc.setField("text", "Canada borders the United States. Canada is north.");
    stage.processDocument(doc);

    List<String> out = doc.getStringList("out");
    long count = out.stream().filter("North America"::equals).count();
    assertEquals(2, count);
  }

  @Test
  public void testLowercaseOnlyPunctSplitAndLowercase() throws Exception {
    Stage stage = factory.get("ExtractEntitiesFSTTest/configLowercaseOnly.conf");

    Document doc = Document.create("doc");
    doc.setField("input1", "i live in the united-states.");
    stage.processDocument(doc);

    assertEquals("North America", doc.getString("payload"));
    assertEquals("united states", doc.getString("entity"));
  }

  @Test
  public void testCustomBreaksWhitespaceOnlyNoRegex() throws Exception {
    Stage stage = factory.get("ExtractEntitiesFSTTest/configCustomWhitespaceOnly.conf");

    Document docA = Document.create("docA");
    docA.setField("input1", "I live in United States");
    stage.processDocument(docA);
    assertEquals("North America", docA.getString("payload"));

    Document docB = Document.create("docB");
    docB.setField("input1", "I live in United-States");
    stage.processDocument(docB);
    assertFalse(docB.has("payload"));
  }

  @Test
  public void testCustomBreaksWithAllPunctRegex() throws Exception {
    Stage stage = factory.get("ExtractEntitiesFSTTest/configCustomRegexAllPunct.conf");

    Document doc = Document.create("doc");
    doc.setField("input1", "United,States United-States United;States,, United States");
    stage.processDocument(doc);

    List<String> payloads = doc.getStringList("payload");
    assertNotNull(payloads);
    assertEquals(4, payloads.size());
    for (String p : payloads) {
      assertEquals("North America", p);
    }
  }

  @Test
  public void testDefaultNoLowercaseCaseSensitive() throws Exception {
    Stage stage = factory.get("ExtractEntitiesFSTTest/configDefaultCaseSensitive.conf");

    Document doc = Document.create("doc");
    doc.setField("input1", "i live in the united states.");
    stage.processDocument(doc);

    assertFalse("Default should be case-sensitive without ignore_case=true", doc.has("payload"));

    Document doc2 = Document.create("doc2");
    doc2.setField("input1", "I live in the United States.");
    stage.processDocument(doc2);
    assertEquals("North America", doc2.getString("payload"));
  }

}