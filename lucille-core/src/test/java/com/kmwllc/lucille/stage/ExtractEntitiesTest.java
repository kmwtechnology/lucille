package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class ExtractEntitiesTest {

  private StageFactory factory = StageFactory.of(com.kmwllc.lucille.stage.ExtractEntities.class);

  @Test
  public void testStartOfStringBoundary() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/startOfString.conf");

    Document doc = Document.create("doc1");
    doc.setField("input1", "United States is large");
    stage.processDocument(doc);

    assertEquals(List.of("North America"), doc.getStringList("output"));
  }

  @Test
  public void testEndOfStringBoundary() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/endOfString.conf");

    Document doc = Document.create("doc2");
    doc.setField("input1", "I live in Canada");
    stage.processDocument(doc);

    assertEquals(List.of("North America"), doc.getStringList("output"));
  }

  @Test
  public void testPunctuationBoundaries() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/punctBoundaries.conf");

    Document doc = Document.create("doc3");
    doc.setField("input1", "I live in the (United States).");
    stage.processDocument(doc);

    assertEquals(List.of("North America"), doc.getStringList("output"));
  }

  @Test
  public void testLetterOnRightBlocks() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/letterRightBlocks.conf");

    Document doc = Document.create("doc4");
    doc.setField("input1", "Vexatronics");
    stage.processDocument(doc);

    assertFalse("Should not match when a letter immediately follows the term", doc.has("output"));
  }

  @Test
  public void testDigitOnRightDoesNotBlock() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/digitRightOk.conf");

    Document doc = Document.create("doc5");
    doc.setField("input1", "Model Z57 is discontinued"); // right neighbor of "Z5" is a digit
    stage.processDocument(doc);

    assertEquals(List.of("Code"), doc.getStringList("output"));
  }

  @Test
  public void testDigitOnlyInsideNumber() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/digitInside.conf");

    // Embedded within a longer number, left boundary present
    Document docA = Document.create("doc6A");
    docA.setField("input1", "The code is 90731 today.");
    stage.processDocument(docA);
    assertEquals(List.of("Batch"), docA.getStringList("output"));

    // Followed by punctuation, right boundary present
    Document docB = Document.create("doc6B");
    docB.setField("input1", "Use 0731.");
    stage.processDocument(docB);
    assertEquals(List.of("Batch"), docB.getStringList("output"));
  }

  @Test
  public void testLetterOnLeftBlocks() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/letterLeftBlocks.conf");

    Document doc = Document.create("doc7");
    doc.setField("input1", "akilo"); // left neighbor before "kilo" is a letter 'a'
    stage.processDocument(doc);

    assertFalse("Should not match when a letter precedes the term", doc.has("output"));
  }

  @Test
  public void testDigitOnLeftDoesNotBlock() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/digitLeftOk.conf");

    Document doc = Document.create("doc8");
    doc.setField("input1", "5kilo"); // left neighbor before "kilo" is a digit '5'
    stage.processDocument(doc);

    assertEquals(List.of("Unit"), doc.getStringList("output"));
  }

  @Test
  public void testApostropheBoundary() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/apostropheBoundary.conf");

    Document doc = Document.create("doc9");
    doc.setField("input1", "alpha's release is tomorrow");
    stage.processDocument(doc);

    assertEquals(List.of("TAG"), doc.getStringList("output"));
  }

  @Test
  public void testHyphenBoundary() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/hyphenBoundary.conf");

    Document doc = Document.create("doc10");
    doc.setField("input1", "lorem-ipsum text");
    stage.processDocument(doc);

    assertEquals(List.of("Hit"), doc.getStringList("output"));
  }

  @Test
  public void testSlashBoundary() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/slashBoundary.conf");

    Document doc = Document.create("doc11");
    doc.setField("input1", "alpha/beta gamma");
    stage.processDocument(doc);

    assertEquals(List.of("Hit"), doc.getStringList("output"));
  }

  @Test
  public void testUnderscoreBoundary() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/underscoreBoundary.conf");

    Document doc = Document.create("doc12");
    doc.setField("input1", "one_two three");
    stage.processDocument(doc);

    assertEquals(List.of("Hit"), doc.getStringList("output"));
  }

  @Test
  public void testPunctPeriodCommaBoundary() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/punctPeriodComma.conf");

    Document docA = Document.create("doc13A");
    docA.setField("input1", "city,town");
    stage.processDocument(docA);
    assertEquals(List.of("HitCity", "HitTown"), docA.getStringList("output"));

    Document docB = Document.create("doc13B");
    docB.setField("input1", "Hello. town!");
    stage.processDocument(docB);
    assertEquals(List.of("HitTown"), docB.getStringList("output"));
  }

  @Test
  public void testEmojiBoundary() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/emojiBoundary.conf");

    Document doc = Document.create("doc14");
    doc.setField("input1", "world🙂");
    stage.processDocument(doc);

    assertEquals(List.of("HIT"), doc.getStringList("output"));
  }

  @Test
  public void testCjkBoundary() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/cjkBoundary.conf");

    Document doc = Document.create("doc15");
    doc.setField("input1", "東京。"); // right neighbor is punct
    stage.processDocument(doc);

    assertEquals(List.of("Hit"), doc.getStringList("output"));
  }

  @Test
  public void testCaseSensitiveDefault() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/caseSensitiveDefault.conf");

    Document doc = Document.create("doc16");
    doc.setField("input1", "blue sky over town"); // lowercased text
    stage.processDocument(doc);

    assertFalse(doc.has("output"));
  }

  @Test
  public void testIgnoreCaseTrue() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/ignoreCaseTrue.conf");

    Document doc = Document.create("doc17");
    doc.setField("input1", "blue sky over town"); // lowercased text
    stage.processDocument(doc);

    assertEquals(List.of("Hit"), doc.getStringList("output"));
  }

  @Test
  public void testNoCrossPunctuationForMultiword() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/noCrossPunct.conf");

    Document doc = Document.create("doc18");
    doc.setField("input1", "new-town"); // hyphen breaks the exact substring "new town"
    stage.processDocument(doc);
    assertFalse(doc.has("output"));

    Document doc2 = Document.create("doc18b");
    doc2.setField("input1", "new, town"); // comma + space breaks the exact substring "new town"
    stage.processDocument(doc2);
    assertFalse(doc2.has("output"));
  }

  @Test
  public void testExactSpacesMultiword() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/exactSpaces.conf");

    Document doc = Document.create("doc19");
    doc.setField("input1", "we moved to new town recently");
    stage.processDocument(doc);

    assertEquals(List.of("Hit"), doc.getStringList("output"));
  }

  @Test
  public void testOverlapsEmitAll() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/overlapsEmitAll.conf");

    Document doc = Document.create("doc20");
    doc.setField("input1", "new town center");
    stage.processDocument(doc);

    // Matches at same start "new", "new town"
    assertEquals(List.of("T1", "T2"), doc.getStringList("output"));
  }

  @Test
  public void testOverlapsEmitLongestOnly() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/overlapsLongest.conf");

    Document doc = Document.create("doc21");
    doc.setField("input1", "new town center");
    stage.processDocument(doc);

    // Only the longest match that starts at the same position should be emitted
    assertEquals(List.of("T2"), doc.getStringList("output"));
  }

  @Test
  public void testStopOnHit() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/stopOnHit.conf");

    Document doc = Document.create("doc22");
    doc.setField("input1", "alpha beta gamma");
    stage.processDocument(doc);

    // Only the first match should appear
    assertEquals(List.of("Alpha"), doc.getStringList("output"));
  }

  @Test
  public void testUsePayloadsWithEntityField() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/usePayloadsWithEntity.conf");

    Document doc = Document.create("doc23");
    doc.setField("input1", "delta");
    stage.processDocument(doc);

    // entity_field gets the matched key
    assertEquals(List.of("DeltaPayload"), doc.getStringList("output"));
    assertEquals(List.of("delta"), doc.getStringList("entity"));
  }

  @Test
  public void testDontUsePayloads() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/dontUsePayloads.conf");

    Document doc = Document.create("doc24");
    doc.setField("input1", "alpha beta");
    stage.processDocument(doc);

    // Keys should be written, ignoring payloads in the dict
    assertEquals(List.of("alpha", "beta"), doc.getStringList("output"));
  }

  //
  //
  // DIFFERENCE
  //
  //
  @Ignore("Current difference")
  @Test
  public void testEmptyPayloadDefaultsToTerm() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/emptyPayloadDefaults.conf");

    Document doc = Document.create("doc25");
    doc.setField("input1", "omega");
    stage.processDocument(doc);

    // Payload column is empty, stage should use the term itself
    assertEquals(List.of("omega"), doc.getStringList("output"));
  }

  @Test
  public void testManySourcesOneDestOrder() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/manySourcesOneDest.conf");

    Document doc = Document.create("doc26");
    doc.setField("s1", "alpha");
    doc.setField("s2", "beta");
    doc.setField("s3", "gamma");
    stage.processDocument(doc);

    assertEquals(List.of("A", "B", "C"), doc.getStringList("out"));
  }

  @Test
  public void testOneToOneSourceDest() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/oneToOne.conf");

    Document doc = Document.create("doc27");
    doc.setField("s1", "alpha");
    doc.setField("s2", "beta");
    stage.processDocument(doc);

    assertEquals(List.of("A"), doc.getStringList("o1"));
    assertEquals(List.of("B"), doc.getStringList("o2"));
  }

  @Test
  public void testTrimAsciiSpacesInDict() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/trimSpaces.conf");

    Document doc = Document.create("doc28");
    doc.setField("input1", "apple pie");
    stage.processDocument(doc);

    assertEquals(List.of("Fruit"), doc.getStringList("output"));
  }

  @Test
  public void testDuplicateKeysFirstWins() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/dupKeysFirstWins.conf");

    Document doc = Document.create("doc29");
    doc.setField("input1", "alpha");
    stage.processDocument(doc);

    // The first occurrence of "alpha" should win
    assertEquals(List.of("One"), doc.getStringList("output"));
  }

  @Test
  public void testMalformedEntryIgnored() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/malformedIgnored.conf");

    // This should NOT match because the dict row contains � and is skipped
    Document docA = Document.create("doc30A");
    docA.setField("input1", "alpha");
    stage.processDocument(docA);
    assertFalse(docA.has("output"));

    // A valid row still works
    Document docB = Document.create("doc30B");
    docB.setField("input1", "beta");
    stage.processDocument(docB);
    assertEquals(List.of("Two"), docB.getStringList("output"));
  }

  @Test
  public void testUtf8NonAsciiTerm() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/utf8NonAscii.conf");

    Document doc = Document.create("doc31");
    doc.setField("input1", "We visited München last year.");
    stage.processDocument(doc);

    assertEquals(List.of("City"), doc.getStringList("output"));
  }

  @Test
  public void testMultiDictFirstWins() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/multiDictFirstWins.conf");

    Document doc = Document.create("doc32");
    doc.setField("input1", "alpha");
    stage.processDocument(doc);

    // Should take payload from the first dictionary listed in config
    assertEquals(List.of("One"), doc.getStringList("output"));
  }

  @Test
  public void testPunctuationInKey() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/punctInKey.conf");

    Document doc = Document.create("doc33");
    doc.setField("input1", "we use C++ daily");
    stage.processDocument(doc);

    assertEquals(List.of("Hit"), doc.getStringList("output"));
  }

  @Test
  public void testApostropheInKey() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/apostropheInKey.conf");

    Document doc = Document.create("doc34");
    doc.setField("input1", "meet O'Malley today");
    stage.processDocument(doc);

    assertEquals(List.of("Hit"), doc.getStringList("output"));
  }

  @Test
  public void testSubstringInsideWordNoMatch() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/substringNoMatch.conf");

    Document doc = Document.create("doc35");
    doc.setField("input1", "human anatomy");
    stage.processDocument(doc);

    // "man" appears inside "human"
    assertFalse(doc.has("output"));
  }

  @Test
  public void testOffByOneNearMiss() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/offByOne.conf");

    Document doc = Document.create("doc36");
    doc.setField("input1", "United State.");
    stage.processDocument(doc);

    assertFalse(doc.has("output"));
  }

  //
  //
  // DIFFERENCE IN ORDER
  //
  //
  @Test
  public void testLadderPrefixesEmitAll() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/ladderEmitAll.conf");

    Document doc = Document.create("doc37");
    doc.setField("input1", "a a a!");
    stage.processDocument(doc);

    assertEquals(List.of("X1","X1","X2","X1","X2","X3"), doc.getStringList("output"));
  }

  @Test
  public void testLadderPrefixesLongestOnly() throws Exception {
    Stage stage = factory.get("ExtractEntitiesTest/ladderLongest.conf");

    Document doc = Document.create("doc38");
    doc.setField("input1", "a a a!");
    stage.processDocument(doc);

    assertEquals(List.of("X3"), doc.getStringList("output"));
  }
}