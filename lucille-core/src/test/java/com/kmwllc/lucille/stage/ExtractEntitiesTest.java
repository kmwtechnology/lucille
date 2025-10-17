package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import java.util.ArrayList;
import java.util.Collections;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public abstract class ExtractEntitiesTest {

  protected abstract Stage newStage(String raw);

  // United States,North America
  @Test
  public void testStartOfStringBoundary() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/startOfString.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc1");
    doc.setField("input1", "United States is large");
    stage.processDocument(doc);

    assertEquals(List.of("North America"), doc.getStringList("output"));
  }

  // Canada,North America
  @Test
  public void testEndOfStringBoundary() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/endOfString.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc2");
    doc.setField("input1", "I live in Canada");
    stage.processDocument(doc);

    assertEquals(List.of("North America"), doc.getStringList("output"));
  }

  // United States,North America
  @Test
  public void testPunctuationBoundaries() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/punctBoundaries.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc3");
    doc.setField("input1", "I live in the (United States).");
    stage.processDocument(doc);

    assertEquals(List.of("North America"), doc.getStringList("output"));
  }

  // Vexa,Brand
  @Test
  public void testLetterOnRightBlocks() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/letterRightBlocks.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc4");
    doc.setField("input1", "Vexatronics");
    stage.processDocument(doc);

    assertFalse(doc.has("output"));
  }

  // Z5,Code
  @Test
  public void testDigitOnRightDoesNotBlock() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/digitRightOk.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc5");
    doc.setField("input1", "Model Z57 is discontinued"); // right neighbor of "Z5" is a digit
    stage.processDocument(doc);

    assertEquals(List.of("Code"), doc.getStringList("output"));
  }

  // 0731,Batch
  @Test
  public void testDigitOnlyInsideNumber() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/digitInside.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document docA = Document.create("doc6A");
    docA.setField("input1", "The code is 90731 today.");
    stage.processDocument(docA);
    assertEquals(List.of("Batch"), docA.getStringList("output"));

    Document docB = Document.create("doc6B");
    docB.setField("input1", "Use 0731.");
    stage.processDocument(docB);
    assertEquals(List.of("Batch"), docB.getStringList("output"));
  }

  // kilo,Unit
  @Test
  public void testLetterOnLeftBlocks() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/letterLeftBlocks.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc7");
    doc.setField("input1", "akilo");
    stage.processDocument(doc);

    assertFalse(doc.has("output"));
  }

  // kilo,Unit
  @Test
  public void testDigitOnLeftDoesNotBlock() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/digitLeftOk.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc8");
    doc.setField("input1", "5kilo");
    stage.processDocument(doc);

    assertEquals(List.of("Unit"), doc.getStringList("output"));
  }

  // alpha,TAG
  @Test
  public void testApostropheBoundary() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/apostropheBoundary.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc9");
    doc.setField("input1", "alpha's release is tomorrow");
    stage.processDocument(doc);

    assertEquals(List.of("TAG"), doc.getStringList("output"));
  }

  // ipsum,Hit
  @Test
  public void testHyphenBoundary() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/hyphenBoundary.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc10");
    doc.setField("input1", "lorem-ipsum text");
    stage.processDocument(doc);

    assertEquals(List.of("Hit"), doc.getStringList("output"));
  }

  // alpha,Hit
  @Test
  public void testSlashBoundary() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/slashBoundary.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc11");
    doc.setField("input1", "alpha/beta gamma");
    stage.processDocument(doc);

    assertEquals(List.of("Hit"), doc.getStringList("output"));
  }

  // one,Hit
  @Test
  public void testUnderscoreBoundary() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/underscoreBoundary.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc12");
    doc.setField("input1", "one_two three");
    stage.processDocument(doc);

    assertEquals(List.of("Hit"), doc.getStringList("output"));
  }

  // city,HitCity
  // town,HitTown
  @Test
  public void testPunctPeriodCommaBoundary() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/punctPeriodComma.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document docA = Document.create("doc13A");
    docA.setField("input1", "city,town");
    stage.processDocument(docA);
    assertEquals(List.of("HitCity", "HitTown"), docA.getStringList("output"));

    Document docB = Document.create("doc13B");
    docB.setField("input1", "Hello. town!");
    stage.processDocument(docB);
    assertEquals(List.of("HitTown"), docB.getStringList("output"));
  }

  // world,HIT
  @Test
  public void testEmojiBoundary() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/emojiBoundary.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc14");
    doc.setField("input1", "world🙂");
    stage.processDocument(doc);

    assertEquals(List.of("HIT"), doc.getStringList("output"));
  }

  // 東京,Hit
  @Test
  public void testCjkBoundary() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/cjkBoundary.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc15");
    doc.setField("input1", "東京。");
    stage.processDocument(doc);

    assertEquals(List.of("Hit"), doc.getStringList("output"));
  }

  // Blue Sky,Hit
  @Test
  public void testCaseSensitiveDefault() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/caseSensitiveDefault.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc16");
    doc.setField("input1", "blue sky over town");
    stage.processDocument(doc);

    assertFalse(doc.has("output"));
  }

  // Blue Sky,Hit
  @Test
  public void testIgnoreCaseTrue() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/ignoreCaseTrue.dict"]
          ignore_case = true
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc17");
    doc.setField("input1", "blue sky over town");
    stage.processDocument(doc);

    assertEquals(List.of("Hit"), doc.getStringList("output"));
  }

  // new town,Hit
  @Test
  public void testNoCrossPunctuationForMultiword() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/noCrossPunct.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc18");
    doc.setField("input1", "new-town");
    stage.processDocument(doc);
    assertFalse(doc.has("output"));

    Document doc2 = Document.create("doc18b");
    doc2.setField("input1", "new, town");
    stage.processDocument(doc2);
    assertFalse(doc2.has("output"));
  }

  // new town,Hit
  @Test
  public void testExactSpacesMultiword() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/exactSpaces.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc19");
    doc.setField("input1", "we moved to new town recently");
    stage.processDocument(doc);

    assertEquals(List.of("Hit"), doc.getStringList("output"));
  }

  // new,T1
  // new town,T2
  @Test
  public void testOverlapsEmitAll() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/overlapsEmitAll.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc20");
    doc.setField("input1", "new town center");
    stage.processDocument(doc);

    assertEquals(List.of("T1", "T2"), doc.getStringList("output"));
  }

  // new,T1
  // new town,T2
  @Test
  public void testOverlapsEmitLongestOnly() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/overlapsLongest.dict"]
          ignore_overlaps = true
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc21");
    doc.setField("input1", "new town center");
    stage.processDocument(doc);

    assertEquals(List.of("T2"), doc.getStringList("output"));
  }

  // alpha,Alpha
  // beta,Beta
  // gamma,Gamma
  @Test
  public void testStopOnHit() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/stopOnHit.dict"]
          stop_on_hit = true
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc22");
    doc.setField("input1", "alpha beta gamma");
    stage.processDocument(doc);

    assertEquals(List.of("Alpha"), doc.getStringList("output"));
  }

  // delta,DeltaPayload
  @Test
  public void testUsePayloadsWithEntityField() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/usePayloadsWithEntity.dict"]
          entity_field = "entity"
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc23");
    doc.setField("input1", "delta");
    stage.processDocument(doc);

    assertEquals(List.of("DeltaPayload"), doc.getStringList("output"));
    assertEquals(List.of("delta"), doc.getStringList("entity"));
  }

  // alpha,PA
  // beta,PB
  @Test
  public void testDontUsePayloads() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/dontUsePayloads.dict"]
          use_payloads = false
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc24");
    doc.setField("input1", "alpha beta");
    stage.processDocument(doc);

    assertEquals(List.of("alpha", "beta"), doc.getStringList("output"));
  }

  // alpha,A
  // beta,B
  // gamma,C
  @Test
  public void testManySourcesOneDestOrder() throws Exception {
    String config = """
        {
          source = ["s1", "s2", "s3"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/manySourcesOneDest.dict"]
          update_mode = "append"
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc25");
    doc.setField("s1", "alpha");
    doc.setField("s2", "beta");
    doc.setField("s3", "gamma");
    stage.processDocument(doc);

    assertEquals(List.of("A", "B", "C"), doc.getStringList("output"));
  }

  // alpha,A
  // beta,B
  @Test
  public void testOneToOneSourceDest() throws Exception {
    String config = """
        {
          source = ["s1", "s2"]
          dest = ["o1", "o2"]
          dictionaries = ["classpath:ExtractEntitiesTest/oneToOne.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc26");
    doc.setField("s1", "alpha");
    doc.setField("s2", "beta");
    stage.processDocument(doc);

    assertEquals(List.of("A"), doc.getStringList("o1"));
    assertEquals(List.of("B"), doc.getStringList("o2"));
  }

  //   apple  ,Fruit
  @Test
  public void testTrimAsciiSpacesInDict() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/trimSpaces.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc27");
    doc.setField("input1", "apple pie");
    stage.processDocument(doc);

    assertEquals(List.of("Fruit"), doc.getStringList("output"));
  }

  // alpha,One
  // alpha,Two
  @Test
  public void testDuplicateKeysFirstWins() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/dupKeysFirstWins.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc28");
    doc.setField("input1", "alpha");
    stage.processDocument(doc);

    assertEquals(List.of("One"), doc.getStringList("output"));
  }

  // alp�ha,One
  // beta,Two
  @Test
  public void testMalformedEntryIgnored() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/malformedIgnored.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document docA = Document.create("doc29A");
    docA.setField("input1", "alpha");
    stage.processDocument(docA);
    assertFalse(docA.has("output"));

    Document docB = Document.create("doc29B");
    docB.setField("input1", "beta");
    stage.processDocument(docB);
    assertEquals(List.of("Two"), docB.getStringList("output"));
  }

  // München,City
  @Test
  public void testUtf8NonAsciiTerm() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/utf8NonAscii.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc30");
    doc.setField("input1", "We visited München last year.");
    stage.processDocument(doc);

    assertEquals(List.of("City"), doc.getStringList("output"));
  }

  // alpha,One

  // alpha,Two
  @Test
  public void testMultiDictFirstWins() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/multiA.dict", "classpath:ExtractEntitiesTest/multiB.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc31");
    doc.setField("input1", "alpha");
    stage.processDocument(doc);

    assertEquals(List.of("One"), doc.getStringList("output"));
  }

  // C++,Hit
  @Test
  public void testPunctuationInKey() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/punctInKey.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc32");
    doc.setField("input1", "we use C++ daily");
    stage.processDocument(doc);

    assertEquals(List.of("Hit"), doc.getStringList("output"));
  }

  // O'Malley,Hit
  @Test
  public void testApostropheInKey() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/apostropheInKey.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc33");
    doc.setField("input1", "meet O'Malley today");
    stage.processDocument(doc);

    assertEquals(List.of("Hit"), doc.getStringList("output"));
  }

  // man,Hit
  @Test
  public void testSubstringInsideWordNoMatch() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/substringNoMatch.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc34");
    doc.setField("input1", "human anatomy");
    stage.processDocument(doc);

    assertFalse(doc.has("output"));
  }

  // United States,Hit
  @Test
  public void testOffByOneNearMiss() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/offByOne.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc35");
    doc.setField("input1", "United State.");
    stage.processDocument(doc);

    assertFalse(doc.has("output"));
  }

  // a,X1
  // a a,X2
  // a a a,X3
  @Test
  public void testLadderPrefixesEmitAll() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/ladderEmitAll.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc36");
    doc.setField("input1", "a a a!");
    stage.processDocument(doc);

    List<String> expected = List.of("X1", "X1", "X1", "X2", "X2", "X3");
    List<String> actual = new ArrayList<>(doc.getStringList("output"));
    Collections.sort(actual);

    assertEquals(expected, actual);
  }

  // a,X1
  // a a,X2
  // a a a,X3
  @Test
  public void testLadderPrefixesLongestOnly() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/ladderLongest.dict"]
          ignore_overlaps = true
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc37");
    doc.setField("input1", "a a a!");
    stage.processDocument(doc);

    assertEquals(List.of("X3"), doc.getStringList("output"));
  }

  // alpha & bet,ALPHA&BET
  // & beta,&BETA
  @Test
  public void testLaterStartAfterDeadPrefix() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/laterStartAfterDeadPrefix.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc38");
    doc.setField("input1", "alpha & beta");
    stage.processDocument(doc);

    assertEquals(List.of("&BETA"), doc.getStringList("output"));
  }

  // omega
  @Test
  public void testMissingPayloadDefaultsToTerm() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/missingPayloadDefaults.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc39");
    doc.setField("input1", "omega");
    stage.processDocument(doc);

    // Payload column is missing, stage should use the term itself
    assertEquals(List.of("omega"), doc.getStringList("output"));
  }

  // omega
  @Test
  public void testEmptyPayloadDefaultsToEmpty() throws Exception {
    String config = """
        {
          source = ["input1"]
          dest = ["output"]
          dictionaries = ["classpath:ExtractEntitiesTest/emptyPayloadDefaults.dict"]
        }
        """;
    Stage stage = newStage(config);

    Document doc = Document.create("doc40");
    doc.setField("input1", "omega");
    stage.processDocument(doc);

    // Payload column is blank, stage should use ""
    assertEquals(List.of(""), doc.getStringList("output"));
  }
}