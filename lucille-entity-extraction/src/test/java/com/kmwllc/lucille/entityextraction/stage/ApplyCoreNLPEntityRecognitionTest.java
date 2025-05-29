package com.kmwllc.lucille.entityextraction.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.stage.ApplyCoreNLPEntityRecognition;
import com.kmwllc.lucille.stage.StageFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class ApplyCoreNLPEntityRecognitionTest {

  private final StageFactory factory = StageFactory.of(ApplyCoreNLPEntityRecognition.class);

  @Test
  public void extractionTest() throws StageException {
    Stage stage = factory.get("ApplyCoreNLPEntityRecognitionTest/default.conf");

    Document doc = Document.create("doc1");
    doc.setField("text", "Jim Cramer spoke at a leadership conference hosted by the NYSE in Downtown Manhattan yesterday.");

    stage.processDocument(doc);

    assertEquals(List.of("Jim Cramer"), doc.getStringList("PERSON"));
    assertEquals(List.of("NYSE"), doc.getStringList("ORGANIZATION"));
    assertEquals(List.of("Downtown Manhattan"), doc.getStringList("LOCATION"));
    assertEquals(List.of("yesterday"), doc.getStringList("DATE"));
  }

  @Test
  public void extractionTestWithNameTypes() throws StageException {
    Stage stage = factory.get("ApplyCoreNLPEntityRecognitionTest/personOrganizationLocationOnly.conf");

    Document doc = Document.create("doc1");
    doc.setField("text", "Jim Cramer spoke at a leadership conference hosted by the NYSE in Downtown Manhattan yesterday.");

    stage.processDocument(doc);

    assertEquals(List.of("Jim Cramer"), doc.getStringList("PERSON"));
    assertEquals(List.of("NYSE"), doc.getStringList("ORGANIZATION"));
    assertEquals(List.of("Downtown Manhattan"), doc.getStringList("LOCATION"));
    assertFalse(doc.has("DATE"));
  }

  @Test
  public void fineGrained() throws StageException {
    Stage stage = factory.get("ApplyCoreNLPEntityRecognitionTest/fineGrained.conf");

    Document doc = Document.create("doc1");
    doc.setField("text", "Jim Cramer and David Faber spoke at a leadership conference hosted by the NYSE in Downtown Manhattan yesterday. We all know that New York is the heart of the U.S. economic engine. Be sure to tweet me @JimCramer.");

    stage.processDocument(doc);

    assertEquals(List.of("Jim Cramer", "David Faber"), doc.getStringList("PERSON"));
    assertEquals(List.of("NYSE"), doc.getStringList("ORGANIZATION"));
    assertEquals(List.of("Downtown"), doc.getStringList("LOCATION"));
    assertEquals(List.of("yesterday"), doc.getStringList("DATE"));

    // Specific to fine grained.
    assertEquals(List.of("Manhattan"), doc.getStringList("CITY"));
    assertEquals(List.of("New York"), doc.getStringList("STATE_OR_PROVINCE"));
    assertEquals(List.of("U.S."), doc.getStringList("COUNTRY"));
    assertEquals(List.of("@JimCramer"), doc.getStringList("HANDLE"));
  }

  @Test
  public void longerExtractionTest() throws StageException {
    Config config = ConfigFactory.parseMap(Map.of("textField", "content"));
    Stage stage = factory.get(config);

    Document doc = Document.create("doc1");
    doc.setField("content", """
    Jim Cramer spoke at a leadership conference hosted by the NYSE in Downtown Manhattan yesterday. On Squawk on the Street later that
    Tuesday, he came out as bearish against Apple stock, suggesting they could be negatively affected by the China tariffs. "Do not be
    mistaken here, Carl", he said to his co-host. "What we have here is effectively a total embargo on Chinese trade - and no matter 
    what you think about Tim Cook as an executive, Apple can not work around that". 
    
    David Faber then pivoted to focus on how companies like Temu and Shein might be affected by the end of the de minimis exception.
    Cramer referenced a joke he saw on X (formerly known as Twitter) the other day, suggesting a "Temu warehouse entirely burned down",
    causing an estimated "$550 of losses" in total. Noting Cramer's excessive energy, Carl asked which Celsius Energy Drink Jim had
    this morning, to which he emphatically replied "Carl, I heavily reject your question, because it assumes I only had ONE!"
    
    At one point, the island of Fiji came up, causing Jim Cramer to call his co-host "Jeff Probst" (as he had repeatedly been doing
    the past few days), sarcastically proclaiming, "got nothing for ya'".
    """);

    stage.processDocument(doc);

    List<String> people = doc.getStringList("PERSON");
    List<String> organizations = doc.getStringList("ORGANIZATION");
    List<String> locations = doc.getStringList("LOCATION");

    assertTrue(people.contains("Jim Cramer"));
    assertTrue(people.contains("Tim Cook"));
    assertTrue(people.contains("David Faber"));

    assertTrue(organizations.contains("Apple"));
    assertTrue(organizations.contains("NYSE"));
    assertTrue(organizations.contains("Twitter"));

    assertTrue(locations.contains("China"));
    assertTrue(locations.contains("Downtown Manhattan"));
    assertTrue(locations.contains("Fiji"));
  }

  @Test
  public void testBadConfig() {
    assertThrows(StageException.class, () -> factory.get("ApplyCoreNLPEntityRecognitionTest/emptyNameTypes.conf"));
  }
}
