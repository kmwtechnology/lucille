package com.kmwllc.lucille.entityextraction.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.stage.ApplyOpenNLPNameFinders;
import com.kmwllc.lucille.stage.StageFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class ApplyOpenNLPNameFindersTest {

  private final StageFactory factory = StageFactory.of(ApplyOpenNLPNameFinders.class);

  @Test
  public void extractionTest() throws StageException {
    Config config = ConfigFactory.parseMap(Map.of("textField", "text"));
    Stage stage = factory.get(config);

    Document doc = Document.create("doc1");
    doc.setField("text", "Jim Cramer spoke at a leadership conference hosted by the NYSE in Downtown Manhattan yesterday.");

    stage.processDocument(doc);

    // Model does not pick up on "Jim Cramer" for a "person" name
    assertEquals(List.of("NYSE"), doc.getStringList("ORGANIZATION"));
    assertEquals(List.of("Manhattan"), doc.getStringList("LOCATION"));
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

    System.out.println(doc);

    assertTrue(people.contains("Jim Cramer"));
    assertTrue(people.contains("Tim Cook"));
    assertTrue(people.contains("David Faber"));

    assertTrue(organizations.contains("Apple"));
    assertTrue(organizations.contains("NYSE"));
    // Model doesn't pick up on Twitter as an organization

    // model doesn't pick up on locations "china", "downtown manhattan", or "Fiji". Only picks up on "Manhattan".
    assertTrue(locations.contains("Manhattan"));

    // Pointing out some of the model's quirks:
    // People includes Shein, Temu
    // Organizations includes "Celsius Energy Drink Jim"
  }

  // no organizations --> shouldn't be a field on the Document
  @Test
  public void testMissingTypes() throws StageException {
    Config config = ConfigFactory.parseMap(Map.of("textField", "text"));
    Stage stage = factory.get(config);

    Document doc = Document.create("doc1");
    doc.setField("text", "Sabrina Carpenter made a guest appearance in New York last week.");

    stage.processDocument(doc);

    assertFalse(doc.has("ORGANIZATION"));
  }
}
