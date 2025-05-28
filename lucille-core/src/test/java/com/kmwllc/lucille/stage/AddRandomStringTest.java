package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import java.io.IOException;
import java.nio.file.Files;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.junit.Test;

public class AddRandomStringTest {

  private final StageFactory factory = StageFactory.of(AddRandomString.class);
  private static final Set<String> acceptableStrings = Set.of(
      "Artichoke",
      "Arugula",
      "Asparagus",
      "Avocado",
      "BambooShoots",
      "BeanSprouts",
      "Beans",
      "Beet",
      "BelgianEndive",
      "BellPepper",
      "BitterMelon",
      "BokChoy",
      "Broccoli",
      "Brussels Sprouts",
      "Burdock Root/Gobo",
      "Cabbage",
      "Calabash",
      "Capers",
      "Carrot",
      "Cassava/Yuca"
  );

  /**
   * Tests stage without a file path
   *
   * @throws StageException
   */
  @Test
  public void testNumeric() throws StageException {
    Stage stage = factory.get("AddRandomStringTest/nofilepath.conf");
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");

    for (Document doc : Arrays.asList(doc1, doc2, doc3)) {
      stage.processDocument(doc);
    }

    for (List<String> docVal : List.of(doc1.getStringList("data"), doc2.getStringList("data"), doc3.getStringList("data"))) {
      for (String docValStr : docVal) {
        int docValInt = Integer.valueOf(docValStr);
        assertTrue(0 <= docValInt && docValInt < 20);
      }
    }
  }

  @Test
  public void testConcatenate() throws StageException {
    Stage stage = factory.get("AddRandomStringTest/concatenate.conf");
    Document doc = Document.create("doc1");
    stage.processDocument(doc);
    assertFalse(doc.isMultiValued("data"));
    assertEquals(10, doc.getString("data").split(" ").length);
  }

  /**
   * Tests stage with a basic file path
   *
   * @throws StageException
   */
  @Test
  public void testBasicFilePath() throws StageException, IOException {
    Stage stage = factory.get("AddRandomStringTest/basicfilepath.conf");
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");

    for (Document doc : Arrays.asList(doc1, doc2, doc3)) {
      stage.processDocument(doc);
    }

    List<String> data = Files.readAllLines(Path.of("src/test/resources/AddRandomStringTest/foods.txt"));

    String docVal1 = doc1.getString("data");
    String docVal2 = doc2.getString("data");
    String docVal3 = doc3.getString("data");
    NumberFormatException thrown = assertThrows(NumberFormatException.class, () -> Integer.valueOf(docVal1));
    assertTrue(thrown.getLocalizedMessage().startsWith("For input string"));
    assertTrue(data.contains(docVal1));
    assertTrue(data.contains(docVal2));
    assertTrue(data.contains(docVal3));
  }

  /**
   * Tests that the rangeSize option works as intended
   *
   * @throws StageException
   */
  @Test
  public void testRangeSize() throws StageException {
    Stage stage = factory.get("AddRandomStringTest/basicfilepath.conf");
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");

    for (Document doc : Arrays.asList(doc1, doc2, doc3)) {
      stage.processDocument(doc);
    }

    List<String> docVal1 = doc1.getStringList("data");
    List<String> docVal2 = doc2.getStringList("data");
    List<String> docVal3 = doc3.getStringList("data");

    for (List<String> docVal : Arrays.asList(docVal1, docVal2, docVal3)) {
      assertTrue(docVal.stream().allMatch(s -> s.equals(docVal.get(0))));
    }
  }

  /**
   * Tests that the min and max options work as intended
   *
   * @throws StageException
   */
  @Test
  public void testMinMax() throws StageException {
    Stage stage = factory.get("AddRandomStringTest/basicfilepath.conf");
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");

    for (Document doc : Arrays.asList(doc1, doc2, doc3)) {
      stage.processDocument(doc);
    }

    List<String> docVal1 = doc1.getStringList("data");
    List<String> docVal2 = doc2.getStringList("data");
    List<String> docVal3 = doc3.getStringList("data");

    for (List<String> docVal : Arrays.asList(docVal1, docVal2, docVal3)) {
      assertTrue(docVal.size() == 10);
    }
  }

  @Test
  public void testNested() throws StageException {
    Stage stage = factory.get("AddRandomStringTest/nestedNoFile.conf");
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");

    for (Document doc : Arrays.asList(doc1, doc2, doc3)) {
      stage.processDocument(doc);
    }

    for (JsonNode node : List.of(doc1.getJson("data"), doc2.getJson("data"), doc3.getJson("data"))) {
      assertTrue(node.isArray());

      Iterator<JsonNode> elements = node.elements();

      while (elements.hasNext()) {
        JsonNode currentElement = elements.next();
        String currentElementData = currentElement.get("data").asText();

        int currentElementDataInt = Integer.valueOf(currentElementData);
        assertTrue(0 <= currentElementDataInt && currentElementDataInt < 20);
      }
    }
  }

  @Test
  public void testNestedWithFile() throws StageException {
    Stage stage = factory.get("AddRandomStringTest/nestedWithFile.conf");
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");

    for (Document doc : Arrays.asList(doc1, doc2, doc3)) {
      stage.processDocument(doc);
    }

    for (JsonNode node : List.of(doc1.getJson("data"), doc2.getJson("data"), doc3.getJson("data"))) {
      assertTrue(node.isArray());

      Iterator<JsonNode> elements = node.elements();

      while (elements.hasNext()) {
        JsonNode currentElement = elements.next();
        String currentElementText = currentElement.get("data").asText();
        assertTrue(acceptableStrings.contains(currentElementText));
      }
    }
  }

  @Test
  public void testInvalidConfig() {
    assertThrows(StageException.class,
        () -> factory.get("AddRandomStringTest/moreMinThanMax.conf"));

    assertThrows(StageException.class,
        () -> factory.get("AddRandomStringTest/inputAndRangeNull.conf"));

    assertThrows(StageException.class,
        () -> factory.get("AddRandomStringTest/concatAndNested.conf"));

    assertThrows(StageException.class,
        () -> factory.get("AddRandomStringTest/minIsNullMaxIsNot.conf"));

    assertThrows(StageException.class,
        () -> factory.get("AddRandomStringTest/maxIsNullMinIsNot.conf"));
  }
}