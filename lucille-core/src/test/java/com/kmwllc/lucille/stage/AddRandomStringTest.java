package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.nio.file.Files;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;

public class AddRandomStringTest {

  private final StageFactory factory = StageFactory.of(AddRandomString.class);

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

    int docVal1 = Integer.valueOf(doc1.getString("data"));
    int docVal2 = Integer.valueOf(doc2.getString("data"));
    int docVal3 = Integer.valueOf(doc3.getString("data"));
    assertTrue(0 <= docVal1 && docVal1 < 20);
    assertTrue(0 <= docVal2 && docVal2 < 20);
    assertTrue(0 <= docVal3 && docVal3 < 20);
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
  public void testDefaultNumOfTerms() throws Exception {
    Stage stage = factory.get("AddRandomStringTest/noMinOrMaxNum.conf");

    // defaults to min/max of 1
    Document doc = Document.create("doc");

    stage.processDocument(doc);

    assertTrue(doc.has("data"));
    int data = doc.getInt("data");
    // lower is inclusive, upper is exclusive
    assertTrue(data >= 0 && data < 100);
  }

  // When range size is equal to the length of the file data. Want to make sure we see all of the file data.
  @Test
  public void testFileDataEqualRangeSize() throws Exception {
    Stage stage = factory.get("AddRandomStringTest/equalFileDataRangeSize.conf");

    Document doc = Document.create("doc");
    stage.processDocument(doc);

    // has 300 terms. there's a 0.00000000000072745% chance this test will fail due to natural variability.
    List<String> randomStrings = doc.getStringList("data");

    Set<String> encounteredFoods = new HashSet<>(randomStrings);

    assertEquals("Not all foods were in the random String list. There is a 0.00000000000072% chance of this occurring naturally.", 20, encounteredFoods.size());
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

    assertThrows(StageException.class,
        () -> factory.get("AddRandomStringTest/largeRangeForFileData.conf"));
  }
}