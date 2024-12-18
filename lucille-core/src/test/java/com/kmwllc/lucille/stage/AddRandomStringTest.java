package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import java.io.IOException;
import java.nio.file.Files;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
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
    Stage stage = factory.get("AddRandomFieldTest/nofilepath.conf");
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
    Stage stage = factory.get("AddRandomFieldTest/concatenate.conf");
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
    Stage stage = factory.get("AddRandomFieldTest/basicfilepath.conf");
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");

    for (Document doc : Arrays.asList(doc1, doc2, doc3)) {
      stage.processDocument(doc);
    }

    List<String> data = Files.readAllLines(Path.of("src/test/resources/AddRandomFieldTest/foods.txt"));

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
    Stage stage = factory.get("AddRandomFieldTest/basicfilepath.conf");
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
    Stage stage = factory.get("AddRandomFieldTest/basicfilepath.conf");
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
}