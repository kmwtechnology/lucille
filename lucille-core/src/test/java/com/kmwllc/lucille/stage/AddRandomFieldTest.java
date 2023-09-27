package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import com.kmwllc.lucille.stage.StageFactory;
import java.util.Arrays;
import org.junit.Test;

public class AddRandomFieldTest {

  private StageFactory factory = StageFactory.of(AddRandomField.class);

  /**
   * Tests
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

    int docVal1 = Integer.valueOf(doc1.getStringList("data"));
    int docVal2 = Integer.valueOf(doc2.getStringList("data"));
    int docVal3 = Integer.valueOf(doc3.getStringList("data"));
    assertTrue(0 < docVal1 && docVal1 < 20);
    assertTrue(0 < docVal2 && docVal2 < 20);
    assertTrue(0 < docVal3 && docVal3 < 20);
  }

  @Test
  public void testBasicFilePath() throws StageException {
    Stage stage = factory.get("AddRandomFieldTest/basicfilepath.conf");
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");

    for (Document doc : Arrays.asList(doc1, doc2, doc3)) {
      stage.processDocument(doc);
    }

    String docVal1 = doc1.getString("data");
    String docVal2 = doc2.getString("data");
    String docVal3 = doc3.getString("data");
    NumberFormatException thrown = assertThrows(NumberFormatException.class, () -> Integer.valueOf(docVal1));
    assertTrue(thrown.getLocalizedMessage().startsWith("For input string"));

    // TODO: load in foods.txt and check that the values exist as part of the arraylist from file
  }

  // TODO:
  // - test to validate that cardinality works
  // - test to validate that field name works
  // - test to validate that min
}