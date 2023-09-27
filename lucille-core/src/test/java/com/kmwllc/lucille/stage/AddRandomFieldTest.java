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

  /*
    TODO:
     - Give some thought to whether the config file path to the foods.txt needs to be referenced via classpath or via relative path
     -
   */

  private StageFactory factory = StageFactory.of(AddRandomField.class);

  /**
   * Tests
   *
   * @throws StageException
   */
  @Test
  public void testBasicFilePath() throws StageException {
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
    assertTrue(0 < docVal1 && docVal1 < 20);
    assertTrue(0 < docVal2 && docVal2 < 20);
    assertTrue(0 < docVal3 && docVal3 < 20);
  }
}