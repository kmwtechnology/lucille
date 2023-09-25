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
  public void testBasicFilePath() throws StageException {
    Stage stage = factory.get("AddRandomFieldTest/basicfilepath.conf");
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");

    for (Document doc : Arrays.asList(doc1, doc2, doc3)) {
      stage.processDocument(doc);
    }

    assertEquals("Hi There!\n", doc1.getString("data"));
  }
}