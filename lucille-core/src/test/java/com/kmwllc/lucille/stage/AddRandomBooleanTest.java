package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import java.util.Arrays;
import org.junit.Test;

public class AddRandomBooleanTest {

  private final StageFactory factory = StageFactory.of(AddRandomBoolean.class);

  /**
   * Validate that an Exception is thrown if percent_true is less than 0.
   */
  @Test( expected = StageException.class )
  public void testInvalidTooLow() throws StageException {
    Stage stage = factory.get("AddRandomBooleanTest/toolow.conf");

    Document doc = Document.create("doc");
    stage.processDocument(doc);
  }

  /**
   * Validate that an Exception is thrown if percent_true is greater than 100.
   */
  @Test( expected = StageException.class )
  public void testInvalidTooHigh() throws StageException {
    Stage stage = factory.get("AddRandomBooleanTest/toohigh.conf");

    Document doc = Document.create("doc");
    stage.processDocument(doc);
  }

  /**
   * Validate that a field is added with a boolean when a valid config is used.
   */
  @Test
  public void testValid() throws StageException {
    Stage stage = factory.get("AddRandomBooleanTest/valid.conf");

    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");

    for (Document doc : Arrays.asList(doc1, doc2, doc3)) {
      stage.processDocument(doc);
    }

    assertTrue(doc1.has("bool"));
    assertTrue(doc2.has("bool"));
    assertTrue(doc3.has("bool"));
  }
}
