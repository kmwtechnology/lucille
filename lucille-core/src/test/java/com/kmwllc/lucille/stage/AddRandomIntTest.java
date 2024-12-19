package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import java.util.Arrays;
import org.junit.Test;

public class AddRandomIntTest {

  private final StageFactory factory = StageFactory.of(AddRandomInt.class);

  /**
   * Validate that an exception is thrown if the range start is greater than the range end.
   */
  @Test( expected = StageException.class )
  public void testInvalidStartEnd() throws StageException {
    Stage stage = factory.get("AddRandomIntTest/invalid.conf");

    Document doc = Document.create("doc");
    stage.processDocument(doc);
  }

  /**
   * Validates that a valid config creates random ints in the given range.
   */
  @Test
  public void testValid() throws StageException {
    Stage stage = factory.get("AddRandomIntTest/valid.conf");

    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");

    for (Document doc : Arrays.asList(doc1, doc2, doc3)) {
      stage.processDocument(doc);
    }

    int start = 10;
    int end = 50;

    assertTrue(start <= doc1.getInt("integer") && end > doc1.getInt("integer"));
    assertTrue(start <= doc2.getInt("integer") && end > doc2.getInt("integer"));
    assertTrue(start <= doc3.getInt("integer") && end > doc3.getInt("integer"));
  }
}
