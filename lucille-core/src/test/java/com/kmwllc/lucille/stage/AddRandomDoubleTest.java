package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import java.util.Arrays;
import org.junit.Test;

public class AddRandomDoubleTest {

  private final StageFactory factory = StageFactory.of(AddRandomDouble.class);

  /**
   * Validate that an exception is thrown if the range start is greater than the range end.
   */
  @Test( expected = StageException.class )
  public void testInvalidStartEnd() throws StageException {
    Stage stage = factory.get("AddRandomDoubleTest/invalid.conf");

    Document doc = Document.create("doc");
    stage.processDocument(doc);
  }

  /**
   * Validates that a valid config creates random doubles in the given range.
   */
  @Test
  public void testValid() throws StageException {
    Stage stage = factory.get("AddRandomDoubleTest/valid.conf");

    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");

    for (Document doc : Arrays.asList(doc1, doc2, doc3)) {
      stage.processDocument(doc);
    }

    double start = 5.5;
    double end = 20.2;

    assertTrue(start <= doc1.getDouble("double") && end > doc1.getDouble("double"));
    assertTrue(start <= doc2.getDouble("double") && end > doc2.getDouble("double"));
    assertTrue(start <= doc3.getDouble("double") && end > doc3.getDouble("double"));
  }
}
