package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import java.util.Arrays;
import org.junit.Test;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;

public class ComputeFieldSizeTest {

  private final StageFactory factory = StageFactory.of(ComputeFieldSize.class);

  @Test
  public void testNotAllRequiredFields() throws StageException {
    assertThrows(StageException.class, () -> {
      factory.get("ComputeFieldSizeTest/no-source.conf");
    });
    assertThrows(StageException.class, () -> {
      factory.get("ComputeFieldSizeTest/no-output.conf");
    });
  }

  @Test
  public void testSourceFieldDoesNotExist() throws StageException {
    Stage stage = factory.get("ComputeFieldSizeTest/basic.conf");
    Document doc1 = Document.create("doc1");

    assertThrows(StageException.class, () -> {
      stage.processDocument(doc1);
    });
  }

  @Test
  public void testSourceNotByteArray() throws StageException {
    Stage stage = factory.get("ComputeFieldSizeTest/basic.conf");
    Document doc1 = Document.create("doc1");
    doc1.update("sourceField", UpdateMode.OVERWRITE, "string");

    assertThrows(NullPointerException.class, () -> {
      stage.processDocument(doc1);
    });
  }

  @Test
  public void testNormalComputeFields() throws StageException {
    Stage stage = factory.get("ComputeFieldSizeTest/basic.conf");
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");

    doc1.update("sourceField", UpdateMode.OVERWRITE, new byte[0]);
    doc2.update("sourceField", UpdateMode.OVERWRITE, new byte[5]);
    doc3.update("sourceField", UpdateMode.OVERWRITE, new byte[10]);

    for (Document doc : Arrays.asList(doc1, doc2, doc3)) {
      stage.processDocument(doc);
    }

    assertEquals(0, doc1.getInt("outputField").intValue());
    assertEquals(5, doc2.getInt("outputField").intValue());
    assertEquals(10, doc3.getInt("outputField").intValue());
  }

  @Test
  public void testOverwriteField() throws StageException {
    Stage stage = factory.get("ComputeFieldSizeTest/basic.conf");
    Document doc1 = Document.create("doc1");

    doc1.update("sourceField", UpdateMode.OVERWRITE, new byte[10]);
    doc1.update("outputField", UpdateMode.OVERWRITE, "default");

    stage.processDocument(doc1);

    assertEquals(10, doc1.getInt("outputField").intValue());
  }
}