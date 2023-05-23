package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;

public class PrintTest {

  private static final StageFactory factory = StageFactory.of(Print.class);

  @Test
  public void testBasic() throws StageException {
    Stage stage = factory.get("PrintTest/config.conf");

    Document doc1 = Document.create("doc1");
    doc1.setField("test", "this is a test");
    doc1.initializeRunId("runID1");
    stage.processDocument(doc1);
    stage.stop();
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("PrintTest/config.conf");
    assertEquals(
        Set.of(
            "overwriteFile",
            "outputFile",
            "shouldLog",
            "name",
            "excludeFields",
            "conditions",
            "class"),
        stage.getLegalProperties());
  }
}
