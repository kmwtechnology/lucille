package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;
import static org.junit.Assert.*;

public class PrintTest {

  private static final StageFactory factory = StageFactory.of(Print.class);

  @Test
  public void testBasic() throws StageException {
    Stage stage = factory.get("PrintTest/config.conf");

    JsonDocument doc1 = new JsonDocument("doc1");
    doc1.setField("test", "this is a test");
    doc1.initializeRunId("runID1");
    stage.processDocument(doc1);
    stage.stop();
  }
}
