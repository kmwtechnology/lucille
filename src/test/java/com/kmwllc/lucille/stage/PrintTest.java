package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;
import static org.junit.Assert.*;

public class PrintTest {

  private static final StageFactory factory = StageFactory.of(Print.class);

  @Test
  public void testNoFieldsCSV() throws StageException {
    Stage stage = factory.get("PrintTest/noFieldsCSV.conf");

    Document doc1 = new Document("doc1");
    doc1.setField("test", "this is a test");
    stage.processDocument(doc1);
  }
}
