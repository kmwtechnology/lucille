package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import java.nio.file.Paths;
import java.util.Iterator;
import org.junit.Test;

public class ApplyFileHandlersTest {

  private final StageFactory factory = StageFactory.of(ApplyFileHandlers.class);

  @Test
  public void testApplyFileHandlersCSV() throws StageException {
    Stage stage = factory.get("ApplyFileHandlersTest/allTypes.conf");

    Document csvDoc = Document.create("csv_doc");
    csvDoc.setField("source", Paths.get("src/test/resources/ApplyFileHandlersTest/test.csv").toString());

    Iterator<Document> docs = stage.processDocument(csvDoc);

    int docsCount = 0;

    while (docs.hasNext()) {
      Document d = docs.next();
      docsCount++;

      assertEquals("test.csv", d.getString("filename"));
    }

    assertEquals(4, docsCount);
  }

  @Test
  public void testApplyFileHandlersJSON() throws StageException {
    Stage stage = factory.get("ApplyFileHandlersTest/allTypes.conf");

    Document csvDoc = Document.create("json_doc");
    csvDoc.setField("source", Paths.get("src/test/resources/ApplyFileHandlersTest/test.jsonl").toString());

    Iterator<Document> docs = stage.processDocument(csvDoc);

    int docsCount = 0;

    while (docs.hasNext()) {
      Document d = docs.next();
      docsCount++;
      assertEquals(docsCount + "", d.getId());
    }

    assertEquals(4, docsCount);
  }

  @Test
  public void testNoHandlers() throws StageException {

  }

  @Test
  public void testInvalidConfs() throws StageException {
    assertThrows(StageException.class, () -> factory.get("ApplyFileHandlersTest/empty.conf"));
    assertThrows(StageException.class, () -> factory.get("ApplyFileHandlersTest/noHandlers.conf"));
  }
}
