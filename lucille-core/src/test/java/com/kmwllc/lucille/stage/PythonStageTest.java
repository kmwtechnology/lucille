package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;

public class PythonStageTest {

  private final StageFactory factory = StageFactory.of(PythonStage.class);
  private Stage stage;

  private static void waitForPortToBeFree(int port, int timeoutMs) throws InterruptedException {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timeoutMs) {
      try (java.net.ServerSocket ignored = new java.net.ServerSocket(port)) {
        return; // port is currently free
      } catch (java.io.IOException e) {
        Thread.sleep(100);
      }
    }
    throw new RuntimeException("Port " + port + " did not become available in time");
  }

  @After
  public void tearDown() throws Exception {
    if (stage != null) {
      stage.stop();
      stage = null;
      waitForPortToBeFree(25333, 15000); // Wait up to 5 seconds for the default port to be free
    }
  }

  // @Test
  // public void testBasicPythonPrint() throws StageException {
  //   stage = factory.get("PythonStageTest/print_time.conf");
  //   Document doc = Document.create("doc1");
  //   stage.processDocument(doc);
  //   assertTrue(doc.has("python_stage_executed"));
  // }

  @Test
  public void testPythonUpdateToDoc() throws StageException {
    stage = factory.get("PythonStageTest/process_document_1.conf");
    Document doc = Document.create("doc2");
    stage.processDocument(doc);
    // The placeholder implementation sets this field, but the real implementation should set 'current_time'
    // This will fail until PythonStage is implemented to actually call Python and update the doc
    assertNotNull(doc.getString("field_added_by_python"));
    assertEquals("Hello from Python!", doc.getString("field_added_by_python"));
  }

}
