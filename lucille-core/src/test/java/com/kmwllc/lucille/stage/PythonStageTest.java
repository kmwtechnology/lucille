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
        return; // Port is free
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
      waitForPortToBeFree(25333, 5000); // Wait up to 5 seconds for the default port to be free
    }
  }

  @Test
  public void testBasicPythonPrint() throws StageException {
    stage = factory.get("PythonStageTest/print_time.conf");
    Document doc = Document.create("doc1");
    stage.processDocument(doc);
    assertTrue(doc.has("python_stage_executed"));
  }

  @Test
  public void testPythonReturnsTime() throws StageException {
    stage = factory.get("PythonStageTest/return_time.conf");
    Document doc = Document.create("doc2");
    stage.processDocument(doc);
    assertTrue(doc.has("current_time"));
    assertNotNull(doc.getString("current_time"));
  }

  // Additional tests for error handling, port config, etc. can be added here
}
