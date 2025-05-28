package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
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
        // wait for FIN_WAIT_2 to be resolved
        ignored.close();
        Thread.sleep(1000);
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
      // waitForPortToBeFree(25333, 15000); // Wait up to 5 seconds for the default port to be free
    }
  }

  @Test
  public void testPythonUpdateToDoc() throws Exception {
    String confPath = "PythonStageTest/process_document_1.conf";
    stage = factory.get(confPath);
    Document doc = Document.create("doc2");
    stage.processDocument(doc);
    stage.processDocument(doc);
    stage.processDocument(doc);
    assertEquals("Hello from Python!", doc.getString("field_added_by_python"));
    Thread.sleep(1000);
  }

  @Test
  public void testBasicPythonPrint() throws Exception {
    String confPath = "PythonStageTest/print_time.conf";
    stage = factory.get(confPath);
    Document doc = Document.create("doc1");
    stage.processDocument(doc);
    Thread.sleep(1000);
  }

  @Test
  public void testPythonUpdateToDocMultiThreaded() throws Exception {
    String confPath = "PythonStageTest/process_document_1.conf";
    stage = factory.get(confPath);
    int numThreads = 5;
    int numDocsPerThread = 3;
    Thread[] threads = new Thread[numThreads];
    Document[] docs = new Document[numThreads];
    for (int i = 0; i < numThreads; i++) {
      final int idx = i;
      docs[i] = Document.create("doc_mt_" + idx);
      threads[i] = new Thread(() -> {
        try {
          for (int j = 0; j < numDocsPerThread; j++) {
            stage.processDocument(docs[idx]);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }
    for (Thread t : threads) t.start();
    for (Thread t : threads) t.join();
    for (int i = 0; i < numThreads; i++) {
      assertEquals("Hello from Python!", docs[i].getString("field_added_by_python"));
    }
    Thread.sleep(1000);
  }

}
