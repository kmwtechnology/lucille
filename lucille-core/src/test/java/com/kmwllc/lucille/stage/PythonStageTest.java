package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;

public class PythonStageTest {

  private final StageFactory factory = StageFactory.of(PythonStage.class);
  private Stage stage;

  @After
  public void tearDown() throws Exception {
    if (stage != null) {
      stage.stop();
      stage = null;
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
    int numThreads = 5;
    int numDocsPerThread = 3;
    Thread[] threads = new Thread[numThreads];
    Document[] docs = new Document[numThreads];
    for (int i = 0; i < numThreads; i++) {
      final int idx = i;
      docs[i] = Document.create("doc_mt_" + idx);
      threads[i] = new Thread(() -> {
        Stage myStage = null;
        try {
          myStage = factory.get(confPath);
          for (int j = 0; j < numDocsPerThread; j++) {
            myStage.processDocument(docs[idx]);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          if (myStage != null) {
            try {
              myStage.stop();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        }
      });
    }
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    for (int i = 0; i < numThreads; i++) {
      assertEquals("Hello from Python!", docs[i].getString("field_added_by_python"));
    }
    Thread.sleep(1000);
  }
}
