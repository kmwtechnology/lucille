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
    Document doc = Document.create("doc1");
    stage.processDocument(doc);
    stage.processDocument(doc);
    stage.processDocument(doc);
    assertEquals("Hello from Python!", doc.getString("field_added_by_python"));
  }

  @Test
  public void testPythonUpdateToDocMultiThreaded() throws Exception {
    String confPath = "PythonStageTest/copy_doc_id.conf";
    int numThreads = 5;
    int numDocsPerThread = 20;
    Thread[] threads = new Thread[numThreads];
    Document[][] docs = new Document[numThreads][numDocsPerThread];
    for (int i = 0; i < numThreads; i++) {
      for (int j = 0; j < numDocsPerThread; j++) {
        docs[i][j] = Document.create("doc_" + i + "_" + j);
      }
    }
    for (int i = 0; i < numThreads; i++) {
      final int i2 = i;
      threads[i2] = new Thread(() -> {
        Stage myStage = null;
        try {
          myStage = factory.get(confPath);
          for (int j = 0; j < numDocsPerThread; j++) {
            myStage.processDocument(docs[i2][j]);
            Thread.sleep(100); // simulate a longer running operation
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
      for (int j = 0; j < numDocsPerThread; j++) {
        assertEquals("doc_" + i + "_" + j, docs[i][j].getId());
        assertEquals("doc_" + i + "_" + j, docs[i][j].getString("field_added_by_python"));
      }
    }
  }

  @Test
  public void testCustomFunctionName() throws Exception {
    String confPath = "PythonStageTest/custom_method.conf";
    stage = factory.get(confPath);
    Document doc = Document.create("doc1");

    stage.processDocument(doc);

    assertEquals("Hello from Python!", doc.getString("field_added_by_python"));
  }

  @Test
  public void testRequirementsInstallAndUsage() throws Exception {
    String confPath = "PythonStageTest/numpy_example.conf";
    stage = factory.get(confPath);
    Document doc = Document.create("doc1");

    stage.processDocument(doc);

    assertEquals(10, doc.getInt("field_added_by_python").intValue());
  }
}
