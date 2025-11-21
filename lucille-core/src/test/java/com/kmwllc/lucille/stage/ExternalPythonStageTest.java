package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import java.util.HashSet;
import java.util.Set;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;

public class ExternalPythonStageTest {

  private final StageFactory factory = StageFactory.of(ExternalPythonStage.class);
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
    String confPath = "ExternalPythonStageTest/process_document_1.conf";
    stage = factory.get(confPath);
    Document doc = Document.create("doc1");
    stage.processDocument(doc);
    stage.processDocument(doc);
    stage.processDocument(doc);
    assertEquals("Hello from process_document_1.py", doc.getString("field_added_by_python"));
  }

  @Test
  public void testPythonUpdateToDocMultiThreaded() throws Exception {
    String confPath = "ExternalPythonStageTest/copy_doc_id.conf";
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
  public void testNoReturn() throws Exception {
    String confPath = "ExternalPythonStageTest/no_return.conf";
    stage = factory.get(confPath);
    Document doc = Document.create("doc1");
    doc.setField("field1", "field1val");
    doc.setField("field2", "field2val");

    Set<String> beforeFields = new HashSet<>(doc.getFieldNames());
    stage.processDocument(doc);
    Set<String> afterFields = new HashSet<>(doc.getFieldNames());

    assertEquals(beforeFields, afterFields);
  }

  @Test
  public void testCustomFunctionName() throws Exception {
    String confPath = "ExternalPythonStageTest/custom_method.conf";
    stage = factory.get(confPath);
    Document doc = Document.create("doc1");

    stage.processDocument(doc);

    assertEquals("Hello from custom_method.py (method 1)", doc.getString("field_added_by_python"));
  }

  @Test
  public void testCustomPort() throws Exception {
    String confPath = "ExternalPythonStageTest/custom_port.conf";
    stage = factory.get(confPath);
    Document doc = Document.create("doc1");

    stage.processDocument(doc);

    assertEquals("Hello from custom_port.py", doc.getString("field_added_by_python"));
  }

  @Test
  public void testRequirementsInstallAndUsage() throws Exception {
    String confPath = "ExternalPythonStageTest/numpy.conf";
    stage = factory.get(confPath);
    Document doc = Document.create("doc1");

    stage.processDocument(doc);

    assertEquals(10, doc.getInt("field_added_by_python").intValue());
  }

  @Test
  public void testConflictingConfigs() throws Exception {
    // We can initialize two PythonStage instances that use the same config,
    // therefore passing the same parameters to Py4JRuntimeManager.acquire()
    Stage stage1 = factory.get("ExternalPythonStageTest/process_document_1.conf");
    Stage stage2 = factory.get("ExternalPythonStageTest/process_document_1.conf");

    // An exception should be thrown if we attempt to initialize a PythonStage instance
    // that results in different parameters being passed to Py4jRuntimeManager.acquire()
    assertThrows(StageException.class, () -> factory.get("ExternalPythonStageTest/copy_doc_id.conf"));

    stage1.stop();
    assertThrows(StageException.class, () -> factory.get("ExternalPythonStageTest/copy_doc_id.conf"));
    stage2.stop();

    // Once we have stopped all existing PythonStage instances we should be able to
    // create a new instance with a different config
    Stage stage3 = factory.get("ExternalPythonStageTest/copy_doc_id.conf");
    stage3.stop();
  }

  @Test
  public void testCompatibleConfigs() throws Exception {

    // We can start two PythonStage instances that use different configs
    // but have the same pythonExecutable, scriptPath, requirementsPath, and port;
    // in this case the difference in the configs is the value of "functionName";
    // this scenario works because both functions are available in the same python script
    // at the same scriptPath

    Stage stage1 = factory.get("ExternalPythonStageTest/custom_method.conf");
    Stage stage2 = factory.get("ExternalPythonStageTest/custom_method2.conf");
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");

    stage1.processDocument(doc1);
    stage2.processDocument(doc2);

    assertEquals("Hello from custom_method.py (method 1)", doc1.getString("field_added_by_python"));
    assertEquals("Hello from custom_method.py (method 2)", doc2.getString("field_added_by_python"));
    stage1.stop();
    stage2.stop();
  }

  @Test
  public void testDocWithNestedJson() throws Exception {
    String confPath = "ExternalPythonStageTest/process_document_1.conf";
    stage = factory.get(confPath);
    String json = "{\"id\":\"doc1\", \"field1\": {\"field2\": [1, 2, 3], \"field4\": false, \"field5\": {\"field6\": true}}}";
    Document doc = Document.createFromJson(json);
    stage.processDocument(doc);
    assertEquals("Hello from process_document_1.py", doc.getString("field_added_by_python"));
    doc.removeField("field_added_by_python");
    assertEquals(Document.createFromJson(json), doc);
  }

  @Test
  public void testRemoveField() throws Exception {
    String confPath = "ExternalPythonStageTest/remove_field.conf";
    stage = factory.get(confPath);
    Document doc = Document.create("doc1");
    doc.setField("field1", "field1Value");
    doc.setField("field2", "field2Value");
    stage.processDocument(doc);
    assertTrue(doc.has("field1"));
    assertEquals("field1Value", doc.getString("field1"));
    assertFalse(doc.has("field2"));
  }
}
