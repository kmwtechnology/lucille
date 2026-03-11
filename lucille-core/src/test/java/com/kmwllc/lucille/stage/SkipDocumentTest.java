package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Runner;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.message.TestMessenger;
import java.util.List;
import org.junit.Test;

public class SkipDocumentTest {
  private final StageFactory factory = StageFactory.of(SkipDocument.class);

  @Test
  public void testSkip() throws Exception {
    Stage stage = factory.get("SkipDocumentTest/config.conf");

    Document doc = Document.create("doc");
    // setting a field called .skipped should throw an exception as a reserved field
    assertThrows(IllegalArgumentException.class, () -> doc.setField(".skipped", "test"));

    assertFalse(doc.isSkipped());
    stage.processDocument(doc);
    assertTrue(doc.isSkipped());
  }

  @Test
  public void testSkipConditional() throws Exception {
    Stage stage = factory.get("SkipDocumentTest/conditional_config.conf");

    Document doc1 = Document.create("doc1");
    assertFalse(doc1.isSkipped());
    stage.processConditional(doc1);
    assertFalse(doc1.isSkipped());

    Document doc2 = Document.create("doc2");
    doc2.setField("field", "a");
    assertFalse(doc2.isSkipped());
    stage.processConditional(doc2);
    assertTrue(doc2.isSkipped());
  }

  @Test
  public void testSkipEmittedChild() throws Exception {
    Stage setStaticStage = StageFactory.of(SetStaticValues.class).get("SetStaticValuesTest/config.conf");
    Stage stage = factory.get("SkipDocumentTest/conditional_setStatic.conf");
    Stage addRandomStage = StageFactory.of(AddRandomBoolean.class).get("AddRandomBooleanTest/valid.conf");

    Document doc1 = Document.create("doc1");
    doc1.setField("field", "a");
    assertFalse(doc1.isSkipped());

    // add child docs
    Document childDoc1 = Document.create("childDoc1");
    doc1.addChild(childDoc1);
    childDoc1.setField("field", "b");
    assertFalse(childDoc1.isSkipped());

    Document childDoc2 = Document.create("childDoc2");
    doc1.addChild(childDoc2);
    childDoc2.setField("field", "c");
    assertFalse(childDoc2.isSkipped());

    // process parent
    setStaticStage.processDocument(doc1);
    assertEquals(Integer.valueOf(1), doc1.getInt("myInt"));

    stage.processConditional(doc1);
    assertTrue(doc1.isSkipped());
    assertFalse(childDoc1.isSkipped());

    // process children
    setStaticStage.processDocument(childDoc1);
    assertEquals(Integer.valueOf(1), childDoc1.getInt("myInt"));
    setStaticStage.processDocument(childDoc2);
    assertEquals(Integer.valueOf(1), childDoc2.getInt("myInt"));

    stage.processConditional(childDoc1);
    assertTrue(childDoc1.isSkipped());


    // validate that stages after Skip do not process the doc.
    addRandomStage.processConditional(doc1);
    assertNull(doc1.getBoolean("bool"));

    addRandomStage.processConditional(childDoc1);
    assertNull(childDoc1.getBoolean("bool"));

    // ensure childDoc2 which was not skipped
    addRandomStage.processConditional(childDoc2);
    assertTrue(childDoc2.has("bool"));
  }

  @Test
  public void testSkipParent() throws Exception {
    TestMessenger messenger =
        Runner.runInTestMode("SkipDocumentTest/skipParent.conf").get("connector1");

    // one doc will be sent from the connector
    List<Document> docsSentForProcessing = messenger.getDocsSentForProcessing();
    assertEquals(1, docsSentForProcessing.size());
    Document parent = docsSentForProcessing.get(0);
    assertEquals("1", parent.getId());

    // only skipped parent will be sent for indexing because children should have never been created
    List<Document> docsSentForIndexing = messenger.getDocsSentForIndexing();
    assertEquals(1, docsSentForIndexing.size());
    assertEquals("1", docsSentForIndexing.get(0).getId());
  }

  @Test
  public void testSkipAttachedChild() throws Exception {
    TestMessenger messenger =
        Runner.runInTestMode("SkipDocumentTest/skipChild.conf").get("connector1");

    // one doc will be sent from the connector
    List<Document> docsSentForProcessing = messenger.getDocsSentForProcessing();
    assertEquals(1, docsSentForProcessing.size());
    Document parent = docsSentForProcessing.get(0);
    assertEquals("1", parent.getId());

    List<Document> docsSentForIndexing = messenger.getDocsSentForIndexing();

    // make sure only non-skipped child was processed on the final stage
    assertFalse(docsSentForIndexing.get(0).has("field5")); // child1
    assertTrue(docsSentForIndexing.get(1).has("field5")); // child2
    assertFalse(docsSentForIndexing.get(2).has("field5")); // parent

    // skipped parent, skipped child 1, and child 2 will be sent for indexing
    assertEquals(3, docsSentForIndexing.size());
    assertEquals("1_child1", docsSentForIndexing.get(0).getId());
    assertEquals("1_child2", docsSentForIndexing.get(1).getId());
    assertEquals("1", docsSentForIndexing.get(2).getId());
  }
}
