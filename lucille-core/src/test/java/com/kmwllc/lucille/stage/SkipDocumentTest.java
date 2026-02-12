package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
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
  public void testSkipAfterWithChild() throws Exception {
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
}
