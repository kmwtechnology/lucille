package com.kmwllc.lucille.stage;

import static org.junit.Assert.*;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

public class DropDocumentTest {

  private final StageFactory factory = StageFactory.of(DropDocument.class);

  @Test
  public void testDropped() throws StageException {

    Stage stage = factory.get("DropDocumentTest/config.conf");

    Document doc = Document.create("doc");

    assertFalse(doc.isDropped());
    stage.processDocument(doc);
    assertTrue(doc.isDropped());
  }

  @Test
  public void testDroppedConditional() throws StageException {

    Stage stage = factory.get("DropDocumentTest/conditional.conf");

    Document doc1 = Document.create("doc1");
    doc1.setField("field", "a");

    assertFalse(doc1.isDropped());
    stage.processConditional(doc1);
    assertTrue(doc1.isDropped());

    Document doc2 = Document.create("doc2");
    doc2.setField("field", "b");

    assertFalse(doc2.isDropped());
    stage.processConditional(doc2);
    assertFalse(doc2.isDropped());
  }

  @Test
  public void testDroppedAfterLookup() throws StageException {

    Stage lookupStage = StageFactory.of(DictionaryLookup.class).get("DictionaryLookupTest/set_config.conf");
    Stage drop = factory.get("DropDocumentTest/conditional_lookup.conf");

    Document doc1 = Document.create("doc1");
    doc1.setField("field", "a");
    assertFalse(doc1.isDropped());

    lookupStage.processDocument(doc1);
    assertTrue(doc1.getBoolean("setContains"));

    drop.processConditional(doc1);
    assertTrue(doc1.isDropped());
  }

  @Test
  public void testBooleanConditional() throws StageException {
    // test that both boolean literal value and string boolean can be matched
    Stage stage = factory.get("DropDocumentTest/conditional_booleans.conf");

    Document doc1 = Document.create("doc1");
    doc1.setField("field1", true);
    doc1.setField("field2", false);

    assertFalse(doc1.isDropped());
    stage.processConditional(doc1);
    assertFalse(doc1.isDropped());

    Document doc2 = Document.create("doc2");
    doc2.setField("field1", true);
    doc2.setField("field2", true);

    assertFalse(doc2.isDropped());
    stage.processConditional(doc2);
    assertTrue(doc2.isDropped());
  }

  @Test
  public void testNumberConditional() throws StageException {
    // test that both int literal value and string int can be matched
    Stage stage = factory.get("DropDocumentTest/conditional_ints.conf");

    Document doc1 = Document.create("doc1");
    doc1.setField("field1", 12);
    doc1.setField("field2", 12);

    assertFalse(doc1.isDropped());
    stage.processConditional(doc1);
    assertFalse(doc1.isDropped());

    Document doc2 = Document.create("doc2");
    doc2.setField("field1", 12);
    doc2.setField("field2", 123.5);

    assertFalse(doc2.isDropped());
    stage.processConditional(doc2);
    assertTrue(doc2.isDropped());
  }
}