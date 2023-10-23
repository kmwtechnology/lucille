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
}