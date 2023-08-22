package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.StageFactory;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.*;

public class NormalizeFieldNamesTest {

  StageFactory factory = StageFactory.of(NormalizeFieldNames.class);

  @Test
  public void testNormalizeFieldNames() throws StageException {
    Stage stage = factory.get("FieldNormalizerTest/config.conf");

    Document doc1 = Document.create("doc1");
    doc1.setField("input1", "test1");
    doc1.setField("input 2", "test2");
    doc1.setField("input 3 (test123)", "test3");
    doc1.setField(".sonarcontent", "content");
    stage.processDocument(doc1);
    assertTrue(doc1.has("input1"));
    assertFalse(doc1.has("input 2"));
    assertTrue(doc1.has("input_2"));
    assertFalse(doc1.has("input 3 (test123)"));
    assertTrue(doc1.has("input_3_test123"));
    assertTrue(doc1.has(".sonarcontent"));
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("FieldNormalizerTest/config.conf");
    assertEquals(
        Set.of("delimiter", "name", "conditions", "class", "nonAlphanumReplacement"),
        stage.getLegalProperties());
  }
}
