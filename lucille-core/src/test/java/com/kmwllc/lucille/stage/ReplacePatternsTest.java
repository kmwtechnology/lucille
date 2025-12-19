package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class ReplacePatternsTest {

  private StageFactory factory = StageFactory.of(ReplacePatterns.class);

  @Test
  public void testReplacePatterns() throws Exception {
    Stage stage = factory.get("ReplacePatternsTest/config.conf");

    Document doc = Document.create("doc");
    doc.setField("input1", "The term false should be replaced.");
    stage.processDocument(doc);
    assertEquals("The term REPLACED should be replaced.", doc.getStringList("output1").get(0));

    Document doc2 = Document.create("doc2");
    doc2.setField("input2", "false should be replaced wherever false is found, there should be no false left.");
    stage.processDocument(doc2);
    assertEquals("REPLACED should be replaced wherever REPLACED is found, there should be no REPLACED left.",
        doc2.getStringList("output2").get(0));

    Document doc3 = Document.create("doc3");
    doc3.setField("input1", "false remove this should be kept false");
    doc3.setField("input2", "remove remove remove");
    doc3.setField("input3", "This should be untouched");
    stage.processDocument(doc3);
    assertEquals("REPLACED REPLACED this should be kept REPLACED", doc3.getStringList("output1").get(0));
    assertEquals("REPLACED REPLACED REPLACED", doc3.getStringList("output2").get(0));
    assertEquals("This should be untouched", doc3.getStringList("output3").get(0));
  }

  @Test 
  public void testOneFlag() throws Exception {
    Stage stage = factory.get("ReplacePatternsTest/one_flag.conf");

    Document doc = Document.create("doc");
    doc.setField("input1", "The term fAlse should be replaced.");
    stage.processDocument(doc);
    assertEquals("The term REPLACED should be replaced.", doc.getStringList("output1").get(0));
  }

  @Test 
  public void testTwoFlag() throws Exception {
    Stage stage = factory.get("ReplacePatternsTest/two_flags.conf");

    Document doc = Document.create("doc");
    doc.setField("input1", "The term fAlse should be replaced,\n but not this false");
    stage.processDocument(doc);
    assertEquals("The term REPLACED should be replaced,\n but not this REPLACED", doc.getStringList("output1").get(0));
  }

  @Test 
  public void testThreeFlag() throws Exception {
    Stage stage = factory.get("ReplacePatternsTest/three_flags.conf");

    Document doc = Document.create("doc");
    doc.setField("input1", "The term fAlse\\\\ should be replaced,\n but not this false");
    stage.processDocument(doc);
    assertEquals("The term REPLACED\\ should be replaced,\n but not this false", doc.getStringList("output1").get(0));
  }

  @Test 
  public void testFourFlag() throws Exception {
    Stage stage = factory.get("ReplacePatternsTest/four_flags.conf");

    Document doc = Document.create("doc");
    doc.setField("input1", "The term f.Alse\\\\ should be replaced,\n but not this false");
    stage.processDocument(doc);
    assertEquals("The term REPLACED\\ should be replaced,\n but not this false", doc.getStringList("output1").get(0));
  }

  @Test
  public void testReplacementField() throws Exception {
    Stage stage = factory.get("ReplacePatternsTest/replacement_field.conf");

    Document replacement_doc = Document.create("replacement_document");
    replacement_doc.setField("input1", "The term false should be replaced.");
    replacement_doc.setField("replacement_string", "OVERWRITTEN");
    stage.processDocument(replacement_doc);
    assertEquals("The term OVERWRITTEN should be replaced.", replacement_doc.getStringList("output1").get(0));
  }

  @Test
  public void testReplacementFieldFallback() throws Exception {
    Stage stage = factory.get("ReplacePatternsTest/replacement_field.conf");

    // 1. Fallback to "replacement" if the replacement_field isn't in the document.
    Document doc = Document.create("doc");
    doc.setField("input1", "The term false should be replaced.");
    stage.processDocument(doc);
    assertEquals("The term REPLACED should be replaced.", doc.getStringList("output1").get(0));

    // 2. Fallback to "replacement" if the replacement_field is set to null.
    doc = Document.create("doc");
    doc.setField("input1", "The term false should be replaced.");
    doc.setField("replacement_string", (String) null);
    stage.processDocument(doc);
    assertEquals("The term REPLACED should be replaced.", doc.getStringList("output1").get(0));
  }

  @Test
  public void testNoPatternMatching() throws Exception {
    // 1. No matching if no replacement and the replacement_field is not in a document
    Stage replacementFieldNoFallback = factory.get("ReplacePatternsTest/replacement_field_no_fallback.conf");
    Document doc = Document.create("doc");
    doc.setField("input1", "The term false will not be replaced.");
    replacementFieldNoFallback.processDocument(doc);
    assertNull(doc.getStringList("output1"));

    // 2. No matching if no replacement and the replacement_field is set to null in a document
    Document docWithNullReplacement = Document.create("doc_with_null");
    docWithNullReplacement.setField("input1", "The term false will not be replaced.");
    docWithNullReplacement.setField("replacement_string", (String) null);
    replacementFieldNoFallback.processDocument(docWithNullReplacement);
    assertNull(docWithNullReplacement.getStringList("output1"));
  }

  @Test
  public void testInvalidConfig() throws Exception {
    // This .conf has no replacement or replacement_field.
    assertThrows(StageException.class,
        () -> factory.get("ReplacePatternsTest/no_fallback.conf")
    );
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("ReplacePatternsTest/config.conf");
    assertEquals(
        Set.of(
            "ignoreCase",
            "regex",
            "updateMode",
            "multiline",
            "name",
            "source",
            "dest",
            "conditions",
            "replacement",
            "replacementField",
            "class",
            "dotall",
            "literal",
            "conditionPolicy"),
        stage.getLegalProperties());
  }
}
