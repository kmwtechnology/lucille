package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;

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
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("ReplacePatternsTest/config.conf");
    assertEquals(
        Set.of(
            "ignore_case",
            "regex",
            "update_mode",
            "multiline",
            "name",
            "source",
            "dest",
            "conditions",
            "replacement",
            "class",
            "dotall",
            "literal"),
        stage.getLegalProperties());
  }
}
