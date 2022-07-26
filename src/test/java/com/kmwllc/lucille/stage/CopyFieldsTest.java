package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

public class CopyFieldsTest {

  private StageFactory factory = StageFactory.of(CopyFields.class);

  @Test
  public void testCopyFieldsReplace() throws Exception {
    Stage stage = factory.get("CopyFieldsTest/replace.conf");

    // Ensure that one field is correctly copied over
    JsonDocument doc = new JsonDocument("doc");
    String inputVal = "This will be copied to output1";
    doc.setField("input1", inputVal);
    stage.processDocument(doc);
    assertEquals("Value from input1 should be copied to output1", inputVal, doc.getStringList("output1").get(0));

    // Ensure that field 2 in the source list is copied to output2
    JsonDocument doc2 = new JsonDocument("doc2");
    inputVal = "This will be copied to output2";
    doc2.setField("input2", inputVal);
    doc2.setField("output2", "here's some junk data.");
    stage.processDocument(doc2);
    assertEquals("Value from input2 should be copied to output2", inputVal, doc2.getStringList("output2").get(0));

    // Ensure that several fields can be copied at the same time.
    JsonDocument doc3 = new JsonDocument("doc3");
    String inputVal1 = "This will be copied to output1";
    String inputVal2 = "This will be copied to output2";
    String inputVal3 = "This will be copied to output3";
    doc3.setField("input1", inputVal1);
    doc3.setField("input2", inputVal2);
    doc3.setField("input3", inputVal3);
    stage.processDocument(doc3);
    assertEquals("Value from input1 should be copied to output1", inputVal1, doc3.getStringList("output1").get(0));
    assertEquals("Value from input2 should be copied to output2", inputVal2, doc3.getStringList("output2").get(0));
    assertEquals("Value from input3 should be copied to output3", inputVal3, doc3.getStringList("output3").get(0));
  }

  @Test
  public void testCopyFieldsSkip() throws Exception {
    Stage stage = factory.get("CopyFieldsTest/skip.conf");

    JsonDocument doc = new JsonDocument("doc");
    doc.setField("input1", "Here is some input.");
    doc.setField("input2", "Here is another input.");
    doc.setField("input3", "This will be skipped along with input 1.");
    doc.setField("output1", "input1 should be skipped.");
    doc.setField("output3", "input3 should be skipped.");
    stage.processDocument(doc);
    assertEquals("input1 should be skipped.", doc.getStringList("output1").get(0));
    assertEquals("Here is another input.", doc.getStringList("output2").get(0));
    assertEquals("input3 should be skipped.", doc.getStringList("output3").get(0));
  }
}
