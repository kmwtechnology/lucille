package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class CopyFieldsTest {

  @Test
  public void testCopyFields() throws Exception {
    Config config = ConfigFactory.load("CopyFieldsTest/config.conf");
    CopyFields stage = new CopyFields(config);
    stage.start();

    Document doc = new Document("doc");
    String inputVal = "This will be copied to output1";
    doc.setField("input1", inputVal);
    stage.processDocument(doc);
    assertEquals("Value from input1 should be copied to output1", inputVal, doc.getString("output1"));

    Document doc2 = new Document("doc2");
    inputVal = "This will be copied to output2";
    doc2.setField("input2", inputVal);
    stage.processDocument(doc2);
    assertEquals("Value from input2 should be copied to output2", inputVal, doc2.getString("output2"));

    Document doc3 = new Document("doc3");
    String inputVal1 = "This will be copied to output1";
    String inputVal2 = "This will be copied to output2";
    String inputVal3 = "This will be copied to output3";
    doc3.setField("input1", inputVal1);
    doc3.setField("input2", inputVal2);
    doc3.setField("input3", inputVal3);
    stage.processDocument(doc3);
    assertEquals("Value from input1 should be copied to output1", inputVal1, doc3.getString("output1"));
    assertEquals("Value from input2 should be copied to output2", inputVal2, doc3.getString("output2"));
    assertEquals("Value from input3 should be copied to output3", inputVal3, doc3.getString("output3"));
  }

}
