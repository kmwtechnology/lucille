package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.Stage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class ApplyRegexTest {

  private StageFactory factory = StageFactory.of(ApplyRegex.class);

  @Test
  public void testApplyRegex() throws Exception {
    Stage stage = factory.get("ApplyRegexTest/config.conf");

    // Test extracting a single pattern match
    JsonDocument doc = new JsonDocument("doc");
    doc.setField("input1", "here is a number 12");
    stage.processDocument(doc);
    assertEquals("Regex pattern should extract numbers from the input", "12",
        doc.getStringList("output1").get(0));

    // Test extracting several pattern matches
    JsonDocument doc2 = new JsonDocument("doc2");
    doc2.setField("input1", "here are some numbers: 1, 2, 3, 4, 5");
    stage.processDocument(doc2);
    List<String> tokens = doc2.getStringList("output1");
    for (int i = 1; i <= 5; i++) {
      assertEquals("Regex should extract all the numbers in order", String.valueOf(i), tokens.get(i - 1));
    }

    // Test inputting/outputting to the third field in the list
    JsonDocument doc3 = new JsonDocument("doc3");
    doc3.setField("input3", "this is field #3");
    stage.processDocument(doc3);
    assertEquals("Field output3 should contain the extracted values from input3",
        doc3.getStringList("output3").get(0), "3");

    // Test extracting from several source fields to several destination fields
    JsonDocument doc4 = new JsonDocument("doc4");
    doc4.setField("input1", "this is field input 1");
    doc4.setField("input2", "this is field input 2");
    doc4.setField("input3", "this is field input 3");
    stage.processDocument(doc4);
    assertEquals("output1 should contain values from input", "1", doc4.getStringList("output1").get(0));
    assertEquals("output2 should contain values from input2", "2", doc4.getStringList("output2").get(0));
    assertEquals("output3 should contain values from input3", "3", doc4.getStringList("output3").get(0));
  }

  @Test
  public void testCapturingGroup() throws Exception {
    Stage stage = factory.get("ApplyRegexTest/capturing.conf");

    JsonDocument doc = new JsonDocument("1");
    doc.setField("input", "test~123");
    stage.processDocument(doc);
    assertEquals("123", doc.getString("output"));
  }
}
