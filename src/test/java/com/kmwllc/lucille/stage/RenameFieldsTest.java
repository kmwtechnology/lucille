package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;
import static org.junit.Assert.*;

public class RenameFieldsTest {

  @Test
  public void testRenameFields() throws Exception {
    Config config = ConfigFactory.load("RenameFieldsTest/config.conf");
    Stage stage = new RenameFields(config);
    stage.start();

    Document doc = new Document("doc");
    String fieldVal = "this will be renamed to output1";
    doc.setField("input1", fieldVal);
    stage.processDocument(doc);
    assertEquals("Field was not correctly renamed", doc.getStringList("output1").get(0), fieldVal);

    Document doc2 = new Document("doc2");
    doc.setField("input1", "this will be output1");
    doc.setField("input2", "this will be output2");
    doc.setField("input3", "this will be output3");
    stage.processDocument(doc2);
    assertEquals("Field was not correctly renamed", doc.getStringList("input1").get(0),
        "this will be output1");
    assertEquals("Field was not correctly renamed", doc.getStringList("input2").get(0),
        "this will be output2");
    assertEquals("Field was not correctly renamed", doc.getStringList("input3").get(0),
        "this will be output3");
  }

}
