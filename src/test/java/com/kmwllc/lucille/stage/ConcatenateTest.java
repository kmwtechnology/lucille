package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;
import static org.junit.Assert.*;

public class ConcatenateTest {

  @Test
  public void testConcatenate() throws Exception {
    Config config = ConfigFactory.load("ConcatenateTest/config.conf");
    Stage stage = new Concatenate(config);
    stage.start();

    // Test placing one field value into the format String
    Document doc = new Document("doc");
    doc.setField("country", "US");
    stage.processDocument(doc);
    assertEquals("{city}, {state}, US", doc.getStringList("dest").get(0));

    // Test placing several distinct fields into the format String
    Document doc2 = new Document("doc2");
    doc2.setField("country", "Canada");
    doc2.setField("city", "Worsley");
    doc2.setField("state", "Alberta");
    stage.processDocument(doc2);
    assertEquals("Worsley, Alberta, Canada", doc2.getStringList("dest").get(0));

    // Ensure that only the first value of a multivalued field is used placed into the format String
    Document doc3 = new Document("doc3");
    doc3.setField("country", "US");
    doc3.addToField("country", "China");
    doc3.setField("city", "San Francisco");
    doc3.setField("state", "California");
    stage.processDocument(doc3);
    assertEquals("San Francisco, California, US", doc3.getStringList("dest").get(0));
  }

}
