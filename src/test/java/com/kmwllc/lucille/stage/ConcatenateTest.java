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
    assertEquals("{city}, {state}, US", doc.getString("dest"));

    // Test placing several distinct fields into the format String
    Document doc2 = new Document("doc2");
    doc.setField("country", "Canada");
    doc.setField("city", "Worsley");
    doc.setField("state", "Alberta");
    stage.processDocument(doc);
    assertEquals("Worsley, Alberta, Canada", doc.getString("dest"));

    // Ensure that only the first value of a multivalued field is used placed into the format String
    Document doc3 = new Document("doc3");
    doc.setField("country", "US");
    doc.addToField("country", "China");
    doc.setField("city", "San Francisco");
    doc.setField("state", "California");
    stage.processDocument(doc);
    assertEquals("San Francisco, California, US", doc.getString("dest"));
  }

}
