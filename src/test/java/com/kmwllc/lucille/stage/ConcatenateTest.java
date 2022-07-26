package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.Stage;
import org.junit.Test;
import static org.junit.Assert.*;

public class ConcatenateTest  {

  private StageFactory factory = StageFactory.of(Concatenate.class);

  @Test
  public void testConcatenate() throws Exception {

    Stage stage = factory.get("ConcatenateTest/config.conf");

    // Test placing one field value into the format String
    JsonDocument doc = new JsonDocument("doc");
    doc.setField("country", "US");
    stage.processDocument(doc);
    assertEquals("{city}, {state}, US", doc.getStringList("dest").get(0));

    // Test placing several distinct fields into the format String
    JsonDocument doc2 = new JsonDocument("doc2");
    doc2.setField("country", "Canada");
    doc2.setField("city", "Worsley");
    doc2.setField("state", "Alberta");
    stage.processDocument(doc2);
    assertEquals("Worsley, Alberta, Canada", doc2.getStringList("dest").get(0));

    // Ensure that only the first value of a multivalued field is used placed into the format String
    JsonDocument doc3 = new JsonDocument("doc3");
    doc3.setField("country", "US");
    doc3.addToField("country", "China");
    doc3.setField("city", "San Francisco");
    doc3.setField("state", "California");
    stage.processDocument(doc3);
    assertEquals("San Francisco, California, US", doc3.getStringList("dest").get(0));
  }

  @Test
  public void testConcatenateWDefaults() throws Exception {
    Stage stage = factory.get("ConcatenateTest/defaults.conf");

    JsonDocument doc1 = new JsonDocument("doc1");
    stage.processDocument(doc1);
    assertEquals("{city}, MA, U.S.", doc1.getStringList("dest").get(0));

    JsonDocument doc2 = new JsonDocument("doc2");
    doc2.setField("country", "Canada");
    doc2.setField("city", "Toronto");
    stage.processDocument(doc2);
    assertEquals("Toronto, MA, Canada", doc2.getString("dest"));
  }

}
