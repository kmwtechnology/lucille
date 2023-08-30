package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;

public class ConcatenateTest {

  private StageFactory factory = StageFactory.of(Concatenate.class);

  @Test
  public void testConcatenate() throws Exception {

    Stage stage = factory.get("ConcatenateTest/config.conf");

    // Test placing one field value into the format String
    Document doc = Document.create("doc");
    doc.setField("country", "US");
    stage.processDocument(doc);
    assertEquals("{city}, {state}, US", doc.getStringList("dest").get(0));

    // Test placing several distinct fields into the format String
    Document doc2 = Document.create("doc2");
    doc2.setField("country", "Canada");
    doc2.setField("city", "Worsley");
    doc2.setField("state", "Alberta");
    stage.processDocument(doc2);
    assertEquals("Worsley, Alberta, Canada", doc2.getStringList("dest").get(0));

    // Ensure that only the first value of a multivalued field is used placed into the format String
    Document doc3 = Document.create("doc3");
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

    Document doc1 = Document.create("doc1");
    stage.processDocument(doc1);
    assertEquals("{city}, MA, U.S.", doc1.getStringList("dest").get(0));

    Document doc2 = Document.create("doc2");
    doc2.setField("country", "Canada");
    doc2.setField("city", "Toronto");
    stage.processDocument(doc2);
    assertEquals("Toronto, MA, Canada", doc2.getString("dest"));
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("ConcatenateTest/config.conf");
    assertEquals(
        Set.of("update_mode", "name", "format_string", "dest", "conditions", "class"),
        stage.getLegalProperties());
  }
}
