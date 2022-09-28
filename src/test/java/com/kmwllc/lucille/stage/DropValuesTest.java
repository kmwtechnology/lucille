package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;
import static org.junit.Assert.*;

public class DropValuesTest {

  private StageFactory factory = StageFactory.of(DropValues.class);

  @Test
  public void testDropValues() throws Exception {
    Stage stage = factory.get("DropValuesTest/config.conf");

    // Ensure that only exact matches are dropped
    Document doc = Document.create("doc");
    doc.addToField("input", "don't drop this");
    doc.addToField("input", "drop");
    stage.processDocument(doc);
    assertEquals(1, doc.getStringList("input").size());
    assertEquals("don't drop this", doc.getStringList("input").get(0));

    // Ensure that the correct values are dropped from a complex multivalued field
    Document doc2 = Document.create("doc2");
    doc2.addToField("input", "keep this");
    doc2.addToField("input", "drop");
    doc2.addToField("input", "keep this as well");
    doc2.addToField("input", "1234");
    doc2.addToField("input", "5678");
    stage.processDocument(doc2);
    assertEquals(3, doc2.getStringList("input").size());
    assertEquals("keep this", doc2.getStringList("input").get(0));
    assertEquals("keep this as well", doc2.getStringList("input").get(1));
    assertEquals("5678", doc2.getStringList("input").get(2));
  }

}
