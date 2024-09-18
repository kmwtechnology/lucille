package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;

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

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("DropValuesTest/config.conf");
    assertEquals(
        Set.of("values", "name", "source", "conditions", "class", "conditionPolicy"), stage.getLegalProperties());
  }
}
