package com.kmwllc.lucille.stage;

import java.util.List;
import java.util.Map;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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
  public void testSpec() throws StageException {
    Stage stage = factory.get("DropValuesTest/config.conf");
    assertEquals(Set.of("values", "source"), stage.getNonDefaultLegalProperties());
  }

  @Test
  public void testOnlyRewritesMatchedFields() throws Exception {
    Stage stage = factory.get(Map.of("source", List.of("status", "count", "tags", "gone"), "values", List.of("x")));
    Document doc = Document.createFromJson(
        "{\"id\":\"1\",\"status\":\"active\",\"count\":5,\"tags\":[1,\"x\",true],\"gone\":\"x\"}");
    stage.processDocument(doc);

    assertFalse(doc.isMultiValued("status"));
    assertEquals("active", doc.getString("status"));
    assertEquals(Integer.valueOf(5), doc.getInt("count"));
    assertTrue(doc.getJsonList("tags").get(0).isInt());
    assertTrue(doc.getJsonList("tags").get(1).isBoolean());
    assertEquals(2, doc.getJsonList("tags").size());
    assertFalse(doc.has("gone"));
  }
}
