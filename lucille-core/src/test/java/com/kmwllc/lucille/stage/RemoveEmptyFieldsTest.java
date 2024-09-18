package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class RemoveEmptyFieldsTest {

  private StageFactory factory = StageFactory.of(RemoveEmptyFields.class);

  @Test
  public void test() throws Exception {
    Stage stage = factory.get("RemoveEmptyFieldsTest/config.conf");
    Document doc = Document.create("doc");
    doc.setField("foo", "bar");
    doc.setField("bar", "");
    stage.processDocument(doc);
    assertEquals("bar", doc.getString("foo"));
    assertFalse(doc.has("bar"));
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("RemoveEmptyFieldsTest/config.conf");
    assertEquals(Set.of("name", "conditions", "class", "conditionPolicy"), stage.getLegalProperties());
  }
}
