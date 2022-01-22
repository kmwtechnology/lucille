package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;

public class RemoveEmptyFieldsTest {

  private StageFactory factory = StageFactory.of(RemoveEmptyFields.class);

  @Test
  public void test() throws Exception {
    Stage stage = factory.get("RemoveEmptyFieldsTest/config.conf");
    Document doc = new Document("doc");
    doc.setField("foo", "bar");
    doc.setField("bar", "");
    stage.processDocument(doc);
    assertEquals("bar", doc.getString("foo"));
    assertFalse(doc.has("bar"));
  }

}
