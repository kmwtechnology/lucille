package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class DeleteFieldsTest {

  private StageFactory factory = StageFactory.of(DeleteFields.class);

  @Test
  public void testDeleteFields() throws StageException {
    Stage stage = factory.get("DeleteFieldsTest/config.conf");

    Document doc = Document.create("doc");
    doc.setField("save", "this will be kept");
    doc.setField("delete", "this will be removed");
    stage.processDocument(doc);
    assertFalse(doc.has("delete"));
    assertEquals("this will be kept", doc.getString("save"));

    Document doc2 = Document.create("doc2");
    doc2.setField("delete", "delete this");
    doc2.addToField("delete", "this too");
    doc2.setField("remove", "remove this");
    doc2.setField("save", "save this one");
    doc2.setField("save2", "and this one");
    stage.processDocument(doc2);
    assertFalse(doc2.has("delete"));
    assertFalse(doc2.has("remove"));
    assertEquals("save this one", doc2.getString("save"));
    assertEquals("and this one", doc2.getString("save2"));
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("DeleteFieldsTest/config.conf");
    assertEquals(Set.of("name", "fields", "conditions", "class", "conditionPolicy"), stage.getLegalProperties());
  }
}
