package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;

public class RenameFieldsTest {

  private StageFactory factory = StageFactory.of(RenameFields.class);

  @Test
  public void testRenameFields() throws Exception {
    Stage stage = factory.get("RenameFieldsTest/config.conf");

    // Ensure one field is correctly renamed
    Document doc = Document.create("doc");
    String fieldVal = "this will be renamed to output1";
    doc.setField("input1", fieldVal);
    stage.processDocument(doc);
    assertEquals("Field was not correctly renamed", doc.getStringList("output1").get(0), fieldVal);

    // Ensure several fields are correctly renamed
    Document doc2 = Document.create("doc2");
    doc.setField("input1", "this will be output1");
    doc.setField("input2", "this will be output2");
    doc.setField("input3", "this will be output3");
    stage.processDocument(doc2);
    assertEquals("Field was not correctly renamed", doc.getStringList("input1").get(0),
        "this will be output1");
    assertEquals("Field was not correctly renamed", doc.getStringList("input2").get(0),
        "this will be output2");
    assertEquals("Field was not correctly renamed", doc.getStringList("input3").get(0),
        "this will be output3");
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("RenameFieldsTest/config.conf");
    assertEquals(Set.of("updateMode", "name", "conditions", "class", "conditionPolicy", "fieldMapping"), stage.getLegalProperties());
  }
}
