package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
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
    doc2.setField("input1", "this will be output1");
    doc2.setField("input2", "this will be output2");
    doc2.setField("input3", "this will be output3");
    stage.processDocument(doc2);
    assertEquals("Field was not correctly renamed", doc2.getStringList("output1").get(0),
        "this will be output1");
    assertEquals("Field was not correctly renamed", doc2.getStringList("output2").get(0),
        "this will be output2");
    assertEquals("Field was not correctly renamed", doc2.getStringList("output3").get(0),
        "this will be output3");
  }

  //do we want any fields named the same on the child and parent doc to both be renamed?
  @Test
  public void testRenameFieldsWithChildrenSameFieldName() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("RenameFieldsTest/config.conf")
        .withValue("applyToChildren", ConfigValueFactory.fromAnyRef(true));

    Stage stage = factory.get(config);

    // Ensure one field is correctly renamed
    Document doc = Document.create("doc");
    String fieldVal = "this will be renamed to output1";
    doc.setField("input1", fieldVal);

    Document childDoc = Document.create("childDoc");
    String childFieldVal = "this will be renamed to output1";
    childDoc.setField("input1", childFieldVal);

    doc.addChild(childDoc);
    stage.processDocument(doc);

    assertEquals("Field was not correctly renamed", doc.getStringList("output1").get(0), fieldVal);

    Document renamedChild = doc.getChildren().get(0);
    assertEquals(renamedChild.getStringList("output1").get(0), childFieldVal);
  }

  @Test
  public void testRenameFieldsWithGrandchildren() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("RenameFieldsTest/config.conf")
        .withValue("applyToChildren", ConfigValueFactory.fromAnyRef(true));

    Stage stage = factory.get(config);

    Document doc = Document.create("doc");
    String fieldVal = "this will be renamed to output1";
    doc.setField("input1", fieldVal);

    Document childDoc = Document.create("childDoc");
    String childFieldVal = "this will be renamed to output2";
    childDoc.setField("input2", childFieldVal);

    Document grandchildDoc = Document.create("grandchildDoc");
    String grandchildFieldVal = "this will be renamed to output3";
    grandchildDoc.setField("input3", grandchildFieldVal);

    childDoc.addChild(grandchildDoc);
    doc.addChild(childDoc);

    stage.processDocument(doc);

    assertEquals(fieldVal, doc.getStringList("output1").get(0));

    Document renamedChild = doc.getChildren().get(0);
    assertEquals(childFieldVal, renamedChild.getStringList("output2").get(0));

    Document renamedGrandchild = renamedChild.getChildren().get(0);
    assertEquals(grandchildFieldVal, renamedGrandchild.getStringList("output3").get(0));
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("RenameFieldsTest/config.conf");
    assertEquals(Set.of("updateMode", "applyToChildren", "name", "conditions", "class", "conditionPolicy", "fieldMapping"), stage.getLegalProperties());
  }
}
