package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;

public class SetStaticValuesTest {

  private StageFactory factory = StageFactory.of(SetStaticValues.class);

  @Test
  public void testSetValues() throws Exception {
    Stage stage = factory.get("SetStaticValuesTest/config.conf");

    Document doc = Document.create("doc");
    stage.processDocument(doc);

    assertEquals("File", doc.getString("myString"));
    assertEquals(1, doc.getInt("myInt").intValue());
    assertEquals(true, doc.getBoolean("myBool"));

    doc.setField("myString", "Directory");
    doc.setField("myInt", 2);
    doc.setField("myBool", false);

    stage.processDocument(doc);

    // update_mode is set to "skip" in the config
    assertEquals("Directory", doc.getString("myString"));
    assertEquals(2, doc.getInt("myInt").intValue());
    assertEquals(false, doc.getBoolean("myBool"));
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("SetStaticValuesTest/config.conf");
    assertEquals(Set.of("update_mode", "name", "conditions", "class", "conditionPolicy"), stage.getLegalProperties());
  }
}
