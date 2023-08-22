package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.StageFactory;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;

public class SetStaticValuesTest {

  private StageFactory factory = StageFactory.of(SetStaticValues.class);

  @Test
  public void test() throws Exception {
    Stage stage = factory.get("SetStaticValuesTest/config.conf");

    Document doc = Document.create("doc");
    stage.processDocument(doc);

    assertEquals("File", doc.getString("type"));

    doc.setField("type", "Directory");
    stage.processDocument(doc);

    assertEquals("Directory", doc.getString("type"));
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("SetStaticValuesTest/config.conf");
    assertEquals(Set.of("update_mode", "name", "conditions", "class"), stage.getLegalProperties());
  }
}
