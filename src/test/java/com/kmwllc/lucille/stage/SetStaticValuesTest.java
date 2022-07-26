package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.Stage;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SetStaticValuesTest {

  private StageFactory factory = StageFactory.of(SetStaticValues.class);

  @Test
  public void test() throws Exception {
    Stage stage = factory.get("SetStaticValuesTest/config.conf");

    JsonDocument doc = new JsonDocument("doc");
    stage.processDocument(doc);

    assertEquals("File", doc.getString("type"));

    doc.setField("type", "Directory");
    stage.processDocument(doc);

    assertEquals("Directory", doc.getString("type"));
  }

}
