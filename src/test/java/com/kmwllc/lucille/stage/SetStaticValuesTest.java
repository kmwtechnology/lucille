package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import org.junit.Test;

public class SetStaticValuesTest {

  private final StageFactory factory = StageFactory.of(SetStaticValues.class);

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
}
