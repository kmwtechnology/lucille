package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import java.util.Set;

public class SplitFieldValuesTest {
  private StageFactory factory = StageFactory.of(SplitFieldValues.class);

  @Test
  public void test() throws Exception {
    Stage stage = factory.get("SplitFieldValuesTest/config.conf");

    Document doc = Document.create("doc");
    doc.setField("data", "this,that, the ,other");
    stage.processDocument(doc);

    assertEquals(4, doc.getStringList("data").size());
    
    assertEquals("this", doc.getStringList("data").get(0));
    assertEquals("that", doc.getStringList("data").get(1));
    assertEquals("the", doc.getStringList("data").get(2));
    assertEquals("other", doc.getStringList("data").get(3));
    
    
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("SplitFieldValuesTest/config.conf");
    assertEquals(
        Set.of(
            "inputField",
            "delimiter",
            "trimWhitespace",
            "name",
            "conditions",
            "class",
            "outputField"),
        stage.getLegalProperties());
  }
}
