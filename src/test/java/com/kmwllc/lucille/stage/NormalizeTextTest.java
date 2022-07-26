package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;
import static org.junit.Assert.*;

public class NormalizeTextTest {

  private StageFactory factory = StageFactory.of(NormalizeText.class);

  // TODO : Change this to use multiple configs
  @Test
  public void testLowercase() throws StageException {
    Stage stage = factory.get("NormalizeTextTest/lowercase.conf");
    JsonDocument doc = new JsonDocument("doc");
    doc.setField("input1", "ALL tHiS Is lowerCASED");
    stage.processDocument(doc);
    assertEquals("all this is lowercased", doc.getStringList("output1").get(0));
  }

  @Test
  public void testUppercase() throws Exception {
    Stage stage = factory.get("NormalizeTextTest/uppercase.conf");
    JsonDocument doc2 = new JsonDocument("doc2");
    doc2.setField("input1", "this WILL Be UppERCased");
    stage.processDocument(doc2);
    assertEquals("THIS WILL BE UPPERCASED", doc2.getStringList("output1").get(0));
  }

  @Test
  public void testSentenceCase() throws Exception {
    Stage stage = factory.get("NormalizeTextTest/sentencecase.conf");
    JsonDocument doc4 = new JsonDocument("doc4");
    doc4.setField("input2", "this is a sentence. and this! this too? test");
    stage.processDocument(doc4);
    assertEquals("This is a sentence. And this! This too? Test", doc4.getStringList("output2").get(0));
  }

  @Test
  public void testTitleCase() throws Exception {
    Stage stage = factory.get("NormalizeTextTest/titlecase.conf");
    JsonDocument doc3 = new JsonDocument("doc3");
    doc3.setField("input3", "this will be in title case");
    stage.processDocument(doc3);
    assertEquals("This Will Be In Title Case", doc3.getStringList("output3").get(0));
  }
}
