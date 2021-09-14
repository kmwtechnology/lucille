package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;
import static org.junit.Assert.*;

public class NormalizeTextTest {

  @Test
  public void testNormalizeText() throws StageException {
    Config config = ConfigFactory.load("NormalizeTextTest/config.conf");
    NormalizeText stage = new NormalizeText(config);
    stage.start();

    // Ensure that the lowercase mode works as expected
    Document doc = new Document("doc");
    doc.setField("input1", "ALL tHiS Is lowerCASED");
    stage.processDocument(doc);
    assertEquals("all this is lowercased", doc.getStringList("output1").get(0));

    // Ensure that the uppercase mode works as expected
    stage.setMode("uppercase");
    Document doc2 = new Document("doc2");
    doc2.setField("input1", "this WILL Be UppERCased");
    stage.processDocument(doc2);
    assertEquals("THIS WILL BE UPPERCASED", doc2.getStringList("output1").get(0));

    // Ensure that the title case mode works as expected
    stage.setMode("title_case");
    Document doc3 = new Document("doc3");
    doc3.setField("input3", "this will be in title case");
    stage.processDocument(doc3);
    assertEquals("This Will Be In Title Case", doc3.getStringList("output3").get(0));

    // Ensure that the sentence case mode works as expected
    stage.setMode("sentence_case");
    Document doc4 = new Document("doc4");
    doc4.setField("input2", "this is a sentence. and this! this too? test");
    stage.processDocument(doc4);
    assertEquals("This is a sentence. And this! This too? Test", doc4.getStringList("output2").get(0));
  }
}
