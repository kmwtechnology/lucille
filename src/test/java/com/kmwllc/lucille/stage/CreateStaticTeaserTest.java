package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;
import static org.junit.Assert.*;

public class CreateStaticTeaserTest {

  @Test
  public void testCreateStaticTeaser() throws Exception {
    Config config = ConfigFactory.load("CreateStaticTeaserTest/config.conf");
    Stage stage = new CreateStaticTeaser(config);
    stage.start();

    Document doc = new Document("doc");
    String inStr = "Smaller than the char limit";
    doc.addToField("input1", inStr);
    stage.processDocument(doc);
    assertEquals(inStr, doc.getStringList("teaser1").get(0));

    Document doc2 = new Document("doc2");
    doc2.addToField("input1", "Here is a teaser that is longer than the char limit. The whole first sentence should be extracted!");
    stage.processDocument(doc2);
    assertEquals("Here is a teaser that is longer than the char limit.", doc2.getStringList("teaser1").get(0));

    Document doc3 = new Document("doc3");
    doc3.addToField("input1", "Here is a teaser that is longer than the char limit. The whole first sentence should be extracted!");
    doc3.addToField("input2", "This teaser will get cut off in the middle of a sentence, but not in the middle of a word?");
    stage.processDocument(doc3);
    assertEquals("Here is a teaser that is longer than the char limit.", doc3.getStringList("teaser1").get(0));
    assertEquals("This teaser will get cut off in the middle of a sentence,", doc3.getStringList("teaser2").get(0));

  }

}
