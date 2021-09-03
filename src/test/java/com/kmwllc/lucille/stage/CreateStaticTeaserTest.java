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

    // Ensure that if the field value is shorter than max length, the entire value is piped into the destination field
    Document doc = new Document("doc");
    String inStr = "Smaller than the char limit";
    doc.addToField("input1", inStr);
    stage.processDocument(doc);
    assertEquals(inStr, doc.getStringList("teaser1").get(0));

    // Ensure that the teaser will be no longer than the max length, but will not break up words.
    Document doc2 = new Document("doc2");
    doc2.addToField("input1", "Here is a teaser that is longer than the char limit. The extraction will be shorter than max length.");
    stage.processDocument(doc2);
    assertEquals("Here is a teaser that is longer than the char", doc2.getStringList("teaser1").get(0));

    // Ensure that multiple teasers can be created in one pass
    Document doc3 = new Document("doc3");
    doc3.addToField("input1", "Here is a teaser that is longer than the char limit. The extraction will be shorter than max length.");
    doc3.addToField("input2", "This teaser will get cut off in the middle of a sentence, but not in the middle of a word?");
    stage.processDocument(doc3);
    assertEquals("Here is a teaser that is longer than the char", doc3.getStringList("teaser1").get(0));
    assertEquals("This teaser will get cut off in the middle of a", doc3.getStringList("teaser2").get(0));

    // Ensure that Strings with no word breaks will be truncated to the max length.
    Document doc4 = new Document("doc4");
    doc4.addToField("input1", "thisisonelongcontinuousstreamofcharacterssincenodelimitersarefoundthestringwillbetruncatedafter50chars");
    stage.processDocument(doc4);
    assertEquals("thisisonelongcontinuousstreamofcharacterssincenode", doc4.getStringList("teaser1").get(0));
    assertEquals(50, "thisisonelongcontinuousstreamofcharacterssincenode".length());

  }

}
