package com.kmwllc.lucille.stage;

import java.util.List;
import java.util.Map;
import static org.junit.Assert.assertFalse;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;

public class CreateStaticTeaserTest {

  private StageFactory factory = StageFactory.of(CreateStaticTeaser.class);

  @Test
  public void testCreateStaticTeaser() throws Exception {
    Stage stage = factory.get("CreateStaticTeaserTest/config.conf");

    // Ensure that if the field value is shorter than max length, the entire value is piped into the destination field
    Document doc = Document.create("doc");
    String inStr = "Smaller than the char limit";
    doc.addToField("input1", inStr);
    stage.processDocument(doc);
    assertEquals(inStr, doc.getStringList("teaser1").get(0));

    // Ensure that the teaser will be no longer than the max length, but will not break up words.
    Document doc2 = Document.create("doc2");
    doc2.addToField("input1",
        "Here is a teaser that is longer than the char limit. The extraction will be shorter than max length.");
    stage.processDocument(doc2);
    assertEquals("Here is a teaser that is longer than the char", doc2.getStringList("teaser1").get(0));

    // Ensure that multiple teasers can be created in one pass
    Document doc3 = Document.create("doc3");
    doc3.addToField("input1",
        "Here is a teaser that is longer than the char limit. The extraction will be shorter than max length.");
    doc3.addToField("input2", "This teaser will get cut off in the middle of a sentence, but not in the middle of a word?");
    stage.processDocument(doc3);
    assertEquals("Here is a teaser that is longer than the char", doc3.getStringList("teaser1").get(0));
    assertEquals("This teaser will get cut off in the middle of a", doc3.getStringList("teaser2").get(0));

    // Ensure that Strings with no word breaks will be truncated to the max length.
    Document doc4 = Document.create("doc4");
    doc4.addToField("input1",
        "thisisonelongcontinuousstreamofcharacterssincenodelimitersarefoundthestringwillbetruncatedafter50chars");
    stage.processDocument(doc4);
    assertEquals("thisisonelongcontinuousstreamofcharacterssincenode", doc4.getStringList("teaser1").get(0));
    assertEquals(50, "thisisonelongcontinuousstreamofcharacterssincenode".length());

  }

  @Test
  public void testSpec() throws StageException {
    Stage stage = factory.get("CreateStaticTeaserTest/config.conf");
    assertEquals(
        Set.of("updateMode", "source", "dest", "maxLength"),
        stage.getNonDefaultLegalProperties());
  }

  @Test
  public void testNullValues() throws Exception {
    Stage stage = factory.get(Map.of("source", List.of("f"), "dest", List.of("o"), "maxLength", 50));

    Document doc = Document.createFromJson("{\"id\":\"1\",\"f\":null}");
    stage.processDocument(doc);
    assertFalse(doc.has("o"));

    Document mixed = Document.createFromJson("{\"id\":\"1\",\"f\":[null,\"Hi There\"]}");
    stage.processDocument(mixed);
    assertFalse(mixed.has("o"));
  }

  @Test
  public void testWordEndingAtLimitIsKept() throws Exception {
    Stage stage = factory.get(Map.of("source", List.of("in"), "dest", List.of("out"), "maxLength", 11));
    Document doc = Document.create("doc");
    doc.setField("in", "Hello world");
    stage.processDocument(doc);
    assertEquals("Hello world", doc.getString("out"));

    Document longer = Document.create("longer");
    longer.setField("in", "Hello world again");
    stage.processDocument(longer);
    assertEquals("Hello world", longer.getString("out"));
  }
}
