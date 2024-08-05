package com.kmwllc.lucille.stage;



import com.fasterxml.jackson.databind.JsonNode;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.jupiter.api.Test;

class ChunkTest {

  StageFactory factory = StageFactory.of(Chunk.class);

  @Test
  public void testFixedChunkMethod() throws StageException {
    // create chunk Stage
    Stage stage = factory.get("ChunkTest/fixedChunk.conf");
    // create and set document
    Document doc = Document.create("id");
    doc.setField("text", "this is 13 tokens broken up by a 6 token limit");
    // process document
    stage.processDocument(doc);
    // check document chunked
    int index = 0;
    List<String> list = Arrays.asList("this is 13 tokens broken up","by a 6 token limit");

    JsonNode jnode = doc.getJson("chunks");

    Iterator<JsonNode> elements = jnode.elements();
    while (elements.hasNext()) {
      JsonNode element = elements.next();
      Assert.assertEquals(list.get(index), element.asText());
      index++;
    }

    assertFalse(elements.hasNext());
  }


  @Test
  public void testSeparatorMethod() throws StageException {
    // create chunk Stage
    Stage stage = factory.get("ChunkTest/separatorChunk.conf");
    // create and set document
    Document doc = Document.create("id");
    doc.setField("text", "Lorem ipsum odor amet, consectetuer adipiscing elit."
        + "Sollicitudin libero ornare lorem in scelerisque, rhoncus malesuada nec suscipit."
        + "Ajusto eros iaculis blandit aliquet sit taciti. Quis volutpat dignissim ipsum vel, sodales"
        + "interdum sollicitudin ligula. Varius nam a pharetra maecenas nulla justo. Class lorem nascetur,"
        + "ligula consectetur blandit tincidunt ipsum lectus. Lacus torquent metus ante class placerat quisque."
        + "Gravida ex, vestibulum habitasse; ligula ornare maecenas.\n");
    // process document
    stage.processDocument(doc);
    // check document chunked

    int index = 0;
    List<String> list = Arrays.asList("Lorem ipsum odor amet",
        "consectetuer adipiscing elit.Sollicitudin libero ornare lorem in scelerisque",
        "rhoncus malesuada nec suscipit.Ajusto eros iaculis blandit aliquet sit taciti. Quis volutpat dignissim ipsum vel",
        "sodalesinterdum sollicitudin ligula. Varius nam a pharetra maecenas nulla justo. Class lorem nascetur",
        "ligula consectetur blandit tincidunt ipsum lectus. Lacus torquent metus ante class placerat quisque.Gravida ex",
        "vestibulum habitasse; ligula ornare maecenas.");


    JsonNode jnode = doc.getJson("chunks");
    Iterator<JsonNode> elements = jnode.elements();
    while (elements.hasNext()) {
      JsonNode element = elements.next();
      Assert.assertEquals(list.get(index), element.asText());
      index++;
    }

    assertFalse(elements.hasNext());

  }

  @Test
  public void testParagraphMethod() throws StageException {
    // create chunk Stage
    Stage stage = factory.get("ChunkTest/paragraph.conf");
    // create and set document
    Document doc = Document.create("id");
    String paragraph1 = "This is the first paragraph. It contains some text and spans multiple lines.\n\n";
    String paragraph2 = "This is the second paragraph. It also contains text and is separated by a blank line.\n\n";
    String paragraph3 = "Here is the third paragraph. This approach allows you to write as much text as needed.";

    doc.setField("text", paragraph1 + paragraph2 + paragraph3);
    // process document
    stage.processDocument(doc);
    // check document chunked

    // expected value
    List<String> list = Arrays.asList("This is the first paragraph. It contains some text and spans multiple lines.",
                                      "This is the second paragraph. It also contains text and is separated by a blank line.",
                                      paragraph3);

    int index = 0;

    JsonNode jnode = doc.getJson("chunks");
    Iterator<JsonNode> elements = jnode.elements();
    while (elements.hasNext()) {
      JsonNode element = elements.next();
      Assert.assertEquals(list.get(index), element.asText());
      index++;
    }

    assertFalse(elements.hasNext());
  }

  @Test
  public void testSplitLongSentence() throws StageException {
    // create chunk Stage
    Stage stage = factory.get("ChunkTest/longSentence.conf");
    // create and set document
    Document doc = Document.create("id");

    doc.setField("text", "This is a really long sentence that exceeds token limit of 10");
    stage.processDocument(doc);

    JsonNode jnode = doc.getJson("chunks");

    // expected chunks
    List<String> list = Arrays.asList("This is a really long sentence that exceeds",
        "token limit of 10");
    int index = 0;

    Iterator<JsonNode> elements = jnode.elements();
    while (elements.hasNext()) {
      JsonNode element = elements.next();
      Assert.assertEquals(list.get(index), element.asText());
      index++;
    }

    assertFalse(elements.hasNext());
  }

  @Test
  public void testSplitLongWord() throws StageException {
    // create chunk Stage
    Stage stage = factory.get("ChunkTest/longWord.conf");
    // create and set document
    Document doc = Document.create("id");

    doc.setField("text", "supercalifragilisticexpialidocious");

    List<String> list = Arrays.asList("su-","pe-","rc-","al-","if-","ra-","gi-","li-","st-","ic-","ex-","pi-","al-","id-",
                                      "oc-","io-","us");
    // process document
    stage.processDocument(doc);
    // check document chunked
    int index = 0;
    JsonNode jnode = doc.getJson("chunks");
    Iterator<JsonNode> elements = jnode.elements();

    while (elements.hasNext()) {
      JsonNode element = elements.next();
      assertEquals(list.get(index), element.asText());
      index++;
    }
  }

}