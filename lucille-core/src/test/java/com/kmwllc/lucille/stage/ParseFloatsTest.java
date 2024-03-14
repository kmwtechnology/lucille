package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import java.util.List;
import org.junit.Test;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;

public class ParseFloatsTest {

  private StageFactory factory = StageFactory.of(ParseFloats.class);

  @Test
  public void testMalformedConfig() {
    assertThrows(StageException.class, () -> factory.get("ParseFloatsTest/bad.conf"));
  }

  @Test
  public void testProcessDocument() throws StageException {
    Stage stage = factory.get("ParseFloatsTest/basic.conf");
    Stage stageDest = factory.get("ParseFloatsTest/dest.conf");

    Document bad = Document.create("doc1");
    Document empty = Document.create("doc2");
    Document simple = Document.create("doc3");
    Document noField = Document.create("doc4");
    Document floats = Document.create("doc5");
    Document badList = Document.create("doc6");
    Document dest = Document.create("doc7");

    bad.setOrAdd("source", "not a float");
    empty.setOrAdd("source", "[]");
    simple.setOrAdd("source", "[1, 2, 3]");
    floats.setOrAdd("source", "[1.2, 2.5, 3.1]");
    badList.setOrAdd("source", "[1, \"a\", 3]");
    dest.setOrAdd("source", "[1, 2, 3]");

    stage.processDocument(bad);
    stage.processDocument(empty);
    stage.processDocument(simple);
    stage.processDocument(noField);
    stage.processDocument(floats);
    stage.processDocument(badList);
    stageDest.processDocument(dest);

    assertEquals(2, bad.getFieldNames().size());
    assertEquals("not a float", bad.getString("source"));
    assertEquals("doc1", bad.getId());

    assertEquals(1, empty.getFieldNames().size());
    assertEquals("doc2", empty.getId());

    assertEquals(2, simple.getFieldNames().size());
    assertEquals(List.of(1f, 2f, 3f), simple.getFloatList("source"));
    assertEquals("doc3", simple.getId());

    assertEquals(1, noField.getFieldNames().size());
    assertEquals("doc4", noField.getId());

    assertEquals(2, floats.getFieldNames().size());
    assertEquals(List.of(1.2f, 2.5f, 3.1f), floats.getFloatList("source"));
    assertEquals("doc5", floats.getId());

    assertEquals(2, badList.getFieldNames().size());
    assertEquals("[1, \"a\", 3]", badList.getString("source"));
    assertEquals("doc6", badList.getId());

    assertEquals(3, dest.getFieldNames().size());
    assertEquals(List.of(1f, 2f, 3f), dest.getFloatList("dest"));
    assertEquals("[1, 2, 3]", dest.getString("source"));
    assertEquals("doc7", dest.getId());
  }
}
