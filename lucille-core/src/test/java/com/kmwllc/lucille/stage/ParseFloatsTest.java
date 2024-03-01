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

    Document bad = Document.create("doc1");
    Document empty = Document.create("doc2");
    Document simple = Document.create("doc3");
    Document noField = Document.create("doc4");

    bad.setOrAdd("source", "not a float");
    empty.setOrAdd("source", "[]");
    simple.setOrAdd("source", "[1, 2, 3]");

    stage.processDocument(bad);
    stage.processDocument(empty);
    stage.processDocument(simple);
    stage.processDocument(noField);

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
  }
}
