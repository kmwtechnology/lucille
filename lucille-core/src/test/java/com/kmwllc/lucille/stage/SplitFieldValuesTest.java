package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertThrows;
import java.util.List;
import java.util.Map;
import static org.junit.Assert.assertFalse;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;

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
  public void testMultiValuedField() throws Exception {
    Stage stage = factory.get("SplitFieldValuesTest/config.conf");

    Document doc = Document.create("doc");
    doc.setField("data", "this,that, the ,other");
    doc.addToField("data", "value2");
    doc.addToField("data", "value3, value4");
    stage.processDocument(doc);
    
    assertEquals(7, doc.getStringList("data").size());
    assertEquals("this", doc.getStringList("data").get(0));
    assertEquals("that", doc.getStringList("data").get(1));
    assertEquals("the", doc.getStringList("data").get(2));
    assertEquals("other", doc.getStringList("data").get(3));
    assertEquals("value2", doc.getStringList("data").get(4));
    assertEquals("value3", doc.getStringList("data").get(5));
    assertEquals("value4", doc.getStringList("data").get(6));

  }

  
  @Test
  public void testSpec() throws StageException {
    Stage stage = factory.get("SplitFieldValuesTest/config.conf");
    assertEquals(
        Set.of("source", "delimiter", "trimWhitespace", "dest"),
        stage.getNonDefaultLegalProperties());
  }

  @Test
  public void testNullValues() throws Exception {
    Stage stage = factory.get(Map.of("source", "f", "dest", "o", "delimiter", " ", "trimWhitespace", true));

    Document doc = Document.createFromJson("{\"id\":\"1\",\"f\":null}");
    stage.processDocument(doc);
    assertFalse(doc.has("o"));

    Document mixed = Document.createFromJson("{\"id\":\"1\",\"f\":[null,\"Hi There\"]}");
    stage.processDocument(mixed);
    assertEquals(List.of("Hi", "There"), mixed.getStringList("o"));
  }

  @Test
  public void testDelimiterIsRegex() throws Exception {
    Stage stage = factory.get(Map.of("source", "a", "dest", "b", "delimiter", "\\|", "trimWhitespace", true));
    Document doc = Document.create("doc");
    doc.setField("a", "x|y");
    stage.processDocument(doc);
    assertEquals(List.of("x", "y"), doc.getStringList("b"));

    assertThrows(StageException.class,
        () -> factory.get(Map.of("source", "a", "dest", "b", "delimiter", "(", "trimWhitespace", true)));
  }
}
