package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class FilterDuplicateValuesTest {

  StageFactory factory = StageFactory.of(FilterDuplicateValues.class);

  @Test
  public void testMultivaluedStrings() throws StageException {
    Stage stage = factory.get("FilterDuplicateValues/config.conf");

    Document doc = new Document("doc");
    doc.setField("foo", "bar");
    doc.addToField("foo", "cat");
    doc.addToField("foo", "dog");
    doc.addToField("foo", "cat");

    stage.processDocument(doc);

    assertEquals(3, doc.getStringList("foo").size());

    List<String> values = doc.getStringList("foo");
    assertEquals("bar", values.get(0));
    assertEquals("cat", values.get(1));
    assertEquals("dog", values.get(2));
  }

  @Test
  public void testMultivaluedNumbers() throws StageException {
    Stage stage = factory.get("FilterDuplicateValues/config.conf");

    Document doc = new Document("doc");
    doc.setField("foo", 1);
    doc.addToField("foo", 1);
    doc.addToField("foo", 23);
    doc.addToField("foo", 14);
    doc.addToField("foo", -32);

    stage.processDocument(doc);

    assertEquals(4, doc.getStringList("foo").size());

    List<String> values = doc.getStringList("foo");
    assertEquals("1", values.get(0));
    assertEquals("23", values.get(1));
    assertEquals("14", values.get(2));
    assertEquals("-32", values.get(3));
  }
}
