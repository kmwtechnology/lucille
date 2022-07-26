package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class RemoveDuplicateValuesTest {

  StageFactory factory = StageFactory.of(RemoveDuplicateValues.class);

  @Test
  public void testRemoveDuplicateValues() throws StageException {
    Stage stage = factory.get("RemoveDuplicateValuesTest/config.conf");

    JsonDocument doc1 = new JsonDocument("doc1");
    doc1.setField("field1", "regular");
    doc1.addToField("field1", "different");
    doc1.addToField("field1", "regular");
    doc1.addToField("field1", "ahem");
    doc1.addToField("field1", "regular");
    stage.processDocument(doc1);
    assertEquals(3, doc1.getStringList("field1").size());
    assertEquals("regular", doc1.getStringList("field1").get(0));
    assertEquals("different", doc1.getStringList("field1").get(1));
    assertEquals("ahem", doc1.getStringList("field1").get(2));

    JsonDocument doc2 = new JsonDocument("doc2");
    doc2.setField("field1", "same");
    doc2.addToField("field1", "different");
    doc2.addToField("field1", "same");
    doc2.addToField("field1", "different");
    doc2.setField("field2", "1");
    doc2.addToField("field2", "2");
    doc2.addToField("field2", "2");
    doc2.addToField("field2", "2");
    doc2.addToField("field2", "3");
    doc2.addToField("field2", "1");
    doc2.setField("field3", "duplicates");
    doc2.addToField("field3", "allowed");
    doc2.addToField("field3", "allowed");
    stage.processDocument(doc2);
    assertEquals(Arrays.asList("same", "different"), doc2.getStringList("field1"));
    assertEquals(Arrays.asList("1", "2", "3"), doc2.getStringList("field2"));
    assertEquals(Arrays.asList("duplicates", "allowed", "allowed"), doc2.getStringList("field3"));

    JsonDocument doc3 = new JsonDocument("doc3");
    doc3.setField("field1", "single");
    stage.processDocument(doc3);
    assertEquals("single", doc3.getString("field1"));
    assertFalse(doc3.has("field2"));
    assertFalse(doc3.has("field3"));
  }

  @Test(expected = StageException.class)
  public void testNoFields() throws StageException {
    Stage stage = factory.get("RemoveDuplicateValuesTest/nofields.conf");

    JsonDocument doc = new JsonDocument("doc");
    stage.processDocument(doc);
  }

  @Test
  public void testMultivaluedStrings() throws StageException {
    Stage stage = factory.get("RemoveDuplicateValuesTest/config.conf");

    JsonDocument doc = new JsonDocument("doc");
    doc.setField("field1", "bar");
    doc.addToField("field1", "cat");
    doc.addToField("field1", "dog");
    doc.addToField("field1", "cat");

    JsonDocument doc2 = new JsonDocument("doc2");
    doc2.setField("field1", "bar");
    doc2.addToField("field1", "cat");
    doc2.addToField("field1", "dog");
    doc2.addToField("field1", "cat");

    stage.processDocument(doc);

    assertEquals(3, doc.getStringList("field1").size());
    assertEquals(4, doc2.getStringList("field1").size());

    List<String> values = doc.getStringList("field1");
    assertEquals("bar", values.get(0));
    assertEquals("cat", values.get(1));
    assertEquals("dog", values.get(2));
  }

  @Test
  public void testMultivaluedNumbers() throws StageException {
    Stage stage = factory.get("RemoveDuplicateValuesTest/config.conf");

    JsonDocument doc = new JsonDocument("doc");
    doc.setField("field1", 1);
    doc.addToField("field1", 1);
    doc.addToField("field1", 23);
    doc.addToField("field1", 14);
    doc.addToField("field1", -32);

    stage.processDocument(doc);

    assertEquals(4, doc.getStringList("field1").size());

    List<String> values = doc.getStringList("field1");
    assertEquals("1", values.get(0));
    assertEquals("23", values.get(1));
    assertEquals("14", values.get(2));
    assertEquals("-32", values.get(3));

    // ensure that the numbers do not come out as Strings
    assertEquals("{\"id\":\"doc\",\"field1\":[1,23,14,-32]}", doc.toString());
  }

  @Test
  public void testMappingToField() throws StageException {

    Stage stage = factory.get("RemoveDuplicateValuesTest/mapping.conf");

    JsonDocument doc = new JsonDocument("doc");
    doc.setField("field1", 1);
    doc.addToField("field1", 1);
    doc.addToField("field1", 23);
    doc.addToField("field1", 14);
    doc.addToField("field1", -32);

    stage.processDocument(doc);

    assertEquals(4, doc.getStringList("fish").size());

    List<String> values = doc.getStringList("fish");
    assertEquals("1", values.get(0));
    assertEquals("23", values.get(1));
    assertEquals("14", values.get(2));
    assertEquals("-32", values.get(3));

    // verify original field stays, while new values without duplicates are placed into new field
    assertEquals("{\"id\":\"doc\",\"field1\":[1,1,23,14,-32],\"fish\":[1,23,14,-32]}", doc.toString());
  }
}
