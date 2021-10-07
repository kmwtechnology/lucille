package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class RemoveDuplicateValuesTest {

  StageFactory factory = StageFactory.of(RemoveDuplicateValues.class);

  @Test
  public void testRemoveDuplicateValues() throws StageException {
    Stage stage = factory.get("RemoveDuplicateValuesTest/config.conf");

    Document doc1 = new Document("doc1");
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

    Document doc2 = new Document("doc2");
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
  }

  @Test(expected = StageException.class)
  public void testNoFields() throws StageException {
    Stage stage = factory.get("RemoveDuplicateValuesTest/nofields.conf");

    Document doc = new Document("doc");
    stage.processDocument(doc);
  }

}
