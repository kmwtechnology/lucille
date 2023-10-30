package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TrimWhitespaceTest {

  StageFactory factory = StageFactory.of(TrimWhitespace.class);

  @Test
  public void trimWhitespaceTest() throws StageException {
    Stage stage = factory.get("TrimWhitespaceTest/config.conf");

    Document doc1 = Document.create("doc1");
    doc1.setField("input1", "   trim this   ");
    doc1.setField("input2", "   test");
    doc1.addToField("input2", "test   ");
    doc1.setField("test_field", "   this should not be trimmed  ");
    stage.processDocument(doc1);
    assertEquals("trim this", doc1.getString("input1"));
    assertEquals("test", doc1.getStringList("input2").get(0));
    assertEquals("test", doc1.getStringList("input2").get(1));
    assertEquals("   this should not be trimmed  ", doc1.getString("test_field"));
    assertFalse(doc1.has("input3"));

    Document doc2 = Document.create("doc2");
    doc2.setField("input1", "nothing to trim");
    doc2.addToField("input1", " trim this ");
    doc2.addToField("input1", "     test");
    doc2.addToField("input1", "test    ");
    stage.processDocument(doc2);
    assertEquals("nothing to trim", doc2.getStringList("input1").get(0));
    assertEquals("trim this", doc2.getStringList("input1").get(1));
    assertEquals("test", doc2.getStringList("input1").get(2));
    assertEquals("test", doc2.getStringList("input1").get(3));
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("TrimWhitespaceTest/config.conf");
    assertEquals(Set.of("name", "fields", "conditions", "class"), stage.getLegalProperties());
  }
}
