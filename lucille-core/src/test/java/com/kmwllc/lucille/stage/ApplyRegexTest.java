package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ApplyRegexTest {

  private StageFactory factory = StageFactory.of(ApplyRegex.class);

  @Test
  public void testApplyRegex() throws Exception {
    Stage stage = factory.get("ApplyRegexTest/config.conf");

    // Test extracting a single pattern match
    Document doc = Document.create("doc");
    doc.setField("input1", "here is a number 12");
    stage.processDocument(doc);
    assertEquals("Regex pattern should extract numbers from the input", "12",
        doc.getStringList("output1").get(0));

    // Test extracting several pattern matches
    Document doc2 = Document.create("doc2");
    doc2.setField("input1", "here are some numbers: 1, 2, 3, 4, 5");
    stage.processDocument(doc2);
    List<String> tokens = doc2.getStringList("output1");
    for (int i = 1; i <= 5; i++) {
      assertEquals("Regex should extract all the numbers in order", String.valueOf(i), tokens.get(i - 1));
    }

    // Test inputting/outputting to the third field in the list
    Document doc3 = Document.create("doc3");
    doc3.setField("input3", "this is field #3");
    stage.processDocument(doc3);
    assertEquals("Field output3 should contain the extracted values from input3",
        doc3.getStringList("output3").get(0), "3");

    // Test extracting from several source fields to several destination fields
    Document doc4 = Document.create("doc4");
    doc4.setField("input1", "this is field input 1");
    doc4.setField("input2", "this is field input 2");
    doc4.setField("input3", "this is field input 3");
    stage.processDocument(doc4);
    assertEquals("output1 should contain values from input", "1", doc4.getStringList("output1").get(0));
    assertEquals("output2 should contain values from input2", "2", doc4.getStringList("output2").get(0));
    assertEquals("output3 should contain values from input3", "3", doc4.getStringList("output3").get(0));
  }

  @Test
  public void testCapturingGroup() throws Exception {
    Stage stage = factory.get("ApplyRegexTest/capturing.conf");

    Document doc = Document.create("1");
    doc.setField("input", "test~123");
    stage.processDocument(doc);
    assertEquals("123", doc.getString("output"));
  }

  @Test
  public void testIgnoreCase() throws Exception {
    Stage stage = factory.get("ApplyRegexTest/ignoreCase.conf");

    Document doc1 = Document.create("1");
    doc1.setField("input", "Hi abcdef");

    Document doc2 = Document.create("1");
    doc2.setField("input", "Hi AbCDeF");

    stage.processDocument(doc1);
    stage.processDocument(doc2);

    assertEquals("abcdef", doc1.getString("output"));
    assertEquals("AbCDeF", doc2.getString("output"));
  }

  @Test
  public void testMultiline() throws Exception {
    Stage stage = factory.get("ApplyRegexTest/multiline.conf");

    Document doc1 = Document.create("1");
    doc1.setField("input", "line");

    Document doc2 = Document.create("1");
    doc2.setField("input", "hello there\n\nline");

    stage.processDocument(doc1);
    stage.processDocument(doc2);

    assertEquals("line", doc1.getString("output"));
    assertEquals("line", doc2.getString("output"));
  }

  @Test
  public void testDotall() throws Exception {
    Stage stage = factory.get("ApplyRegexTest/dotall.conf");

    Document doc1 = Document.create("1");
    doc1.setField("input", "abc");

    Document doc2 = Document.create("1");
    doc2.setField("input", "a\nc");

    stage.processDocument(doc1);
    stage.processDocument(doc2);

    assertEquals("abc", doc1.getString("output"));
    assertEquals("a\nc", doc2.getString("output"));
  }

  @Test
  public void testLiteral() throws Exception {
    Stage stage = factory.get("ApplyRegexTest/literal.conf");

    Document doc1 = Document.create("1");
    doc1.setField("input", "abc");

    Document doc2 = Document.create("1");
    doc2.setField("input", "a.c");

    stage.processDocument(doc1);
    stage.processDocument(doc2);

    assertNull(doc1.getString("output"));
    assertEquals("a.c", doc2.getString("output"));
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("ApplyRegexTest/config.conf");
    assertEquals(
        Set.of(
            "ignore_case",
            "regex",
            "update_mode",
            "multiline",
            "name",
            "source",
            "dest",
            "conditions",
            "class",
            "dotall",
            "literal",
            "conditionPolicy"),
        stage.getLegalProperties());
  }
}
