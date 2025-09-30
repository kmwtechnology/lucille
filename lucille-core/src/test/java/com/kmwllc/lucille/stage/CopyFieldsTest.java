package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CopyFieldsTest {

  private final StageFactory factory = StageFactory.of(CopyFields.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testCopyFieldsReplace() throws Exception {
    Stage stage = factory.get("CopyFieldsTest/replace.conf");

    // Ensure that one field is correctly copied over
    Document doc = Document.create("doc");
    String inputVal = "This will be copied to output1";
    doc.setField("input1", inputVal);
    stage.processDocument(doc);
    assertEquals("Value from input1 should be copied to output1", inputVal, doc.getStringList("output1").get(0));

    // Ensure that field 2 in the source list is copied to output2
    Document doc2 = Document.create("doc2");
    inputVal = "This will be copied to output2";
    doc2.setField("input2", inputVal);
    doc2.setField("output2", "here's some junk data.");
    stage.processDocument(doc2);
    assertEquals("Value from input2 should be copied to output2", inputVal, doc2.getStringList("output2").get(0));
    assertEquals("Value from input2 should be copied to alsoOutput2", inputVal, doc2.getStringList("alsoOutput2").get(0));

    // Ensure that several fields can be copied at the same time.
    Document doc3 = Document.create("doc3");
    String inputVal1 = "This will be copied to output1";
    String inputVal2 = "This will be copied to output2";
    String inputVal3 = "This will be copied to output3";
    doc3.setField("input1", inputVal1);
    doc3.setField("input2", inputVal2);
    doc3.setField("input3", inputVal3);
    stage.processDocument(doc3);
    assertEquals("Value from input1 should be copied to output1", inputVal1, doc3.getStringList("output1").get(0));
    assertEquals("Value from input2 should be copied to output2", inputVal2, doc3.getStringList("output2").get(0));
    assertEquals("Value from input2 should be copied to alsoOutput2", inputVal2, doc3.getStringList("alsoOutput2").get(0));
    assertEquals("Value from input3 should be copied to output3", inputVal3, doc3.getStringList("output3").get(0));

    // Ensure that if isNested is false, dotted fields will be treated as literals
    Document doc4 = Document.create("doc4");
    doc4.setField("input4.nested", "This will be copied to output4.nested (literal field)");
    stage.processDocument(doc4);
    assertEquals("This will be copied to output4.nested (literal field)", doc4.getString("output4.nested"));
    assertNull(doc4.getJson("output4"));
  }

  @Test
  public void testCopyFieldsSkip() throws Exception {
    Stage stage = factory.get("CopyFieldsTest/skip.conf");

    Document doc = Document.create("doc");
    doc.setField("input1", "Here is some input.");
    doc.setField("input2", "Here is another input.");
    doc.setField("input3", "This will be skipped along with input 1.");
    doc.setField("output1", "input1 should be skipped.");
    doc.setField("output3", "input3 should be skipped.");
    doc.setField("output5", "input3 should be skipped.");
    stage.processDocument(doc);
    assertEquals("input1 should be skipped.", doc.getStringList("output1").get(0));
    assertEquals("Here is another input.", doc.getStringList("output2").get(0));
    assertEquals("input3 should be skipped.", doc.getStringList("output3").get(0));
    assertEquals("Here is another input.", doc.getStringList("output4").get(0));
    assertEquals("input3 should be skipped.", doc.getStringList("output5").get(0));
  }

  @Test
  public void testCopyFieldsNested() throws Exception {
    Stage stage = factory.get("CopyFieldsTest/nestedJson.conf");

    // Ensure that a nested field is correctly copied over to another nested field
    Document doc = Document.create("doc");
    JsonNode inputNode = mapper.createObjectNode().put("nested", "This will be copied to output1");
    doc.setField("input1", inputNode);
    doc.setField("output1", mapper.createObjectNode().set("also",
        mapper.createObjectNode().put("nested", "Current output1 value.")));
    stage.processDocument(doc);
    assertEquals("Value from input1.nested should be copied to output1.also.nested", inputNode, doc.getJson("output1").get("also"));

    // Ensure that a non-nested field is correctly copied over to a nested field
    Document doc2 = Document.create("doc2");
    String inputVal = "This will be copied to output2";
    doc2.setField("input2", inputVal);
    doc2.setField("output2",  mapper.createObjectNode().set("nested",
        mapper.createObjectNode().put("moreNested", "here's some junk data.")));
    stage.processDocument(doc2);
    assertEquals("Value from input2 should be copied to output2.nested", inputVal, doc2.getJson("output2").get("nested").textValue());

    // Ensure that several fields can be copied at the same time and that different types can be copied over
    Document doc3 = Document.create("doc3");
    int inputVal2 = 123;
    boolean inputVal3 = true;
    doc3.setField("input2", inputVal2);
    doc3.setField("input3", inputVal3);
    stage.processDocument(doc3);
    assertNull(doc3.getJson("output1"));
    assertEquals("Value from input2 should be copied to output2.nested",
        mapper.createObjectNode().put("nested",  inputVal2),
        doc3.getJson("output2"));
    assertEquals("Value from input3 should be copied to output3", inputVal3, doc3.getJson("output3").asBoolean());

    // Ensure that nested fields mapped to list are copied
    Document doc4 = Document.create("doc4");
    doc4.setField("input4", mapper.createObjectNode().put("nested","This will be copied to two places under output4"));
    stage.processDocument(doc4);
    assertEquals("Value from input4 should be copied to output4.nested.one",
        "This will be copied to two places under output4",
        doc4.getJson("output4").get("nested").get("one").textValue());
    assertEquals("Value from input4 should be copied to output4.nested.two",
        "This will be copied to two places under output4",
        doc4.getJson("output4").get("nested").get("two").textValue());

    // Ensure that if the source field doesn't exist, nothing happens
    Document doc5 = Document.create("doc5");
    stage.processDocument(doc5);
    assertNull(doc5.getJson("output5"));
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("CopyFieldsTest/replace.conf");
    assertEquals(
        Set.of("class", "conditionPolicy", "conditions", "fieldMapping", "isNested", "name", "update_mode"),
        stage.getLegalProperties());
  }
}
