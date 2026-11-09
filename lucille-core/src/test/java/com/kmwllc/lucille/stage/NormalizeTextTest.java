package com.kmwllc.lucille.stage;

import java.util.List;
import java.util.Map;
import static org.junit.Assert.assertFalse;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;

public class NormalizeTextTest {

  private StageFactory factory = StageFactory.of(NormalizeText.class);

  // TODO : Change this to use multiple configs
  @Test
  public void testLowercase() throws StageException {
    Stage stage = factory.get("NormalizeTextTest/lowercase.conf");
    Document doc = Document.create("doc");
    doc.setField("input1", "ALL tHiS Is lowerCASED");
    stage.processDocument(doc);
    assertEquals("all this is lowercased", doc.getStringList("output1").get(0));
  }

  @Test
  public void testUppercase() throws Exception {
    Stage stage = factory.get("NormalizeTextTest/uppercase.conf");
    Document doc2 = Document.create("doc2");
    doc2.setField("input1", "this WILL Be UppERCased");
    stage.processDocument(doc2);
    assertEquals("THIS WILL BE UPPERCASED", doc2.getStringList("output1").get(0));
  }

  @Test
  public void testSentenceCase() throws Exception {
    Stage stage = factory.get("NormalizeTextTest/sentencecase.conf");
    Document doc4 = Document.create("doc4");
    doc4.setField("input2", "this is a sentence. and this! this too? test");
    stage.processDocument(doc4);
    assertEquals("This is a sentence. And this! This too? Test", doc4.getStringList("output2").get(0));
  }

  @Test
  public void testTitleCase() throws Exception {
    Stage stage = factory.get("NormalizeTextTest/titlecase.conf");
    Document doc3 = Document.create("doc3");
    doc3.setField("input3", "this will be in title case");
    stage.processDocument(doc3);
    assertEquals("This Will Be In Title Case", doc3.getStringList("output3").get(0));
  }

  @Test
  public void testSpec() throws StageException {
    Stage stage = factory.get("NormalizeTextTest/lowercase.conf");
    assertEquals(Set.of("mode", "updateMode", "source", "dest"), stage.getNonDefaultLegalProperties());
  }

  @Test
  public void testNullAndEmptyValues() throws Exception {
    Stage stage = factory.get(Map.of("source", List.of("f"), "dest", List.of("o"), "mode", "sentence_case"));

    Document doc = Document.createFromJson("{\"id\":\"1\",\"f\":null}");
    stage.processDocument(doc);
    assertFalse(doc.has("o"));

    Document mixed = Document.createFromJson("{\"id\":\"1\",\"f\":[null,\"Hi There\"]}");
    stage.processDocument(mixed);
    assertEquals("Hi there", mixed.getString("o"));

    Document empty = Document.create("empty");
    empty.setField("f", "");
    stage.processDocument(empty);
    assertEquals("", empty.getString("o"));
  }
}
