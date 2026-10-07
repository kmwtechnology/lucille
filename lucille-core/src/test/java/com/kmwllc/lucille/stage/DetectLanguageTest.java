package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class DetectLanguageTest {

  private StageFactory factory = StageFactory.of(DetectLanguage.class);

  @Test
  public void testDetectLanguage() throws Exception {
    Stage stage = factory.get("DetectLanguageTest/config.conf");

    // Ensure that the correct language is detected for field pair 1.
    Document doc = Document.create("doc");
    doc.setField("input1", "This is a sentence in English!");
    stage.processDocument(doc);
    assertEquals("en", doc.getStringList("language").get(0));
    assertEquals("0.99", doc.getString("lang_conf"));

    // Ensure that the correct language is detected for field pair 3.
    Document doc2 = Document.create("doc2");
    doc2.setField("input3", "Eso oracion esta en espanol. Ojala que podimos verla.");
    stage.processDocument(doc2);
    assertEquals("es", doc2.getStringList("language").get(0));
    assertEquals("0.99", doc2.getString("lang_conf"));

    // Ensure multiple languages can be extracted in one pass
    Document doc3 = Document.create("doc3");
    Document doc4 = Document.create("doc4");
    Document doc5 = Document.create("doc5");
    doc3.setField("input1", "Это стандартное предложение на моем языке.");
    doc4.setField("input1", "यह मेरी पसंद की भाषा में एक मानक वाक्य है।");
    doc5.setField("input1", "这是我选择的语言的标准句子。");
    stage.processDocument(doc3);
    stage.processDocument(doc4);
    stage.processDocument(doc5);
    assertEquals("ru", doc3.getStringList("language").get(0));
    assertEquals("hi", doc4.getStringList("language").get(0));
    assertEquals("zh-cn", doc5.getStringList("language").get(0));
  }

  @Test
  public void testMaxLengthAcrossValuesAndFields() throws Exception {
    Stage stage = factory.get(Map.of(
        "source", List.of("input1", "input2"),
        "languageField", "language",
        "minLength", 0,
        "maxLength", 60,
        "minProbability", 0.85));

    // only the first maxLength chars of the concatenated values are used, so the trailing Spanish text is ignored
    String english = "This is a sentence in English, and it keeps on going for a while.";
    String spanish = "Eso oracion esta en espanol. Ojala que podimos verla. Eso oracion esta en espanol. Ojala que podimos verla.";
    Document doc = Document.create("doc");
    doc.setField("input1", english.substring(0, 20));
    doc.addToField("input1", english.substring(20));
    doc.addToField("input1", spanish);
    doc.setField("input2", spanish);
    stage.processDocument(doc);
    assertEquals("en", doc.getString("language"));
  }

  @Test
  public void testMinLengthGreaterThanMaxLength() throws Exception {
    Stage stage = factory.get(Map.of(
        "source", List.of("input1", "input2"),
        "languageField", "language",
        "minLength", 80,
        "maxLength", 40,
        "minProbability", 0.85));

    // minLength counts whole values up to the one that passes maxLength in each field, not the truncated detector input
    String english = "This is a sentence in English, and it keeps on going for a while.";
    Document doc = Document.create("doc");
    doc.setField("input1", english);
    doc.setField("input2", english);
    stage.processDocument(doc);
    assertEquals("en", doc.getString("language"));

    Document shortDoc = Document.create("shortDoc");
    shortDoc.setField("input1", english);
    stage.processDocument(shortDoc);
    assertFalse(shortDoc.has("language"));
  }

  @Test
  public void testMinLengthGreaterThanMaxLengthMultiValued() throws Exception {
    Stage stage = factory.get(Map.of(
        "source", List.of("input1"),
        "languageField", "language",
        "minLength", 80,
        "maxLength", 40,
        "minProbability", 0.85));

    // values are 30 chars each; the field stops being read once 60 chars (> maxLength) are seen, so 60 < minLength
    Document doc = Document.create("doc");
    doc.setOrAdd("input1", "This is an English sentence...");
    doc.setOrAdd("input1", "This is an English sentence...");
    doc.setOrAdd("input1", "This is an English sentence...");
    stage.processDocument(doc);
    assertFalse(doc.has("language"));
  }

  @Test
  public void testSpec() throws StageException {
    Stage stage = factory.get("DetectLanguageTest/config.conf");
    assertEquals(
        Set.of(
            "languageField",
            "minLength",
            "languageConfidenceField",
            "updateMode",
            "minProbability",
            "source",
            "maxLength"),
        stage.getNonDefaultLegalProperties());
  }
}
