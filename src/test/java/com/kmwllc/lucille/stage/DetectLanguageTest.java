package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.*;

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
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("DetectLanguageTest/config.conf");
    assertEquals(
        Set.of(
            "language_field",
            "min_length",
            "language_confidence_field",
            "update_mode",
            "min_probability",
            "name",
            "source",
            "conditions",
            "class",
            "max_length"),
        stage.getLegalProperties());
  }
}
