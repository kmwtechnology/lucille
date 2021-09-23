package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;
import static org.junit.Assert.*;

public class DetectLanguageTest {

  private StageFactory factory = StageFactory.of(DetectLanguage.class);

  @Test
  public void testDetectLanguage() throws Exception {
    Stage stage = factory.get("DetectLanguageTest/config.conf");

    // Ensure that the correct language is detected for field pair 1.
    Document doc = new Document("doc");
    doc.setField("input1", "This is a sentence in English!");
    stage.processDocument(doc);
    assertEquals("en", doc.getStringList("language").get(0));
    assertEquals("0.99", doc.getString("lang_conf"));

    // Ensure that the correct language is detected for field pair 3.
    Document doc2 = new Document("doc2");
    doc2.setField("input3", "Eso oracion esta en espanol. Ojala que podimos verla.");
    stage.processDocument(doc2);
    assertEquals("es", doc2.getStringList("language").get(0));
    assertEquals("0.99", doc2.getString("lang_conf"));

    // Ensure multiple languages can be extracted in one pass
    Document doc3 = new Document("doc3");
    Document doc4 = new Document("doc4");
    Document doc5 = new Document("doc5");
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
}
