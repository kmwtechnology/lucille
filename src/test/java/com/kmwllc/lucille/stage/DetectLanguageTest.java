package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;
import static org.junit.Assert.*;

public class DetectLanguageTest {

  // TODO : Should we keep the output as the lang abbreviations
  @Test
  public void testDetectLanguage() throws Exception {
    Config config = ConfigFactory.load("DetectLanguageTest/config.conf");
    Stage stage = new DetectLanguage(config);
    stage.start();

    // Ensure that the correct language is detected for field pair 1.
    Document doc = new Document("doc");
    doc.setField("input1", "This is a sentence in English!");
    stage.processDocument(doc);
    assertEquals("en", doc.getStringList("output1").get(0));

    // Ensure that the correct language is detected for field pair 3.
    Document doc2 = new Document("doc2");
    doc2.setField("input3", "Eso oracion esta en espanol. Ojala que podimos verla.");
    stage.processDocument(doc2);
    assertEquals("es", doc2.getStringList("output3").get(0));

    // Ensure multiple languages can be extracted in one pass
    Document doc3 = new Document("doc3");
    doc3.setField("input1", "Это стандартное предложение на моем языке.");
    doc3.setField("input2", "यह मेरी पसंद की भाषा में एक मानक वाक्य है।");
    doc3.setField("input3", "这是我选择的语言的标准句子。");
    stage.processDocument(doc3);
    assertEquals("ru", doc3.getStringList("output1").get(0));
    assertEquals("hi", doc3.getStringList("output2").get(0));
    assertEquals("zh-cn", doc3.getStringList("output3").get(0));
  }

}
