package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class DictionaryLookupTest {

  @Test
  public void testDictionaryLookup() throws StageException {
    Config config = ConfigFactory.load("DictionaryLookupTest/config.conf");
    Stage stage = new DictionaryLookup(config);
    stage.start();

    Document doc = new Document("doc");
    doc.setField("input1", "China");
    stage.processDocument(doc);
    assertEquals("China", doc.getStringList("output1").get(0));

    Document doc2 = new Document("doc2");
    doc2.setField("input1", "Canada");
    doc2.addToField("input1", "United States");
    doc2.setField("input3", "Taiwan");
    stage.processDocument(doc2);
    assertEquals("Canada", doc2.getStringList("output1").get(0));
    assertEquals("United States", doc2.getStringList("output1").get(1));
    assertEquals("Taiwan", doc2.getStringList("output3").get(0));

    Document doc3 = new Document("doc2");
    doc3.setField("input2", "United States of America");
    stage.processDocument(doc3);
    assertNull(doc3.getStringList("output2"));
  }

}
