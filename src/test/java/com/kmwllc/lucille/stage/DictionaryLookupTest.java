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

  private StageFactory factory = StageFactory.of(DictionaryLookup.class);

  @Test
  public void testDictionaryLookup() throws StageException {
    Stage stage = factory.get("DictionaryLookupTest/config.conf");

    // Ensure that exact matches are correctly extracted
    Document doc = new Document("doc");
    doc.setField("input1", "China");
    stage.processDocument(doc);
    assertEquals("China", doc.getStringList("output1").get(0));

    // Ensure that multiple matches can be extracted, even in multivalued fields
    Document doc2 = new Document("doc2");
    doc2.setField("input1", "Canada");
    doc2.addToField("input1", "United States");
    doc2.setField("input3", "Taiwan");
    stage.processDocument(doc2);
    assertEquals("Canada", doc2.getStringList("output1").get(0));
    assertEquals("United States", doc2.getStringList("output1").get(1));
    assertEquals("Taiwan", doc2.getStringList("output3").get(0));

    // ensure that partial matches do not get extracted
    Document doc3 = new Document("doc2");
    doc3.setField("input2", "United States of America");
    stage.processDocument(doc3);
    assertNull(doc3.getStringList("output2"));
  }

  @Test
  public void testCorpRecDict() throws Exception {
    Config config = ConfigFactory.load("DictionaryLookupTest/corpRecDict.conf");
    Stage stage = new DictionaryLookup(config);

    Document doc1 = new Document("doc1");
    doc1.setField("input1", "CX");
    stage.processDocument(doc1);
    assertEquals("Christmas Island", doc1.getString("output1"));
  }
}
