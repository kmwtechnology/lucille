package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;
import static org.junit.Assert.*;

public class ApplyStopWordsTest {

  StageFactory factory = StageFactory.of(ApplyStopWords.class);

  @Test
  public void noFieldsTest() throws StageException {
    Stage stage = factory.get("ApplyStopWordsTest/nofields.conf");

    JsonDocument doc = new JsonDocument("doc");
    doc.setField("stopwords1", "this is the best part and i know it");
    doc.setField("stopwords2", "I am a politician not a librarian");
    doc.setField("nostopwords", "there are no stopwords");
    stage.processDocument(doc);
    assertEquals("this best part i know", doc.getString("stopwords1"));
    assertEquals("I am a not a", doc.getString("stopwords2"));
    assertEquals("there are no stopwords", doc.getString("nostopwords"));

    JsonDocument doc2 = new JsonDocument("doc2");
    doc2.setField("multivalued", "the stopwords are here");
    doc2.addToField("multivalued", "the historian, hates stopwords!");
    doc2.addToField("multivalued", "the is of and librarian");
    stage.processDocument(doc2);
    assertEquals("stopwords are here", doc2.getStringList("multivalued").get(0));
    assertEquals(", hates stopwords!", doc2.getStringList("multivalued").get(1));
    assertEquals("", doc2.getStringList("multivalued").get(2));

    JsonDocument doc3 = new JsonDocument("is a stopword");
    doc3.setField("only", "is the librarian ready?");
    stage.processDocument(doc3);
    assertEquals("is a stopword", doc3.getId());
    assertEquals("ready?", doc3.getString("only"));
  }

  @Test
  public void fieldsTest() throws StageException {
    Stage stage = factory.get("ApplyStopWordsTest/fields.conf");

    JsonDocument doc1 = new JsonDocument("doc1");
    doc1.setField("input1", "is there a stopword about turn off");
    doc1.setField("input2", "is historian a librarian?");
    doc1.setField("reserved", "and these stopwords is reserved");
    stage.processDocument(doc1);
    assertEquals("there a stopword", doc1.getString("input1"));
    assertEquals("a ?", doc1.getString("input2"));
    assertEquals("and these stopwords is reserved", doc1.getString("reserved"));
  }

}
