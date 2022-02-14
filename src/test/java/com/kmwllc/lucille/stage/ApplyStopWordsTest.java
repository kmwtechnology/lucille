package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;
import static org.junit.Assert.*;

public class ApplyStopWordsTest {

  StageFactory factory = StageFactory.of(ApplyStopWords.class);

  @Test
  public void noFieldsTest() throws StageException {
    Stage stage = factory.get("ApplyStopWordsTest/nofields.conf");

    Document doc = new Document("doc");
    doc.setField("stopwords1", "this is the best part and i know it");
    doc.setField("stopwords2", "I am a politician not a librarian");
    doc.setField("nostopwords", "there are no stopwords");
    stage.processDocument(doc);
    // output should not be changed because no fields were specified
    assertEquals("this is the best part and i know it", doc.getString("stopwords1"));
    assertEquals("I am a politician not a librarian", doc.getString("stopwords2"));
    assertEquals("there are no stopwords", doc.getString("nostopwords"));

    Document doc2 = new Document("doc2");
    doc2.setField("multivalued", "the stopwords are here");
    doc2.addToField("multivalued", "the historian, hates stopwords!");
    doc2.addToField("multivalued", "the is of and librarian");
    stage.processDocument(doc2);
    assertEquals("the stopwords are here", doc2.getStringList("multivalued").get(0));
    assertEquals("the historian, hates stopwords!", doc2.getStringList("multivalued").get(1));
    assertEquals("the is of and librarian", doc2.getStringList("multivalued").get(2));

    Document doc3 = new Document("is a stopword");
    doc3.setField("only", "is the librarian ready?");
    stage.processDocument(doc3);
    assertEquals("is a stopword", doc3.getId());
    assertEquals("is the librarian ready?", doc3.getString("only"));
  }

  @Test
  public void fieldsTest() throws StageException {
    Stage stage = factory.get("ApplyStopWordsTest/fields.conf");

    Document doc1 = new Document("doc1");
    doc1.setField("input1", "is there a stopword about turn off");
    doc1.setField("input2", "is historian a librarian?");
    doc1.setField("reserved", "and these stopwords is reserved");
    stage.processDocument(doc1);
    assertEquals("there a stopword", doc1.getString("input1"));
    assertEquals("a librarian?", doc1.getString("input2"));
    assertEquals("and these stopwords is reserved", doc1.getString("reserved"));
  }

  @Test
  public void splitOnWhiteSpaceTest() throws StageException {
    Stage stage = factory.get("ApplyStopWordsTest/fields.conf");

    Document doc1 = new Document("doc1");
    doc1.setField("input1", "the faucet doesn't turn off");
    doc1.setField("input2", "off123");
    doc1.setField("input3", "0off0");

    stage.processDocument(doc1);
    // test normal stop word splitting
    assertEquals("faucet doesn't", doc1.getString("input1"));
    // verify stop words do not split on white space
    assertEquals("off123", doc1.getString("input2"));
    assertEquals("0off0", doc1.getString("input3"));
  }
}
