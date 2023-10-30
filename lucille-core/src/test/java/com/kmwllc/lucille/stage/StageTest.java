package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;
import org.junit.Test;

import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.*;

public class StageTest {

  private final StageFactory factory = StageFactory.of(MockStage.class);

  private static class MockStage extends Stage {

    public MockStage(Config config) {
      super(config);
    }

    @Override
    public Iterator<Document> processDocument(Document doc) {
      doc.setField("processed", true);

      return null;
    }
  }

  private static void assertProcessed(Stage stage, Document doc, boolean processed) throws StageException {
    // remove if present
    doc.removeField("processed");

    // process and check if processed field is present
    stage.processConditional(doc);
    assertEquals(processed, doc.has("processed"));
  }

  private static void assertProcessedMultiField(Stage stage, boolean processed, String ... values ) throws StageException {

    // create document
    String field = "customer_id";
    Document doc = Document.create("doc");
    doc.update(field, UpdateMode.APPEND, values);

    // save the length of the field before processing
    int fieldLength = doc.getStringList(field).size();

    assertProcessed(stage, doc, processed);

    // confirm that the length of the field is the same before and after processing
    assertEquals(fieldLength, doc.getStringList(field).size());
  }

  @Test
  public void testProcessMust() throws StageException {
    Stage stage = factory.get("StageTest/processMust.conf");

    // one of the values is present in the document's field
    assertProcessedMultiField(stage, true, "45345", "123", "653");

    // none of the values are present in the document's field
    assertProcessedMultiField(stage, false, "this", "is", "not", "processed");
  }

  @Test
  public void testProcessMustNot() throws StageException {
    Stage stage = factory.get("StageTest/processMustNot.conf");

    // one of the values is present in the document's field
    assertProcessedMultiField(stage, false, "45345", "123", "653");

    // none of the values are present in the document's field
    assertProcessedMultiField(stage, true, "3124124", "123312", "121233");
  }

  @Test
  public void testMultiCondField() throws Exception {
    Stage stage = factory.get("StageTest/multiCondField.conf");

    Document doc = Document.create("doc");
    doc.setField("state", "MA");
    doc.setField("country", "China");
    doc.setField("user_id", "987");
    assertProcessed(stage, doc, true);

    Document doc2 = Document.create("doc2");
    doc2.setField("state", "NJ");
    doc2.setField("country", "England");
    doc2.setField("user_id", "123467543453");
    assertProcessed(stage, doc2, false);
  }

  @Test
  public void testProcessNoCondField() throws Exception {
    Stage stage = factory.get("StageTest/multiCondField.conf");

    Document doc = Document.create("doc");
    doc.setField("test", "some field");
    doc.setField("another", "some other field");
    assertProcessed(stage, doc, false);
  }

  @Test
  public void testProcessNoCondFieldMustNot() throws Exception {
    Stage stage = factory.get("StageTest/multiCondFieldMustNot.conf");

    Document doc = Document.create("doc");
    doc.setField("test", "some field");
    doc.setField("another", "some other field");
    assertProcessed(stage, doc, true);
  }

  @Test
  public void testProcessMultipleConditions() throws StageException {
    Stage stage = factory.get("StageTest/multipleConditions.conf");

    // Check that the must condition is applied
    Document doc1 = Document.create("doc1");
    doc1.setField("country", "Russia");
    assertProcessed(stage, doc1, true);

    // add state with value in a list
    doc1.setField("state", "MA");
    assertProcessed(stage, doc1, false);

    // add state with value not in a list
    doc1.setField("state", "NJ");
    assertProcessed(stage, doc1, true);

    // expecting the country field to be present
    doc1.removeField("country");
    assertProcessed(stage, doc1, false);

    // Check that the must not condition is applied
    Document doc2 = Document.create("doc2");
    doc2.setField("country", "US");
    doc2.setField("state", "CA");
    assertProcessed(stage, doc2, false);

    // Check that the must condition works for either field
    Document doc3 = Document.create("doc3");
    doc3.setField("long_country", "United States of America");
    doc3.setField("state", "NJ");
    assertProcessed(stage, doc3, true);

    Document doc4 = Document.create("doc4");
    doc4.setField("country", "Canada");
    doc4.setField("province", "BC");
    assertProcessed(stage, doc4, false);
  }

  @Test
  public void testGetName() throws Exception {
    Stage stage = factory.get("StageTest/name.conf");
    assertEquals("name1", stage.getName());
  }

  @Test
  public void testGetNameDefault() throws Exception {
    Stage stage = factory.get();
    assertNull(stage.getName());
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("StageTest/processMust.conf");
    assertEquals(Set.of("name", "conditions", "class"), stage.getLegalProperties());
  }
}
