package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class StageTest {

  private StageFactory factory = StageFactory.of(MockStage.class);

  private static class MockStage extends Stage {

    public MockStage(Config config) {
      super(config);
    }

    @Override
    public List<JsonDocument> processDocument(JsonDocument doc) throws StageException {
      doc.setField("processed", true);

      return null;
    }
  }

  @Test
  public void testProcessMust() throws StageException {
    Stage stage = factory.get("StageTest/processMust.conf");

    JsonDocument doc1 = new JsonDocument("doc1");
    doc1.update("customer_id", UpdateMode.APPEND, "45345", "123", "653");
    stage.processConditional(doc1);
    assertTrue(doc1.has("processed"));
    assertEquals(3, doc1.getStringList("customer_id").size());

    JsonDocument doc2 = new JsonDocument("doc2");
    doc2.update("customer_id", UpdateMode.APPEND, "this", "is", "not", "processed");
    assertFalse(doc2.has("processed"));
    assertEquals(4, doc2.getStringList("customer_id").size());
  }

  @Test
  public void testProcessMustNot() throws StageException {
    Stage stage = factory.get("StageTest/processMustNot.conf");

    JsonDocument doc1 = new JsonDocument("doc1");
    doc1.update("customer_id", UpdateMode.APPEND, "3124124", "123312", "123");
    stage.processConditional(doc1);
    assertFalse(doc1.has("processed"));
    assertEquals(3, doc1.getStringList("customer_id").size());

    JsonDocument doc2 = new JsonDocument("doc2");
    doc2.update("customer_id", UpdateMode.APPEND, "3124124", "123312", "121233");
    stage.processConditional(doc2);
    assertTrue(doc2.has("processed"));
    assertEquals(3, doc1.getStringList("customer_id").size());
  }

  @Test
  public void testMultiCondField() throws Exception {
    Stage stage = factory.get("StageTest/multiCondField.conf");

    JsonDocument doc = new JsonDocument("doc");
    doc.setField("state", "MA");
    doc.setField("country", "China");
    doc.setField("user_id", "987");
    stage.processConditional(doc);
    assertTrue(doc.has("processed"));

    JsonDocument doc2 = new JsonDocument("doc2");
    doc2.setField("state", "NJ");
    doc2.setField("country", "England");
    doc2.setField("user_id", "123467543453");
    stage.processConditional(doc2);
    assertFalse(doc2.has("processed"));
  }

  @Test
  public void testProcessNoCondField() throws Exception {
    Stage stage = factory.get("StageTest/multiCondField.conf");

    JsonDocument doc = new JsonDocument("doc");
    doc.setField("test", "some field");
    doc.setField("another", "some other field");
    stage.processConditional(doc);
    assertFalse(doc.has("processed"));
  }

  @Test
  public void testProcessNoCondFieldMustNot() throws Exception {
    Stage stage = factory.get("StageTest/multiCondFieldMustNot.conf");

    JsonDocument doc = new JsonDocument("doc");
    doc.setField("test", "some field");
    doc.setField("another", "some other field");
    stage.processConditional(doc);
    assertTrue(doc.has("processed"));
  }

  @Test
  public void testProcessMultipleConditions() throws StageException {
    Stage stage = factory.get("StageTest/multipleConditions.conf");

    // Check that the must condition is applied
    JsonDocument doc1 = new JsonDocument("doc1");
    doc1.setField("country", "Russia");
    stage.processConditional(doc1);
    assertTrue(doc1.has("processed"));

    // Check that the must not condition is applied
    JsonDocument doc2 = new JsonDocument("doc2");
    doc2.setField("country", "US");
    doc2.setField("state", "CA");
    stage.processConditional(doc2);
    assertFalse(doc2.has("processed"));

    // Check that the must condition works for either field
    JsonDocument doc3 = new JsonDocument("doc3");
    doc3.setField("long_country", "United States of America");
    doc3.setField("state", "NJ");
    stage.processConditional(doc3);
    assertTrue(doc3.has("processed"));

    JsonDocument doc4 = new JsonDocument("doc4");
    doc4.setField("country", "Canada");
    doc4.setField("province", "BC");
    stage.processConditional(doc4);
    assertFalse(doc4.has("processed"));
  }

  @Test
  public void testGetName() throws Exception {
    Stage stage = factory.get("StageTest/name.conf");
    assertEquals("name1", stage.getName());
  }

  @Test
  public void testGetNameDefault() throws Exception {
    Stage stage = factory.get();
    assertEquals(null, stage.getName());
  }

}
