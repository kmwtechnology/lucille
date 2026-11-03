package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class StageTest {

  private final StageFactory factory = StageFactory.of(MockStage.class);

  public static class MockStage extends Stage {

    public static final Spec SPEC = SpecBuilder.stage().build();

    public MockStage(Config config) {
      super(config);
    }

    @Override
    public Iterator<Document> processDocument(Document doc) {
      doc.setField("processed", true);

      return null;
    }
  }

  public static class SubclassWithoutSpec extends MockStage {

    public SubclassWithoutSpec(Config config) {
      super(config);
    }
  }

  public static class SubclassWithRedeclaredSpec extends MockStage {

    public static final Spec SPEC = MockStage.SPEC;

    public SubclassWithRedeclaredSpec(Config config) {
      super(config);
    }
  }

  @Test
  public void testEmptyConditionFieldsRejected() {
    // an empty fields list used to make the condition pass for every document, even with must_not
    for (String operator : List.of("must", "must_not")) {
      Config config = ConfigFactory.parseString("conditions: [{ fields: [], operator: " + operator + " }]");
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new MockStage(config));
      assertTrue(e.getMessage().contains("fields must not be empty"));
    }
  }

  @Test
  public void testSpecIsNotInherited() throws Exception {
    Config config = ConfigFactory.empty();

    RuntimeException e = assertThrows(RuntimeException.class, () -> new SubclassWithoutSpec(config));
    assertTrue(e.getMessage().contains(SubclassWithoutSpec.class.getName()));
    assertTrue(e.getMessage().contains("not on its superclasses"));

    Stage stage = new SubclassWithRedeclaredSpec(config);
    assertProcessed(stage, Document.create("doc"), true);
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
  public void testNoValues() throws StageException {
    Stage stage = factory.get("StageTest/noValues.conf");

    // Check that the must condition is applied
    Document doc1 = Document.create("doc1");
    doc1.setField("country", "Russia");
    assertProcessed(stage, doc1, false);
    doc1.setField("long_country", "America");
    assertProcessed(stage, doc1, true);

    // adding state should stop processing regardless of value
    doc1.setField("state", "MA");
    assertProcessed(stage, doc1, false);
    doc1.setField("state", "NJ");
    assertProcessed(stage, doc1, false);
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
  public void testGetNonDefaultLegalProperties() throws StageException {
    Stage stage = factory.get("StageTest/processMust.conf");
    assertEquals(Set.of(), stage.getNonDefaultLegalProperties());
  }

  @Test
  public void testAnyPolicyTwoConditions() throws StageException {
    Stage stage = factory.get("StageTest/anyPolicyTwoConditions.conf");

    // Check that the must condition is applied
    Document doc1 = Document.create("doc1");
    doc1.setField("user_id", "1234");
    assertProcessed(stage, doc1, true);

    // add state with value in a list
    Document doc2 = Document.create("doc2");
    doc2.setField("state", "MA");
    assertProcessed(stage, doc2, true);

    Document doc3 = Document.create("doc3");
    doc3.setField("user_id", "1234");
    doc3.setField("state", "MA");
    assertProcessed(stage, doc3, true);

    Document doc4 = Document.create("doc4");
    doc4.setField("country", "Russia");
    assertProcessed(stage, doc4, false);
  }

  @Test
  public void testAnyPolicyOneCondition() throws StageException {
    Stage stage = factory.get("StageTest/anyPolicyOneCondition.conf");

    Document doc1 = Document.create("doc1");
    doc1.setField("user_id", "1234");
    assertProcessed(stage, doc1, true);
  }

  @Test
  public void testAnyPolicyNoCondition() throws StageException {
    Stage stage = factory.get("StageTest/anyPolicyNoCondition.conf");

    Document doc1 = Document.create("doc1");
    doc1.setField("user_id", "1234");
    assertProcessed(stage, doc1, true);
  }

  @Test
  public void testAllPolicyNoCondition() throws StageException {
    Stage stage = factory.get("StageTest/allPolicyNoCondition.conf");

    Document doc1 = Document.create("doc1");
    doc1.setField("user_id", "1234");
    assertProcessed(stage, doc1, true);
  }

  @Test
  public void testAllPolicyOneCondition() throws StageException {
    Stage stage = factory.get("StageTest/allPolicyOneCondition.conf");

    Document doc1 = Document.create("doc1");
    doc1.setField("user_id", "1234");
    assertProcessed(stage, doc1, true);

    Document doc2 = Document.create("doc2");
    doc2.setField("state", "MA");
    assertProcessed(stage, doc2, false);
  }

  @Test
  public void testAllPolicyTwoConditions() throws StageException {
    Stage stage = factory.get("StageTest/allPolicyTwoConditions.conf");

    Document doc1 = Document.create("doc1");
    doc1.setField("user_id", "1234");
    assertProcessed(stage, doc1, false);

    Document doc2 = Document.create("doc2");
    doc2.setField("state", "MA");
    assertProcessed(stage, doc2, false);


    Document doc3 = Document.create("doc3");
    doc3.setField("user_id", "1234");
    doc3.setField("state", "MA");
    assertProcessed(stage, doc3, true);
  }

  @Test
  public void testUnsupportedPolicy() {
   assertThrows(StageException.class ,() -> factory.get("StageTest/unsupportedPolicy.conf"));
  }


  @Test
  public void testConditionFieldNullValue() throws StageException {
    Stage stage = factory.get("StageTest/nullCondField.conf");

    Document doc1 = Document.create("doc1");
    doc1.setField("user_id", (String) null);
    assertProcessed(stage, doc1, true);

    Document doc2 = Document.create("doc2");
    doc2.setField("user_id", "1234");
    assertProcessed(stage, doc2, false);

    // must condition & field not present
    Document doc3 = Document.create("doc3");
    assertProcessed(stage, doc3, false);
  }

  @Test
  public void testValuesPathMust() throws StageException {
    Stage stage = factory.get("StageTest/valuesPathMust.conf");

    Document hit = Document.create("hit");
    hit.setField("user_id", "1234");
    assertProcessed(stage, hit, true);

    Document miss = Document.create("miss");
    miss.setField("user_id", "n/a");
    assertProcessed(stage, miss, false);
  }

  @Test
  public  void testValuesAndPathMutuallyExclusive() throws StageException {
    assertThrows(StageException.class, () -> factory.get("StageTest/valuesAndPath.conf"));
  }

  @Test
  public void testFromConfigCreatesTheStage() throws StageException {
    Stage stage = Stage.fromConfig(stageConfig(MockStage.class.getName()));
    assertTrue(stage instanceof MockStage);
  }

  @Test
  public void testFromConfigUnknownClass() {
    StageException e = assertThrows(StageException.class, () -> Stage.fromConfig(stageConfig("com.example.NoSuchStage")));
    assertTrue(e.getCause() instanceof ClassNotFoundException);
  }

  @Test
  public void testFromConfigClassThatIsNotAStage() {
    assertThrows(IllegalArgumentException.class, () -> Stage.fromConfig(stageConfig(String.class.getName())));
  }

  @Test
  public void testFromConfigStageWithoutConfigConstructor() {
    StageException e =
        assertThrows(StageException.class, () -> Stage.fromConfig(stageConfig(NoConfigConstructorStage.class.getName())));
    assertTrue(e.getCause() instanceof NoSuchMethodException);
  }

  @Test
  public void testFromConfigRethrowsTheConstructorsStageException() {
    StageException e =
        assertThrows(StageException.class, () -> Stage.fromConfig(stageConfig(StageExceptionStage.class.getName())));
    assertEquals("constructor failed", e.getMessage());
  }

  @Test
  public void testFromConfigWrapsOtherCheckedExceptions() {
    StageException e =
        assertThrows(StageException.class, () -> Stage.fromConfig(stageConfig(IOExceptionStage.class.getName())));
    assertTrue(e.getCause() instanceof IOException);
  }

  @Test
  public void testFromConfigPassesInvalidConfigThrough() {
    Config config = stageConfig(MockStage.class.getName()).withValue("bogus", ConfigValueFactory.fromAnyRef(true));
    assertThrows(IllegalArgumentException.class, () -> Stage.fromConfig(config));
  }

  private static Config stageConfig(String className) {
    return ConfigFactory.parseMap(Map.of("class", className));
  }

  public static class NoConfigConstructorStage extends Stage {

    public static final Spec SPEC = SpecBuilder.stage().build();

    public NoConfigConstructorStage() {
      super(ConfigFactory.empty());
    }

    @Override
    public Iterator<Document> processDocument(Document doc) {
      return null;
    }
  }

  public static class StageExceptionStage extends Stage {

    public static final Spec SPEC = SpecBuilder.stage().build();

    public StageExceptionStage(Config config) throws StageException {
      super(config);
      throw new StageException("constructor failed");
    }

    @Override
    public Iterator<Document> processDocument(Document doc) {
      return null;
    }
  }

  public static class IOExceptionStage extends Stage {

    public static final Spec SPEC = SpecBuilder.stage().build();

    public IOExceptionStage(Config config) throws IOException {
      super(config);
      throw new IOException("resource unavailable");
    }

    @Override
    public Iterator<Document> processDocument(Document doc) {
      return null;
    }
  }
}
