package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.DocumentException;
import com.kmwllc.lucille.core.RunResult;
import com.kmwllc.lucille.core.Runner;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.ConfigFactory;

public class ConfigValidationTest {

  @Test
  public void testConditions() {
    testException(NopStage.class, "conditions-field-missing.conf");
    testException(NopStage.class, "conditions-field-renamed.conf");

    testException(NopStage.class, "conditions-optional-unknown.conf");
    testException(NopStage.class, "conditions-optional-renamed.conf");

    testException(NopStage.class, "conditions-optional-wrong.conf");
  }

  @Test
  public void testConcatenate() {
    testException(Concatenate.class, "concatenate-dest-missing.conf");
    testException(Concatenate.class, "concatenate-format-missing.conf");
    testException(Concatenate.class, "concatenate-invalid-parent.conf");
    testException(Concatenate.class, "concatenate-unknown-property.conf");
  }

  @Test
  public void testApplyRegex() throws DocumentException, JsonProcessingException {

    Document doc = Document.createFromJson("{\"id\":\"id\",\"true\": \"boolean\"}");

    testMessage(ApplyRegex.class, "apply-regex-invalid-type-1.conf", doc,
        "be a list, was \"BOOLEAN\"");

    testMessage(ApplyRegex.class, "apply-regex-invalid-type-2.conf", doc,
        "be a string, was \"LIST\"");

    testMessage(ApplyRegex.class, "apply-regex-invalid-type-3.conf", doc, "be a boolean, was \"STRING\"");
  }

  @Test
  public void testNonValidationModeException() throws Exception {
    RunResult result = Runner.run(ConfigFactory.load(addPath("pipelineWithSingleError.conf")), Runner.RunType.TEST);
    assertFalse(result.getStatus());
  }

  // asserts that if two connectors use the same pipeline it is only validated once
  @Test
  public void testDuplicatePipeline() throws Exception {
    Map<String, List<Exception>> exceptions = Runner.runInValidationMode(addPath("shared-pipeline.conf"));
    assertEquals(1, exceptions.size());

    List<Exception> pipeline1Exceptions = exceptions.get("pipeline1");
    assertEquals(2, pipeline1Exceptions.size());

    testException(pipeline1Exceptions.get(0), IllegalArgumentException.class, "Config contains unknown property invalid_property");
    testException(pipeline1Exceptions.get(1), IllegalArgumentException.class, "Config is missing required property fields");
  }

  // asserts that if no connectors use a pipeline it is still validated
  @Test
  public void testUnusedPipeline() throws Exception {
    Map<String, List<Exception>> exceptions = Runner.runInValidationMode(addPath("no-used-pipelines.conf"));
    assertEquals(1, exceptions.size());

    List<Exception> pipeline1Exceptions = exceptions.get("pipeline1");
    assertEquals(2, pipeline1Exceptions.size());

    testException(pipeline1Exceptions.get(0), IllegalArgumentException.class, "Config contains unknown property invalid_property");
    testException(pipeline1Exceptions.get(1), IllegalArgumentException.class, "Config is missing required property fields");
  }

  @Test
  public void testValidationModeException() throws Exception {
    Map<String, List<Exception>> exceptions = Runner.runInValidationMode(addPath("badPipeline.conf"));
    assertEquals(2, exceptions.size());

    List<Exception> pipeline1Exceptions = exceptions.get("pipeline1");
    assertEquals(2, pipeline1Exceptions.size());

    List<Exception> pipeline2Exceptions = exceptions.get("pipeline2");
    assertEquals(2, pipeline2Exceptions.size());

    testException(pipeline1Exceptions.get(0), IllegalArgumentException.class,
        "Config contains unknown property invalid_property");

    // TODO note that for the following two exceptions, the fields are retrieved before
    //  the config validation is called

    testException(pipeline1Exceptions.get(1), IllegalArgumentException.class,
        "Config is missing required property fields");

    testException(pipeline2Exceptions.get(0), IllegalArgumentException.class,
        "Config is missing required property dest");

    testException(pipeline2Exceptions.get(1), IllegalArgumentException.class,
        "Config contains unknown parent default_inputs3");
  }

  @Test
  public void testValidationModeMultipleExceptionsInStage() throws Exception {
    Map<String, List<Exception>> exceptions = Runner.runInValidationMode(addPath("veryBadStage.conf"));
    assertEquals(1, exceptions.size());

    // should only be one exception, but we should have both of the errors mentioned in the response
    assertEquals(1, exceptions.get("pipeline1").size());

    String message = exceptions.get("pipeline1").get(0).getMessage();

    assertTrue(message.contains("Config contains unknown parent bad_parent"));
    assertTrue(message.contains("Config contains unknown property invalid_property"));
  }

  @Test
  public void testPipelineStagesNoClass() throws Exception {
    Map<String, List<Exception>> exceptions = Runner.runInValidationMode(addPath("pipelineStageNoClass.conf"));

    assertEquals(1, exceptions.size());
    assertEquals(1, exceptions.get("pipeline1").size());
    // want to make sure we have special handling for a missing "class", using a simple, straightforward message
    assertEquals("No Stage class specified", exceptions.get("pipeline1").get(0).getMessage());
  }

  @Test
  public void testBadConnector() throws Exception {
    Map<String, List<Exception>> exceptions = Runner.runInValidationMode(addPath("badConnector.conf"));
    assertEquals(2, exceptions.size());

    assertEquals(1, exceptions.get("connector1").size());
    assertEquals(1, exceptions.get("connector2").size());

    assertTrue(exceptions.get("connector1").get(0).getMessage().contains("Config is missing required property path"));
    assertTrue(exceptions.get("connector2").get(0).getMessage().contains("No Connector class specified"));
  }

  @Test
  public void testBadIndexer() throws Exception {
    // NOTE that, when validating indexers, if there is an exception with both the "indexer" block and the
    // specific implementation's config, the "indexer" config gets validated first / has its exceptions thrown
    // before the second one will. (in other words, you have to fix "indexer" issues before seeing issues with your other
    // specific implementation config).
    Map<String, List<Exception>> exceptions = Runner.runInValidationMode(addPath("badIndexer.conf"));
    // five "things" to validate here - Indexer is only one with exceptions
    assertEquals(1, exceptions.size());
    assertTrue(exceptions.get("indexer").get(0).getMessage().contains("Config contains unknown property sendEnabled"));
    assertTrue(exceptions.get("indexer").get(0).getMessage().contains("Config is missing required property index"));

    exceptions = Runner.runInValidationMode(addPath("extraIndexerProps.conf"));
    assertEquals(1, exceptions.size());
    assertTrue(exceptions.get("indexer").get(0).getMessage().contains("Config contains unknown property blah"));
  }

//  @Test
//  public void testNonDisjointPropertiesValidation() {
//    Spec spec = Spec.connector()
//        .withOptionalProperties("property1", "property2")
//        .withOptionalParents(Spec.parent("property1"), Spec.parent("property3"));
//
//    assertThrows(IllegalArgumentException.class, () -> spec.validate(ConfigFactory.empty(), "name"));
//  }

  @Test
  public void testParentSpecValidation() {
    Spec spec = SpecBuilder.connector()
        .requiredParent(
            SpecBuilder.parent("required")
                .requiredNumber("requiredProp")
                .optionalNumber("optionalProp").build())
        .optionalParent(
            SpecBuilder.parent("optional")
                .requiredNumber("requiredProp")
                .optionalNumber("optionalProp").build()).build();

    // All of the properties allowed are present.
    spec.validate(ConfigFactory.parseResourcesAnySyntax("ConfigValidationTest/parentSpec/allPresent.conf"), "test");
    // Each of the parents do not have "optionalProp"
    spec.validate(ConfigFactory.parseResourcesAnySyntax("ConfigValidationTest/parentSpec/noOptionalPropsInParents.conf"), "test");
    // The optional parent is missing. Note that no exception is thrown for optional.requiredProp not being present!
    spec.validate(ConfigFactory.parseResourcesAnySyntax("ConfigValidationTest/parentSpec/noOptionalParent.conf"), "test");

    // 1. The optional parent is present, but it is missing one of its required properties.
    assertThrows(IllegalArgumentException.class,
        () -> spec.validate(ConfigFactory.parseResourcesAnySyntax("ConfigValidationTest/parentSpec/optionalParentMissingRequired.conf"), "test"));

    // 2. The required parent is present, but it is missing one of its required properties.
    assertThrows(IllegalArgumentException.class,
        () -> spec.validate(ConfigFactory.parseResourcesAnySyntax("ConfigValidationTest/parentSpec/requiredParentMissingRequired.conf"), "test"));

    // 3. The required parent is missing.
    assertThrows(IllegalArgumentException.class,
        () -> spec.validate(ConfigFactory.parseResourcesAnySyntax("ConfigValidationTest/parentSpec/requiredParentMissing.conf"), "test"));
  }

  @Test
  public void testValidationMissingConnectors() throws Exception {
    Map<String, List<Exception>> exceptions = Runner.runInValidationMode(addPath("noConnectors.conf"));

    // the "connector name" used for this error is just "connectors". this is the only error present in the returned map.
    assertEquals(1, exceptions.size());
    assertEquals(1, exceptions.get("connectors").size());
  }

  @Test
  public void testValidationMissingPipelines() throws Exception {
    Map<String, List<Exception>> exceptions = Runner.runInValidationMode(addPath("noPipelines.conf"));

    // the "pipeline name" used for this error is just "pipelines".
    assertEquals(1, exceptions.get("pipelines").size());
  }

  @Test
  public void testValidationOtherParents() throws Exception {
    // only has optional properties. We have one present.
    Map<String, List<Exception>> exceptions = Runner.runInValidationMode(addPath("badRunnerConfig.conf"));

    assertEquals(1, exceptions.get("other").size());
    String exceptionMessage = exceptions.get("other").get(0).getMessage();

    // the message should complain about these properties...
    assertTrue(exceptionMessage.contains("something"));
    assertTrue(exceptionMessage.contains("something_else"));
    // ...but not this property!
    assertFalse(exceptionMessage.contains("metricsLoggingLevel"));

    // Zookeeper only has one required property, "connectString".
    exceptions = Runner.runInValidationMode(addPath("badZookeeperConfig.conf"));
    assertEquals(1, exceptions.get("other").size());
    exceptionMessage = exceptions.get("other").get(0).getMessage();

    assertTrue(exceptionMessage.contains("something"));
    assertTrue(exceptionMessage.contains("connectString"));

    exceptions = Runner.runInValidationMode(addPath("badZookeeperAndRunner.conf"));
    // designed the validation method so that each parent (publisher, runner, zookeeper) gets its own exception
    assertEquals(2, exceptions.get("other").size());

    boolean zookeeperSeen = false;
    boolean runnerSeen = false;

    for (Exception e : exceptions.get("other")) {
      // running assertions + making sure we see an exception for zookeeper and runner configs
      if (e.getMessage().contains("zookeeper")) {
        assertTrue(e.getMessage().contains("connectString"));
        assertTrue(e.getMessage().contains("something"));
        zookeeperSeen = true;
      } else if (e.getMessage().contains("runner")) {
        assertTrue(e.getMessage().contains("something"));
        assertTrue(e.getMessage().contains("something_else"));
        runnerSeen = true;
      } else {
        fail("Exception for \"other\" that doesn't reference zookeeper or runner.");
      }
    }

    assertTrue("Didn't see a validation error for zookeeper and runner.", zookeeperSeen && runnerSeen);
  }

  @Test
  public void testSameConnectorAndPipelineNames() throws Exception {
    Map<String, List<Exception>> exceptions = Runner.runInValidationMode(addPath("repeatedConnector.conf"));

    // there should be three exceptions - one for each implementation having an unknown property, and one mentioning
    // that there are multiple connectors named connector1
    assertEquals(3, exceptions.get("connector1").size());
    assertTrue(exceptions.get("connector1").get(0).getMessage().contains("bad_prop_first_time"));
    assertTrue(exceptions.get("connector1").get(1).getMessage().contains("multiple connectors with"));
    assertTrue(exceptions.get("connector1").get(2).getMessage().contains("bad_prop_second_time"));
  }

  private static void processDoc(Class<? extends Stage> stageClass, String config, Document doc)
      throws StageException {
    Stage s = StageFactory.of(stageClass).get(addPath(config));
    s.processDocument(doc);
  }

  private static String addPath(String config) {
    return "ConfigValidationTest/" + config;
  }

  private static void assertContains(String string, String substring) {
    if (!string.contains(substring)) {
      fail();
    }
  }

  private static void testMessage(Class<? extends Stage> stageClass, String config, Document doc, String message) {
    try {
      processDoc(stageClass, config, doc);
    } catch (StageException e) {
      Throwable cause = e.getCause().getCause();
      assertContains(cause.getMessage(), message);
    }
  }

  private static void testException(Exception e, Class<? extends Exception> clazz, String message) {
    assertEquals(e.getClass(), clazz);
    assertContains(e.getMessage(), message);
  }

  private static void testException(Class<? extends Stage> stageClass, String config) {
    try {
      StageFactory.of(stageClass).get(addPath(config));
      fail();
    } catch (StageException e) {
      // expected
    }
  }
}
