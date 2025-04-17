package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.kmwllc.lucille.core.Spec;
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
        "source has type BOOLEAN rather than LIST");

    testMessage(ApplyRegex.class, "apply-regex-invalid-type-2.conf", doc,
        "regex has type LIST rather than STRING");

    // multiple variants of the class cast error message have been observed so we only look for a short substring
    testMessage(ApplyRegex.class, "apply-regex-invalid-type-3.conf", doc,
        "java.lang.String");
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
    assertEquals(3, exceptions.size());

    List<Exception> exceptions1 = exceptions.get("pipeline1");
    assertEquals(2, exceptions1.size());

    testException(exceptions1.get(0), IllegalArgumentException.class,
        "Error(s) with com.kmwllc.lucille.stage.NopStage Config: [Config contains unknown property invalid_property]");
    testException(exceptions1.get(1), IllegalArgumentException.class, "Config must contain property fields");
  }

  // asserts that if no connectors use a pipeline it is still validated
  @Test
  public void testUnusedPipeline() throws Exception {
    Map<String, List<Exception>> exceptions = Runner.runInValidationMode(addPath("no-used-pipelines.conf"));
    assertEquals(1, exceptions.size());

    List<Exception> exceptions1 = exceptions.get("pipeline1");
    assertEquals(2, exceptions1.size());

    testException(exceptions1.get(0), IllegalArgumentException.class,
        "Error(s) with com.kmwllc.lucille.stage.NopStage Config: [Config contains unknown property invalid_property]");

    testException(exceptions1.get(1), IllegalArgumentException.class, "Config must contain property fields");
  }

  @Test
  public void testValidationModeException() throws Exception {
    Map<String, List<Exception>> exceptions = Runner.runInValidationMode(addPath("pipeline.conf"));
    assertEquals(4, exceptions.size());

    List<Exception> exceptions1 = exceptions.get("pipeline1");
    assertEquals(2, exceptions1.size());

    List<Exception> exceptions2 = exceptions.get("pipeline2");
    assertEquals(2, exceptions2.size());

    testException(exceptions1.get(0), IllegalArgumentException.class,
        "Error(s) with com.kmwllc.lucille.stage.NopStage Config: [Config contains unknown property invalid_property]");

    // TODO note that for the following two exceptions, the fields are retrieved before
    //  the config validation is called

    testException(exceptions1.get(1), IllegalArgumentException.class,
        "Config must contain property fields");

    testException(exceptions2.get(0), IllegalArgumentException.class,
        "Config must contain property dest");

    testException(exceptions2.get(1), IllegalArgumentException.class,
        "Error(s) with com.kmwllc.lucille.stage.Concatenate Config: [Config contains unknown parent default_inputs3]");
  }

  @Test
  public void testValidationModeMultipleExceptionsInStage() throws Exception {
    Map<String, List<Exception>> exceptions = Runner.runInValidationMode(addPath("veryBadStage.conf"));
    assertEquals(3, exceptions.size());

    // should only be one exception, but we should have both of the errors mentioned in the response
    assertEquals(1, exceptions.get("pipeline1").size());

    String message = exceptions.get("pipeline1").get(0).getMessage();

    assertTrue(message.contains("Config contains unknown parent bad_parent"));
    assertTrue(message.contains("Config contains unknown property invalid_property"));
  }

  @Test
  public void testBadConnector() throws Exception {
    Map<String, List<Exception>> exceptions = Runner.runInValidationMode(addPath("badConnector.conf"));
    assertEquals(4, exceptions.size());

    assertEquals(1, exceptions.get("connector1").size());
    assertEquals(1, exceptions.get("connector2").size());

    assertTrue(exceptions.get("connector1").get(0).getMessage().contains("Config must contain property path"));
    assertTrue(exceptions.get("connector2").get(0).getMessage().contains("No configuration setting found for key 'class'"));
  }

  @Test
  public void testNonDisjointPropertiesValidation() {
    Spec spec = Spec.connector()
        .withOptionalProperties("property1", "property2")
        .withOptionalParents(Spec.parent("property1"), Spec.parent("property3"));

    assertThrows(IllegalArgumentException.class, () -> spec.validate(ConfigFactory.empty(), "name"));
  }

  @Test
  public void testParentSpecValidation() {
    Spec spec = Spec.connector()
        .withRequiredParents(
            Spec.parent("required")
                .withRequiredProperties("requiredProp")
                .withOptionalProperties("optionalProp"))
        .withOptionalParents(
            Spec.parent("optional")
                .withRequiredProperties("requiredProp")
                .withOptionalProperties("optionalProp"));

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
