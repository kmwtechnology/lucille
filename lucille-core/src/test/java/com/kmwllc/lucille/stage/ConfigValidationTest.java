package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import com.kmwllc.lucille.core.configSpec.ConfigSpec;
import com.kmwllc.lucille.core.configSpec.StageSpec;
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
        "com.kmwllc.lucille.stage.NopStage: Config contains unknown property invalid_property");
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
        "com.kmwllc.lucille.stage.NopStage: Config contains unknown property invalid_property");

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

    testException(exceptions1.get(0), IllegalArgumentException.class, "com.kmwllc.lucille.stage.NopStage: " +
        "Config contains unknown property invalid_property");

    // TODO note that for the following two exceptions, the fields are retrieved before
    //  the config validation is called

    testException(exceptions1.get(1), IllegalArgumentException.class,
        "Config must contain property fields");

    testException(exceptions2.get(0), IllegalArgumentException.class,
        "Config must contain property dest");

    testException(exceptions2.get(1), IllegalArgumentException.class, "com.kmwllc.lucille.stage.Concatenate: " +
        "Config contains unknown property default_inputs3");
  }

  @Test
  public void testNonDisjointPropertiesValidation() {
    ConfigSpec spec = new StageSpec()
        .withRequiredProperties("property1", "property2")
        .withOptionalParents("property1", "property3");

    assertThrows(IllegalArgumentException.class, () -> spec.validate(ConfigFactory.empty()));
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
      Stage stage = StageFactory.of(stageClass).get(addPath(config));
      stage.validateConfigWithConditions();
      fail();
    } catch (StageException e) {
      // expected
    }
  }
}
