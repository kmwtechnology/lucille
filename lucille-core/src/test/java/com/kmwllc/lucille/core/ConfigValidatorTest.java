package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.connector.CloseCountingConnector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

public class ConfigValidatorTest {

  @Test
  public void testFormatExceptionsMap() {
    Map<String, List<Exception>> exceptions = new LinkedHashMap<>();
    exceptions.put("pipeline1", List.of(new Exception("exception 1"), new Exception("exception 2")));
    exceptions.put("pipeline2", List.of(new Exception("exception 3")));

    String expected = "Pipeline Configuration is invalid. See exceptions for each element:\n"
        + "\tpipeline1:\n"
        + "\t\texception 1\n"
        + "\t\texception 2\n"
        + "\tpipeline2:\n"
        + "\t\texception 3";
    assertEquals(expected, ConfigValidator.format(exceptions, "Pipeline"));
  }

  @Test
  public void testFormatExceptionsList() {
    List<Exception> exceptions = List.of(new Exception("exception 1"), new Exception("exception 2"), new Exception("exception 3"));

    String expected = "Pipeline Configuration is invalid. Errors:\n"
        + "\texception 1\n"
        + "\texception 2\n"
        + "\texception 3";

    assertEquals(expected, ConfigValidator.format(exceptions, "Pipeline"));
  }

  @Test
  public void testFormatNoExceptions() {
    Map<String, List<Exception>> exceptions = new LinkedHashMap<>();
    assertEquals("Pipeline Configuration is valid.", ConfigValidator.format(exceptions, "Pipeline"));
  }

  @Test
  public void testMisspelledTopLevelKeys() {
    Config config = ConfigFactory.parseString(
        "connectors: [], pipelines: [], publisher {}, solr {}, "
            // misspellings of known keys, including a case difference
            + "publsher { maxPendingDocs: 10 }, wrker {}, Indexer {}, logs {}, "
            // the user's own keys, e.g. for HOCON substitution, are not near any known key
            + "csv_connector { class: x }, basePath: \"/data\", loader {}");

    assertEquals(List.of(
            "Unrecognized top-level config key \"Indexer\" will be ignored; did you mean \"indexer\"?",
            "Unrecognized top-level config key \"logs\" will be ignored; did you mean \"log\"?",
            "Unrecognized top-level config key \"publsher\" will be ignored; did you mean \"publisher\"?",
            "Unrecognized top-level config key \"wrker\" will be ignored; did you mean \"worker\"?"),
        ConfigValidator.findMisspelledTopLevelKeys(config));
  }

  @Test
  public void testValidationOtherParents() throws Exception {
    // only has optional properties. We have one present.
    Map<String, List<Exception>> exceptions = ConfigValidator.validate("RunnerTest/badRunnerConfig.conf");

    assertEquals(1, exceptions.get("other").size());
    String exceptionMessage = exceptions.get("other").get(0).getMessage();

    // the message should complain about these properties...
    assertTrue(exceptionMessage.contains("something"));
    assertTrue(exceptionMessage.contains("somethingElse"));
    // ...but not this property!
    assertFalse(exceptionMessage.contains("metricsLoggingLevel"));

    // Zookeeper only has one required property, "connectString".
    exceptions = ConfigValidator.validate("RunnerTest/badZookeeperConfig.conf");
    assertEquals(1, exceptions.get("other").size());
    exceptionMessage = exceptions.get("other").get(0).getMessage();

    assertTrue(exceptionMessage.contains("something"));
    assertTrue(exceptionMessage.contains("connectString"));

    exceptions = ConfigValidator.validate("RunnerTest/allBadOtherParents.conf");

    // every "other" parent should have produced an "unknown property something" error - track them
    // in a set so we can confirm each one is present (and that none are unexpectedly duplicated).
    Set<String> expectedParents = ConfigValidator.PARENTS_AND_SPECS.stream()
        .map(Pair::getLeft)
        .collect(Collectors.toSet());

    for (Exception e : exceptions.get("other")) {
      // file is designed so all exceptions should be about a property named "something"
      assertTrue(e.getMessage().contains("Config contains unknown property something"));

      // the message identifies its parent as "... with <parentName> Config: ..." - find which parent it is,
      // then remove it from the set to prove we saw an error for it exactly once.
      String parentName = expectedParents.stream()
          .filter(parent -> e.getMessage().contains("with " + parent + " Config"))
          .findFirst()
          .orElseThrow(() -> new AssertionError("Exception did not correspond to a known parent: " + e.getMessage()));
      assertTrue("Saw more than one validation error for parent " + parentName, expectedParents.remove(parentName));
    }

    // every parent should have been accounted for.
    assertTrue("Missing validation errors for parents: " + expectedParents, expectedParents.isEmpty());
  }

  @Test
  public void testUndefinedPipelineFailsValidation() throws Exception {
    // RUN-15: a connector whose pipeline is not defined is a validation error, not a late "Error starting workers"
    Map<String, List<Exception>> errors = ConfigValidator.validate("RunnerTest/pipelineNotFound.conf");
    assertTrue(errors.containsKey("connector1"));
    assertTrue(errors.get("connector1").get(0).getMessage().contains("pipeline5"));
  }

  @Test
  public void testValidationClosesTheConnectorsItConstructs() throws Exception {
    // RUN-06: validation constructs every connector; each one must be closed again or its clients leak on every run
    CloseCountingConnector.reset();
    Map<String, List<Exception>> errors = ConfigValidator.validate("RunnerTest/closeCountingConnector.conf");
    assertTrue(errors.isEmpty());
    assertEquals(2, CloseCountingConnector.constructed.get());
    assertEquals(2, CloseCountingConnector.closed.get());
    CloseCountingConnector.reset();
  }

  private static final String SETUP_ONLY = "connectors: [{name: \"setup\", class: \"com.kmwllc.lucille.connector.SequenceConnector\", numDocs: 0}], "
      + "pipelines: [], ";

  @Test
  public void testConfigWithNoPipelineConnectorNeedsNoIndexer() throws Exception {
    assertEquals(Map.of(), ConfigValidator.validate(ConfigFactory.parseString(SETUP_ONLY)));
    assertEquals(Map.of(), ConfigValidator.validate(ConfigFactory.parseString(SETUP_ONLY + "indexer { batchSize: 100 }")));
    assertEquals(Map.of(), ConfigValidator.validate(ConfigFactory.parseString("connectors: [], pipelines: []")));
    assertTrue(Runner.run(ConfigFactory.parseString(SETUP_ONLY), Runner.RunType.LOCAL).getStatus());
  }

  @Test
  public void testSelectedIndexerIsValidatedWithNoPipelineConnector() throws Exception {
    Map<String, List<Exception>> errors = ConfigValidator.validate(ConfigFactory.parseString(SETUP_ONLY + "indexer { type: \"Bogus\" }"));
    assertTrue(errors.get("indexer").get(0).getMessage().contains("Unknown indexer.type"));
  }

  @Test
  public void testConnectorWithPipelineNeedsIndexer() throws Exception {
    Map<String, List<Exception>> errors = ConfigValidator.validate(ConfigFactory.parseString(
        "connectors: [{name: \"setup\", class: \"com.kmwllc.lucille.connector.SequenceConnector\", numDocs: 0}, "
            + "{name: \"ingest\", class: \"com.kmwllc.lucille.connector.SequenceConnector\", numDocs: 0, pipeline: \"p\"}], "
            + "pipelines: [{name: \"p\", stages: []}]"));
    assertTrue(errors.get("indexer").get(0).getMessage().contains("No indexer is selected"));
  }
}
