package com.kmwllc.lucille.core;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.indexer.IndexerFactory;
import com.kmwllc.lucille.message.KafkaUtils;
import com.kmwllc.lucille.message.LocalMessenger;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates a complete Lucille config without running it: its pipelines (by constructing each stage), its connectors
 * (by constructing each connector), its indexer, and the top-level framework blocks (publisher, runner, worker, etc.).
 * Used before every run, by the <code>-validate</code> command line flag, and by the API server before it stores a config.
 */
public final class ConfigValidator {

  private static final Logger log = LoggerFactory.getLogger(ConfigValidator.class);

  /**
   * Config Keys & Specs for the "other parents" in a Lucille Config validated here.
   * If a new "other" parent is added here, it should be added here along with its Spec.
   */
  static final List<Pair<String, Spec>> PARENTS_AND_SPECS = List.of(
      Pair.of("publisher", PublisherImpl.SPEC),
      Pair.of("log", LogUtils.SPEC),
      Pair.of("runner", Runner.SPEC),
      Pair.of("kafka", KafkaUtils.SPEC),
      Pair.of("zookeeper", ZKRetryCounter.SPEC),
      Pair.of("worker", Worker.SPEC)
  );

  /**
   * Every top-level key Lucille reads: the components, the framework blocks above, and the backend blocks indexers read.
   * Users may add top-level keys of their own (for example, to reuse a block through HOCON substitution), so an unknown
   * top-level key is only reported when it looks like a misspelling of one of these.
   */
  static final Set<String> KNOWN_TOP_LEVEL_KEYS = Stream.concat(
      Stream.of("connectors", "pipelines", "indexer", "solr", "opensearch", "elasticsearch", "csv", "pinecone", "weaviate"),
      PARENTS_AND_SPECS.stream().map(Pair::getLeft)).collect(Collectors.toUnmodifiableSet());

  private ConfigValidator() {
  }

  /**
   * Loads the config with the given resource name (see {@link ConfigFactory#load(String)}) and validates it.
   *
   * @param configResource The name of the config resource to load and validate.
   * @return A map of entry names to lists of Exceptions, as described in {@link #validate(Config)}.
   * @throws Exception If a larger error occurs, preventing the Config from being validated.
   */
  public static Map<String, List<Exception>> validate(String configResource) throws Exception {
    return validate(ConfigFactory.load(configResource));
  }

  /**
   * Validates the given Config, including its pipelines, connectors, indexers, and other miscellaneous configuration
   * (Publisher, Runner, Kafka, etc). Returns a map of entry names to lists of Exceptions, representing errors with the
   * configuration. The map uses individual pipeline / connector names for keys, and then "indexer" and "other" for the indexer / other
   * validation issues, respectively. (Keys are only present if there are associated errors.)
   *
   * @param config The configuration you want to validate.
   * @return Returns a map of entry names to lists of Exceptions (errors with the Config).
   * @throws Exception If a larger error occurs, preventing the Config from being validated.
   */
  public static Map<String, List<Exception>> validate(Config config) throws Exception {
    // Resolve any potential substitutions once, to prevent errors about missing values.
    config = config.resolve();
    Map<String, List<Exception>> allExceptionsMap = new HashMap<>();

    Map<String, List<Exception>> pipelineExceptions = validatePipelines(config);
    log.info(format(pipelineExceptions, "Pipeline"));

    Map<String, List<Exception>> connectorExceptions = validateConnectors(config);
    addUndefinedPipelineExceptions(config, connectorExceptions);
    log.info(format(connectorExceptions, "Connector"));

    List<Exception> indexerExceptions = validateIndexer(config);
    log.info(format(indexerExceptions, "Indexer"));

    List<Exception> otherParentExceptions = validateOtherParents(config);
    log.info(format(otherParentExceptions, "Other (Publisher, Runner, etc.)"));

    findMisspelledTopLevelKeys(config).forEach(log::warn);

    allExceptionsMap.putAll(pipelineExceptions);
    allExceptionsMap.putAll(connectorExceptions);

    // only put "indexer" key in the map if there are errors.
    if (!indexerExceptions.isEmpty()) {
      if (allExceptionsMap.containsKey("indexer")) {
        log.warn("A pipeline or connector was named \"indexer\". Its validation error(s) will be combined with error(s) for indexer configuration.");
        allExceptionsMap.get("indexer").addAll(indexerExceptions);
      } else {
        allExceptionsMap.put("indexer", indexerExceptions);
      }
    }

    // only put "other" key in the map if there are errors.
    if (!otherParentExceptions.isEmpty()) {
      if (allExceptionsMap.containsKey("other")) {
        log.warn("A pipeline or connector was named \"other\". Its validation error(s) will be combined with error(s) for other configuration (publisher, worker, etc.).");
        allExceptionsMap.get("other").addAll(otherParentExceptions);
      } else {
        allExceptionsMap.put("other", otherParentExceptions);
      }
    }

    return allExceptionsMap;
  }

  /**
   * Returns a stringified version of the given map of exceptions, which is mapping individual "elements" (usually, pipelines) to their associated
   * exceptions. Map may be empty. Uses the given validation name in logged messages to clarify which element of the Lucille Config is being validated.
   */
  public static String format(Map<String, List<Exception>> exceptions, String validationName) {
    if (exceptions.isEmpty()) {
      return validationName + " Configuration is valid.";
    } else {
      StringBuilder message = new StringBuilder(validationName + " Configuration is invalid. See exceptions for each element:\n");

      for (Map.Entry<String, List<Exception>> entry : exceptions.entrySet()) {
        message.append("\t").append(entry.getKey()).append(":").append("\n");

        for (Exception e : entry.getValue()) {
          message.append("\t\t").append(e.getMessage()).append("\n");
        }
      }
      return message.delete(message.length() - 1, message.length()).toString();
    }
  }

  /**
   * Returns a stringified version of the given list of exceptions. List may be empty. Uses the given validation name in logged messages
   * to clarify which element of the Lucille Config is being validated.
   */
  // package access for unit test
  static String format(List<Exception> exceptions, String validationName) {
    if (exceptions.isEmpty()) {
      return validationName + " Configuration is valid.";
    } else {
      StringBuilder message = new StringBuilder(validationName + " Configuration is invalid. Errors:\n");

      for (Exception exc : exceptions) {
        message.append("\t").append(exc.getMessage()).append("\n");
      }
      return message.delete(message.length() - 1, message.length()).toString();
    }
  }

  private static Map<String, List<Exception>> validateConnectors(Config rootConfig) {
    Map<String, List<Exception>> exceptionMap = new LinkedHashMap<>();

    if (!rootConfig.hasPath("connectors")) {
      exceptionMap.put("connectors", List.of(new Exception("Configuration does not contain key \"connectors\".")));
      return exceptionMap;
    }

    Set<String> seenNames = new HashSet<>();
    int idx = 0;

    for (Config connectorConfig : rootConfig.getConfigList("connectors")) {
      idx++;
      // the name the connector has in a run (see Connector.fromConfig)
      String connectorName = ConfigUtils.getStringOrDefault(connectorConfig, "name", "connector_" + idx);

      List<Exception> exceptionsForConnector = Connector.getConnectorConfigExceptions(connectorConfig, idx);
      if (!seenNames.add(connectorName)) {
        exceptionMap.computeIfAbsent(connectorName, k -> new ArrayList<>()).add(new Exception("There are multiple connectors with the name " + connectorName));
      }

      if (!exceptionsForConnector.isEmpty()) {
        exceptionMap.computeIfAbsent(connectorName, k -> new ArrayList<>()).addAll(exceptionsForConnector);
      }
    }

    return exceptionMap;
  }

  /**
   * Adds an exception for every connector whose <code>pipeline</code> names a pipeline that is not defined. Without this
   * check a typo passes validation; a local run then fails late with "Error starting workers" and a distributed run
   * publishes to a topic nobody consumes.
   */
  private static void addUndefinedPipelineExceptions(Config rootConfig, Map<String, List<Exception>> exceptionMap) {
    if (!rootConfig.hasPath("connectors") || !rootConfig.hasPath("pipelines")) {
      return;
    }

    Set<String> pipelineNames = new HashSet<>();
    for (Config pipelineConfig : rootConfig.getConfigList("pipelines")) {
      if (pipelineConfig.hasPath("name")) {
        pipelineNames.add(pipelineConfig.getString("name"));
      }
    }

    int idx = 0;
    for (Config connectorConfig : rootConfig.getConfigList("connectors")) {
      idx++;
      if (!connectorConfig.hasPath("pipeline")) {
        continue;
      }
      String pipeline = connectorConfig.getString("pipeline");
      if (!pipelineNames.contains(pipeline)) {
        // the name the connector has in a run (see Connector.fromConfig)
        String key = ConfigUtils.getStringOrDefault(connectorConfig, "name", "connector_" + idx);
        exceptionMap.computeIfAbsent(key, k -> new ArrayList<>()).add(new Exception(
            "Connector " + key + " references pipeline \"" + pipeline + "\", which is not defined. Defined pipelines: "
                + pipelineNames));
      }
    }
  }

  /**
   * Returns a mapping from pipeline names to the list of exceptions produced when validating them. (only includes
   * a pipeline name if there are errors with its config.)
   */
  private static Map<String, List<Exception>> validatePipelines(Config rootConfig) throws Exception {
    Map<String, List<Exception>> exceptionMap = new LinkedHashMap<>();

    if (!rootConfig.hasPath("pipelines")) {
      exceptionMap.put("pipelines", List.of(new Exception("Configuration does not contain key \"pipelines\".")));
      return exceptionMap;
    }

    for (Config pipelineConfig : rootConfig.getConfigList("pipelines")) {
      String pipelineName = pipelineConfig.getString("name");

      List<Exception> exceptionsForPipeline = Pipeline.validateStages(rootConfig, pipelineName);

      if (!exceptionsForPipeline.isEmpty()) {
        // No checks on whether the map has the pipeline name, since Pipeline checks for duplicate names...
        exceptionMap.put(pipelineName, exceptionsForPipeline);
      }
    }

    return exceptionMap;
  }

  private static List<Exception> validateIndexer(Config config) {
    Indexer indexer = null;
    try {
      indexer = IndexerFactory.fromConfig(config, new LocalMessenger(), true, "");
    } catch (Exception e) {
      return List.of(e);
    } finally {
      // the indexer was constructed only to validate its config; release any client its constructor opened
      if (indexer != null) {
        try {
          indexer.closeConnection();
        } catch (Exception e) {
          log.debug("Error closing indexer constructed for validation", e);
        }
      }
    }

    return List.of();
  }

  private static List<Exception> validateOtherParents(Config rootConfig) {
    List<Exception> exceptions = new ArrayList<>();

    for (Pair<String, Spec> parentAndSpec : PARENTS_AND_SPECS) {
      String parentName = parentAndSpec.getLeft();

      if (!rootConfig.hasPath(parentName)) {
        continue;
      }

      try {
        Spec spec = parentAndSpec.getRight();
        Config parentConfig = rootConfig.getConfig(parentName);
        spec.validate(parentConfig, parentName);
      } catch (IllegalArgumentException e) {
        exceptions.add(e);
      }
    }

    // closed-set values the Specs above cannot check
    try {
      Runner.getMetricsLoggingLevel(rootConfig);
    } catch (IllegalArgumentException e) {
      exceptions.add(e);
    }

    return exceptions;
  }

  /**
   * Returns a warning for each top-level key that is not one Lucille reads but is within a small edit distance of one
   * (ignoring case), such as "publsher" for "publisher". Other unknown top-level keys are assumed to be the user's own.
   */
  static List<String> findMisspelledTopLevelKeys(Config config) {
    List<String> warnings = new ArrayList<>();

    for (String key : new TreeSet<>(config.root().keySet())) {
      if (KNOWN_TOP_LEVEL_KEYS.contains(key)) {
        continue;
      }

      String closest = null;
      int closestDistance = Integer.MAX_VALUE;
      for (String known : KNOWN_TOP_LEVEL_KEYS) {
        // allow one edit for short names like "log" and "csv", so short keys of the user's own aren't flagged
        int maxDistance = known.length() <= 4 ? 1 : 2;
        int distance = new LevenshteinDistance(maxDistance).apply(key.toLowerCase(Locale.ROOT), known);
        if (distance >= 0 && (distance < closestDistance || (distance == closestDistance && known.compareTo(closest) < 0))) {
          closest = known;
          closestDistance = distance;
        }
      }

      if (closest != null) {
        warnings.add("Unrecognized top-level config key \"" + key + "\" will be ignored; did you mean \"" + closest + "\"?");
      }
    }

    return warnings;
  }
}
