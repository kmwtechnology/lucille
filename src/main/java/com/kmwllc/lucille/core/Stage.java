package com.kmwllc.lucille.core;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * An operation that can be performed on a Document.
 *
 * This abstract class provides some base functionality which should be applicable to all Stages.
 *
 * Config Parameters:
 *
 * - conditional_fields (List<String>, Optional) : The fields which will be used to determine if this stage should be applied.
 * Turns off conditional execution by default.
 * - conditional_values (List<String>, Optional) : The values which we will search the conditional fields for.
 * Should be set iff conditional_fields is set.
 * - conditional_operator (String, Optional) : The operator to determine conditional execution.
 * Can be 'must' or 'must_not'. Defaults to must.
 */
public abstract class Stage {

  protected Config config;
  private final Predicate<Document> condition;
  private String name;

  private Timer timer;
  private Timer.Context context;
  private Counter errorCounter;
  private Counter childCounter;

  private final static Set<String> RESERVED_PROPERTIES = makeSet("class");
  private final static Set<String> THIS_OPTIONAL_PROPERTIES = makeSet("name", "conditions");

  private final Set<String> optionalProperties;
  private final Set<String> requiredProperties;
  private final Set<String> nestedProperties;

  public Stage(Config config) {
    this(config, makeSet(), makeSet(), makeSet());
  }

  protected Stage(Config config, Set<String> requiredProperties) {
    this(config, requiredProperties, makeSet(), makeSet());
  }

  protected Stage(Config config, Set<String> requiredProperties, Set<String> optionalProperties) {
    this(config, requiredProperties, optionalProperties, makeSet());
  }

  protected Stage(Config config, Set<String> requiredProperties, Set<String> optionalProperties,
                  Set<String> nestedProperties) {

    this.config = config;
    this.requiredProperties = new HashSet<>(requiredProperties);
    this.optionalProperties = new HashSet<>(optionalProperties);
    this.nestedProperties = new HashSet<>(nestedProperties);

    // todo verify this
    this.optionalProperties.addAll(RESERVED_PROPERTIES);
//    this.requiredProperties.addAll(RESERVED_PROPERTIES);
    this.optionalProperties.addAll(THIS_OPTIONAL_PROPERTIES);

    validateConfig();

    this.name = ConfigUtils.getOrDefault(config, "name", null);
    List<Predicate<Document>> conditions = config.hasPath("conditions") ?
      config.getConfigList("conditions").stream().map(Condition::fromConfig).collect(Collectors.toList())
      : new ArrayList<>();
    this.condition = conditions.stream().reduce((d) -> true, Predicate::and);
  }

  public void start() throws StageException {
  }

  public void stop() throws StageException {}

  public void logMetrics() {
    if (timer==null || childCounter==null || errorCounter==null) {
      LoggerFactory.getLogger(Stage.class).error("Metrics not initialized");
    } else {
      LoggerFactory.getLogger(Stage.class).info(
        String.format("Stage %s metrics. Docs processed: %d. Mean latency: %.4f ms/doc. Children: %d. Errors: %d.",
          name, timer.getCount(), timer.getSnapshot().getMean() / 1000000, childCounter.getCount(), errorCounter.getCount()));
    }
  }

  /**
   * Determines if this Stage should process this Document based on the conditional execution parameters. If no
   * conditionalFields are supplied in the config, the Stage will always execute. If none of the provided conditionalFields
   * are present on the given document, this should behave the same as if the fields were present but none of the supplied
   * values were found in the fields.
   *
   * @param doc the doc to determine processing for
   * @return  boolean representing - should we process this doc according to its conditionals?
   */
  public boolean shouldProcess(Document doc) {
    return condition.test(doc);
  }

  /**
   * Process this Document iff it adheres to our conditional requirements.
   *
   * @param doc the Document
   * @return  a list of child documents resulting from this Stages processing
   * @throws StageException
   */
  public List<Document> processConditional(Document doc) throws StageException {
    if (shouldProcess(doc)) {
      if (timer!=null) {
        context = timer.time();
      }
      try {
        List<Document> children = processDocument(doc);
        if (children!=null && children.size()>0) {
          childCounter.inc(children.size());
        }
        return children;
      } catch (StageException e) {
        if (errorCounter!=null) {
          errorCounter.inc();
        }
        throw e;
      } finally {
        if (context!=null) {
          context.stop();
        }
      }
    }

    return null;
  }

  /**
   * Applies an operation to a Document in place and returns a list containing any child Documents generated
   * by the operation. If no child Documents are generated, the return value should be null.
   *
   * This interface assumes that the list of child Documents is large enough to hold in memory. To support
   * an unbounded number of child documents, this method would need to return an Iterator (or something similar)
   * instead of a List.
   */
  public abstract List<Document> processDocument(Document doc) throws StageException;

  public String getName() {
    return name;
  }

  /**
   * Initialize metrics and set the Stage's name based on the position if the name has not already been set.
   */
  public void initialize(int position, String metricsPrefix) throws StageException {
    if (name==null) {
      this.name = "stage_" + position;
    }

    MetricRegistry metrics = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);
    this.timer = metrics.timer(metricsPrefix + ".stage." + name + ".processDocumentTime");
    this.errorCounter = metrics.counter(metricsPrefix + ".stage." + name + ".errors");
    this.childCounter = metrics.counter(metricsPrefix + ".stage." + name + ".children");
  }

  public Config getConfig() {
    return config;
  }

  public Set<String> getLegalProperties() {
    Set<String> legalProperties = new HashSet<>(requiredProperties);
    legalProperties.addAll(optionalProperties);
    return legalProperties;
  }

  public void validateConfig() throws IllegalArgumentException {

    // todo are nested properties required?

    // verifies that set intersection is empty
    if (!disjoint(this.requiredProperties, this.optionalProperties, this.nestedProperties))
      throw new IllegalArgumentException("Required, optional and nested properties must be disjoint.");

    // verifies all required properties are present
    for (String property: this.requiredProperties) {
      if (!config.hasPath(property)) {
        throw new IllegalArgumentException("Stage config must contain property " + property);
      }
    }

    // verifies that all remaining properties are in the optional set or are nested
    Set<String> legalProperties = getLegalProperties();
    for (Map.Entry<String, ConfigValue> entry: config.entrySet()) {
      if (!legalProperties.contains(entry.getKey()) && !isNestedProperty(entry.getKey())) {
        throw new IllegalArgumentException("Stage config contains unknown property " + entry.getKey());
      }
    }
  }

  @SafeVarargs
  private static boolean disjoint(Set<String>... sets) {

    if (sets == null) {
      throw new IllegalArgumentException("Sets must not be null");
    }

    for (int i = 0; i < sets.length; i++) {

      if (sets[i] == null) {
        throw new IllegalArgumentException("All sets must not be null");
      }

      for (int j = i + 1; j < sets.length; j++) {
        if (!Collections.disjoint(sets[i], sets[j])) {
          return false;
        }
      }
    }

    return true;
  }

  protected static Set<String> makeSet(String... args) {
    return new HashSet<>(Arrays.asList(args));
  }

  private boolean isNestedProperty(String property) {
    int dotIndex = property.indexOf('.');
    return dotIndex > 0 && this.nestedProperties.contains(property.substring(0, dotIndex));
  }
}
