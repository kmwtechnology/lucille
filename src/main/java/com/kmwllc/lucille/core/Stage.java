package com.kmwllc.lucille.core;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.collect.Sets;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An operation that can be performed on a Document.<br>
 * This abstract class provides some base functionality which should be applicable to all Stages.
 * <br>
 * <br>
 * Config Parameters:
 *
 * <ul>
 *   <li>conditional_fields (List<String>, Optional) : The fields which will be used to determine if
 *       this stage should be applied. Turns off conditional execution by default.
 *   <li>conditional_values (List<String>, Optional) : The values which we will search the
 *       conditional fields for. Should be set iff conditional_fields is set.
 *   <li>conditional_operator (String, Optional) : The operator to determine conditional execution.
 *       Can be 'must' or 'must_not'. Defaults to must.
 * </ul>
 *
 * <p>Config validation:<br>
 * The config validation will happen based on {@link Stage#optionalProperties}, {@link
 * Stage#requiredProperties}, {@link Stage#requiredParents}, and {@link Stage#optionalParents}.
 * Note that all of these properties are stored as sets and are disjoint from each other. As the
 * name suggests {@link Stage#requiredProperties} and {@link Stage#requiredParents} are required to
 * be present in the config while {@link Stage#optionalProperties} and {@link Stage#optionalParents}
 * are optional.
 */
public abstract class Stage {

  private static final Set<String> EMPTY_SET = Collections.emptySet();
  private static final Set<String> CONDITIONS_OPTIONAL = Set.of("operator");
  private static final Set<String> CONDITIONS_REQUIRED = Set.of("fields", "values");
  private static final Set<String> OPTIONAL_PROPERTIES = Set.of("class", "name", "conditions");

  protected Config config;
  private final Predicate<Document> condition;
  private String name;

  private Timer timer;
  private Timer.Context context;
  private Counter errorCounter;
  private Counter childCounter;

  private final Set<String> requiredProperties;
  private final Set<String> optionalProperties;
  private final Set<String> requiredParents;
  private final Set<String> optionalParents;

  public Stage(Config config) {
    this(config, new StageSpec());
  }

  protected Stage(Config config, StageSpec spec) {
    this(config, spec.requiredProperties, spec.optionalProperties, spec.requiredParents, spec.optionalParents);
  }

  private Stage(Config config, Set<String> requiredProperties, Set<String> optionalProperties,
      Set<String> requiredParents, Set<String> optionalParents) {

    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }

    this.name = ConfigUtils.getOrDefault(config, "name", null);

    this.config = config;
    this.requiredParents = Collections.unmodifiableSet(requiredParents);
    this.optionalParents = Collections.unmodifiableSet(optionalParents);
    this.requiredProperties = Collections.unmodifiableSet(requiredProperties);
    this.optionalProperties = mergeSets(OPTIONAL_PROPERTIES, optionalProperties);

    // validates the properties that were just assigned
    validateConfigWithConditions();

    this.condition = getMergedConditions();
  }

  private Predicate<Document> getMergedConditions() {
    Stream<Predicate<Document>> conditions =
        !config.hasPath("conditions")
            ? Stream.empty()
            : config.getConfigList("conditions").stream().map(Condition::fromConfig);
    return conditions.reduce(c -> true, Predicate::and);
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

  private String getClassName() {
    return this.getClass().getName();
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

  private void validateConfigWithConditions() throws IllegalArgumentException {

    validateConfig(config, requiredProperties, optionalProperties, requiredParents, optionalParents);

    // validate conditions
    if (config.hasPath("conditions"))  {
      for (Config condition : config.getConfigList("conditions")) {
        validateConfig(condition, CONDITIONS_REQUIRED, CONDITIONS_OPTIONAL, EMPTY_SET, EMPTY_SET);
      }
    }
  }

  // this can be used in a specific stage to validate nested properties
  protected void validateConfig(
    Config config, Set<String> requiredProperties, Set<String> optionalProperties,
    Set<String> requiredParents, Set<String> optionalParents) {

    // verifies that set intersection is empty
    if (!disjoint(requiredProperties, optionalProperties, requiredParents, optionalParents)) {
      throw new IllegalArgumentException(getClassName()
        + ": Properties and parents sets must be disjoint.");
    }

    // verifies all required properties are present
    Set<String> keys = config.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toSet());
    for (String property: requiredProperties) {
      if (!keys.contains(property)) {
        throw new IllegalArgumentException(getClassName() + ": Stage config must contain property "
          + property);
      }
    }

    // verifies that
    // 1. all remaining properties are in the optional set or are nested;
    // 2. all required parents are present
    Set<String> observedRequiredParents = new HashSet<>();
    Set<String> legalProperties = mergeSets(requiredProperties, optionalProperties);
    for (String key: keys) {
      if (!legalProperties.contains(key)) {
        String parent = getParent(key);
        if (parent == null) {
          throw new IllegalArgumentException(getClassName() + ": Stage config contains unknown property "
            + key);
        } else if (requiredParents.contains(parent)) {
          observedRequiredParents.add(parent);
        } else if (!optionalParents.contains(parent)) {
          throw new IllegalArgumentException(getClassName() + ": Stage config contains unknown property "
            + key);
        }
      }
    }
    if (observedRequiredParents.size() != requiredParents.size()) {
      throw new IllegalArgumentException(getClassName() + ": Stage config is missing required parents: " +
          Sets.difference(requiredParents, observedRequiredParents));
    }
  }

  public Set<String> getLegalProperties() {
    return mergeSets(requiredProperties, optionalProperties);
  }

  private static String getParent(String property) {
    int dotIndex = property.indexOf('.');
    if (dotIndex < 0 ||  dotIndex == property.length() - 1) {
      return null;
    }
    return property.substring(0, dotIndex);
  }

  @SafeVarargs
  private static boolean disjoint(Set<String>... sets) {
    if (sets == null) {
      throw new IllegalArgumentException("Sets must not be null");
    }
    if (sets.length == 0) {
      throw new IllegalArgumentException("expecting at least one set");
    }

    Set<String> observed = new HashSet<>();
    for (Set<String> set : sets) {
      if (set == null) {
        throw new IllegalArgumentException("Each set must not be null");
      }
      for (String s : set) {
        if (observed.contains(s)) {
          return false;
        }
        observed.add(s);
      }
    }
    return true;
  }

  @SafeVarargs
  private static <T> Set<T> mergeSets(Set<T> ... sets) {
    Set<T> merged = new HashSet<>();
    for (Set<T> set : sets) {
      merged.addAll(set);
    }
    return Collections.unmodifiableSet(merged);
  }

  /**
   * This class is used for convenience of initializing the extending stages in the super
   * constructor of {@link Stage}.
   */
  protected static class StageSpec {

    private final Set<String> requiredProperties;
    private final Set<String> optionalProperties;
    private final Set<String> requiredParents;
    private final Set<String> optionalParents;

    public StageSpec() {
      requiredProperties = new HashSet<>();
      optionalProperties = new HashSet<>();
      requiredParents = new HashSet<>();
      optionalParents = new HashSet<>();
    }

    public StageSpec withRequiredProperties(String... properties) {
      requiredProperties.addAll(Arrays.asList(properties));
      return this;
    }

    public StageSpec withOptionalProperties(String... properties) {
      optionalProperties.addAll(Arrays.asList(properties));
      return this;
    }

    public StageSpec withRequiredParents(String... properties) {
      requiredParents.addAll(Arrays.asList(properties));
      return this;
    }

    public StageSpec withOptionalParents(String... properties) {
      optionalParents.addAll(Arrays.asList(properties));
      return this;
    }
  }
}
