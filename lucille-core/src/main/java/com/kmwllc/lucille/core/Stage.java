package com.kmwllc.lucille.core;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.collect.Sets;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import org.apache.commons.collections4.iterators.IteratorChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * An operation that can be performed on a Document.<br>
 * This abstract class provides some base functionality which should be applicable to all Stages.
 * <br>
 * <br>
 * Config Parameters:
 *
 * <ul>
 *   <li>conditional_fields (List&lt;String&gt;, Optional) : The fields which will be used to determine if
 *       this stage should be applied. Turns off conditional execution by default.
 *   <li>conditional_values (List&lt;String&gt;, Optional) : The values which we will search the
 *       conditional fields for. If not set, only the existence of fields is checked.
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
  private static final Logger docLogger = LoggerFactory.getLogger("com.kmwllc.lucille.core.DocLogger");

  private static final Set<String> EMPTY_SET = Collections.emptySet();
  private static final Set<String> CONDITIONS_OPTIONAL = Set.of("operator", "values");
  private static final Set<String> CONDITIONS_REQUIRED = Set.of("fields");
  private static final Set<String> OPTIONAL_PROPERTIES = Set.of("class", "name", "conditions", "conditionPolicy");

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
    try {
      validateConfigWithConditions();
    } catch (StageException e) {
      throw new IllegalArgumentException(e);
    }

    this.condition = getMergedConditions();
  }

  private Predicate<Document> getMergedConditions() {
    List<Predicate<Document>> conditions =
        !config.hasPath("conditions")
            ? Collections.emptyList()
            : config.getConfigList("conditions").stream()
                .map(Condition::fromConfig)
                .collect(Collectors.toUnmodifiableList());

    if (conditions.isEmpty()) {
      return (x -> true);
    }

    String conditionPolicy = config.hasPath("conditionPolicy") ? config.getString("conditionPolicy") : "all";

    if ("any".equalsIgnoreCase(conditionPolicy)) {
      return conditions.stream().reduce(c -> false, Predicate::or);
    } else if ("all".equalsIgnoreCase(conditionPolicy)) {
      return conditions.stream().reduce(c -> true, Predicate::and);
    } else {
      throw new IllegalArgumentException("Unsupported condition policy: " + conditionPolicy);
    }
  }

  public void start() throws StageException {
  }

  public void stop() throws StageException {
  }

  public void logMetrics() {
    if (timer == null || childCounter == null || errorCounter == null) {
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
   * @return boolean representing - should we process this doc according to its conditionals?
   */
  public boolean shouldProcess(Document doc) {
    if (doc.isDropped()) {
      return false;
    }
    return condition.test(doc);
  }

  /**
   * Process this Document iff it adheres to our conditional requirements.
   *
   * @param doc the Document
   * @return a list of child documents resulting from this Stages processing
   * @throws StageException
   */
  public Iterator<Document> processConditional(Document doc) throws StageException {
    docLogger.info("Stage {} may process {}.", name, doc.getId());

    if (shouldProcess(doc)) {
      if (timer != null) {
        context = timer.time();
      }
      try {
        return processDocument(doc);
      } finally {
        if (context != null) {
          // this only tracks the time taken to process the input document and
          // return the iterator over any children; it doesn't track the time taken
          // to actually generate the children documents by exhausting the returned iterator
          context.stop();
        }

        docLogger.info("Stage {} done processing {}.", name, doc.getId());
      }
    }

    docLogger.info("Stage {} did not process {}.", name, doc.getId());
    return null;
  }

  /**
   * Applies an operation to a Document in place and returns an Iterator over any child Documents generated
   * by the operation, not including the parent. If no child Documents are generated, the return value should be null.
   */
  public abstract Iterator<Document> processDocument(Document doc) throws StageException;

  /**
   * Applies an operation to a Document in place and returns an Iterator over any child Documents generated
   * by the operation, with the input or parent document at the end. Unlike processDocument, the return
   * value will always be non-null and will at least contain the input document.
   * If the input document has a run ID, this ID will be copied to any children that do not have it.
   */
  public Iterator<Document> apply(Document doc) throws StageException {

    Iterator<Document> children = processConditional(doc);
    Iterator<Document> parent = doc.iterator();

    if (children == null) {
      return parent;
    }
    String runId = doc.getRunId();

    Iterator<Document> wrappedChildren = new Iterator<>() {

      @Override
      public boolean hasNext() {
        return children.hasNext();
      }

      @Override
      public Document next() {
        Document child = children.next();

        if (child != null) {
          if (childCounter != null) {
            childCounter.inc();
          }

          // copy the parent's RunID to the child
          // TODO: copy the parent's ID as well and store it as parentID on the child
          if ((runId != null) && !child.has(Document.RUNID_FIELD)) {
            child.initializeRunId(runId);
          }
        }

        return child;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };

    return new IteratorChain(wrappedChildren, parent);
  }

  /**
   * Wraps an Iterator over Documents so as to call apply(doc) on each doc in the sequence.
   */
  public Iterator<Document> apply(Iterator<Document> docs) throws StageException {

    return new Iterator<>() {

      Iterator<Document> current = null;

      @Override
      public boolean hasNext() {
        return (current != null && current.hasNext()) || docs.hasNext();
      }

      @Override
      public Document next() {

        if (current != null && current.hasNext()) {
          return current.next();
        }

        Document d = docs.next();
        if (d == null) {
          return null;
        }

        try {
          current = apply(d);
        } catch (StageException e) {
          if (errorCounter != null) {
            errorCounter.inc();
          }

          throw new RuntimeException(e); // TODO
        }

        if (current.hasNext()) {
          return current.next();
        }

        return null;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }


  public String getName() {
    return name;
  }

  /**
   * Returns the class name plus the stage name in parentheses, if it is not null.
   */
  private String getDisplayName() {
    return this.getClass().getName() +
        ((this.getName() == null) ? "" : " (" + this.getName() + ")");
  }

  /**
   * Initialize metrics and set the Stage's name based on the position if the name has not already been set.
   */
  public void initialize(int position, String metricsPrefix) throws StageException {
    if (name == null) {
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

  public void validateConfigWithConditions() throws StageException {

    try {
      validateConfig(config, requiredProperties, optionalProperties, requiredParents, optionalParents);

      // validate conditions
      if (config.hasPath("conditions")) {
        for (Config condition : config.getConfigList("conditions")) {
          validateConfig(condition, CONDITIONS_REQUIRED, CONDITIONS_OPTIONAL, EMPTY_SET, EMPTY_SET);
        }
      }
    } catch (IllegalArgumentException e) {
      throw new StageException(e.getMessage());
    }
  }

  // this can be used in a specific stage to validate nested properties
  protected void validateConfig(
      Config config, Set<String> requiredProperties, Set<String> optionalProperties,
      Set<String> requiredParents, Set<String> optionalParents) {

    // verifies that set intersection is empty
    if (!disjoint(requiredProperties, optionalProperties, requiredParents, optionalParents)) {
      throw new IllegalArgumentException(getDisplayName()
          + ": Properties and parents sets must be disjoint.");
    }

    // verifies all required properties are present
    Set<String> keys = config.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toSet());
    for (String property : requiredProperties) {
      if (!keys.contains(property)) {
        throw new IllegalArgumentException(getDisplayName() + ": Stage config must contain property "
            + property);
      }
    }

    // verifies that
    // 1. all remaining properties are in the optional set or are nested;
    // 2. all required parents are present
    Set<String> observedRequiredParents = new HashSet<>();
    Set<String> legalProperties = mergeSets(requiredProperties, optionalProperties);
    for (String key : keys) {
      if (!legalProperties.contains(key)) {
        String parent = getParent(key);
        if (parent == null) {
          throw new IllegalArgumentException(getDisplayName() + ": Stage config contains unknown property "
              + key);
        } else if (requiredParents.contains(parent)) {
          observedRequiredParents.add(parent);
        } else if (!optionalParents.contains(parent)) {
          throw new IllegalArgumentException(getDisplayName() + ": Stage config contains unknown property "
              + key);
        }
      }
    }
    if (observedRequiredParents.size() != requiredParents.size()) {
      throw new IllegalArgumentException(getDisplayName() + ": Stage config is missing required parents: " +
          Sets.difference(requiredParents, observedRequiredParents));
    }
  }

  public Set<String> getLegalProperties() {
    return mergeSets(requiredProperties, optionalProperties);
  }

  private static String getParent(String property) {
    int dotIndex = property.indexOf('.');
    if (dotIndex < 0 || dotIndex == property.length() - 1) {
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
  private static <T> Set<T> mergeSets(Set<T>... sets) {
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
