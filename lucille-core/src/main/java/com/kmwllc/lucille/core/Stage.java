package com.kmwllc.lucille.core;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.commons.collections4.iterators.IteratorChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * An operation that can be performed on a Document.<br>
 * This abstract class provides some base functionality which should be applicable to all Stages.
 *
 * <p> All implementations must declare a <code>public static Spec SPEC</code> defining the Stage's properties. This Spec will be accessed
 * reflectively in the super constructor, so the Stage will not function without declaring a Spec. The Config provided
 * to <code>super()</code> will be validated against the Spec. Validation errors will reference the Stage's <code>name</code>.
 *
 * <p> A {@link SpecBuilder#stage()} always has "name", "class", "conditions", and "conditionPolicy" as legal properties.
 *
 * <p> Config Parameters (Conditions):
 * <ul>
 *   <li>fields (List&lt;String&gt;, Optional) : The fields which will be used to determine if
 *       this stage should be applied. Turns off conditional execution by default.
 *   <li>values (List&lt;String&gt;, Optional) : The values which we will search the
 *       conditional fields for. If not set, only the existence of fields is checked.
 *   <li>operator (String, Optional) : The operator to determine conditional execution.
 *       Can be 'must' or 'must_not'. Defaults to must.
 * </ul>
 */
public abstract class Stage {
  private static final Logger docLogger = LoggerFactory.getLogger("com.kmwllc.lucille.core.DocLogger");

  protected Config config;
  private final Predicate<Document> condition;
  private String name;

  private Timer timer;
  private Timer.Context context;
  private Counter errorCounter;
  private Counter childCounter;

  /**
   * Creates a Stage without any optional / required properties, other than the default legal properties for all Stages.
   * @param config The configuration for the Stage.
   */
  public Stage(Config config) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }

    this.name = ConfigUtils.getOrDefault(config, "name", null);

    this.config = config;

    // A spec.stage() validates the stage config AND the conditions as well
    getSpec().validate(config, getDisplayName());

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

  public Spec getSpec() {
    try {
      return (Spec) this.getClass().getDeclaredField("SPEC").get(null);
    } catch (Exception e) {
      throw new RuntimeException("Error accessing " + getClass() + " Spec. Is it publicly and statically available under \"SPEC\"?", e);
    }
  }

  /**
   * Starts this stage, performing any setup if needed.
   * @throws StageException If an error occurs.
   */
  public void start() throws StageException {
  }

  /**
   * Stops this stage, freeing any resources / closing any connections as needed.
   * @throws StageException If an error occurs.
   */
  public void stop() throws StageException {
  }

  /**
   * Log metrics relating to the stage's performance, if metrics have been initialized.
   */
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
    if (shouldProcess(doc)) {
      if (timer != null) {
        context = timer.time();
      }
      try {
        docLogger.info("Stage {} to process {}.", name, doc.getId());
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
   *
   * @param docs The iterator of documents you want to wrap around.
   * @return An iterator of documents, wrapping around the given iterator, calling apply on each as they are returned.
   * @throws StageException in the event an error occurs.
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

  /**
   * Gets the name of this stage without any formatting / changes.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the class name plus the stage name in parentheses, if it is not null.
   * @return A formatted version of this stage's name.
   */
  private String getDisplayName() {
    return this.getClass().getName() +
        ((this.getName() == null) ? "" : " (" + this.getName() + ")");
  }

  /**
   * Initialize metrics and set the Stage's name based on the position if the name has not already been set.
   * @param position The position of the stage.
   * @param metricsPrefix The metricsPrefix associated with this stage.
   * @throws StageException In the event of an error.
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

  /**
   * Gets this stage's config.
   * @return This stage's config.
   */
  public Config getConfig() {
    return config;
  }

  public Set<String> getLegalProperties() {
    return getSpec().getLegalProperties();
  }

  /**
   * Construct a Stage from a config that includes a clas property.
   */
  public static Stage fromConfig(Config config) throws Exception {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }
    if (!config.hasPath("class")) {
      throw new IllegalArgumentException("No Stage class specified");
    }

    String className = config.getString("class");
    Class<?> raw = Class.forName(className);
    if (!Stage.class.isAssignableFrom(raw)) {
      throw new IllegalArgumentException("Class " + className + " is not a Stage");
    }

    Class<? extends Stage> cls = (Class<? extends Stage>) raw;
    Constructor<? extends Stage> constructor = cls.getConstructor(Config.class);

    return newInstance(constructor, config);
  }

  private static Stage newInstance(Constructor<? extends Stage> constructor, Config config) throws Exception {
    try {
      return constructor.newInstance(config);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof Exception) {
        throw (Exception) e.getCause();
      } else {
        throw e;
      }
    }
  }
}
