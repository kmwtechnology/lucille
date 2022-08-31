package com.kmwllc.lucille.core;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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

  public Stage(Config config) {
    this.config = config;
    this.name = ConfigUtils.getOrDefault(config, "name", null);
    List<Predicate<Document>> conditions = config.hasPath("conditions") ?
        config.getConfigList("conditions").stream().map(Condition::fromConfig).collect(Collectors.toList())
        : new ArrayList<>();
    this.condition = conditions.stream().reduce((d) -> true, Predicate::and);

    // todo check this
    validateConfig();
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

  public abstract Set<String> getPropertyList();

  public void validateConfig() {
    // todo keep empty for now?
  }

}
