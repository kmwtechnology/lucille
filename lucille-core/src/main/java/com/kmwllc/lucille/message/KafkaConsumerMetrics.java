package com.kmwllc.lucille.message;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.kmwllc.lucille.util.LogUtils;

/**
 * The poll and commit metrics of a Kafka consumer that reads documents: a Worker's source consumer (in distributed and
 * hybrid mode) or the standalone Indexer's destination consumer. Used by every Kafka messenger, so the metrics mean the
 * same thing in each mode.
 *
 * <p> The metrics are registered in the shared registry ({@link LogUtils#METRICS_REG}) under the pipeline name followed
 * by one of the suffixes below. Every consumer of the same kind for a pipeline in one JVM shares
 * them, so each value covers all of those consumers.
 * <ul>
 *   <li>{@code <pipeline>.kafka.worker.pollTime}, {@code <pipeline>.kafka.indexer.pollTime} (Timer): time spent in each
 *   poll, including polls that throw. A poll that finds nothing waits for the poll interval, so an idle consumer shows
 *   poll times close to that interval. A rebalance runs inside a poll, so a poll during one includes rejoining the
 *   group and the work done on revocation.</li>
 *   <li>{@code <pipeline>.kafka.worker.commitTime}, {@code <pipeline>.kafka.indexer.commitTime} (Timer): time spent in
 *   each offset commit sent to the broker, including commits that throw and commits made when partitions are revoked.
 *   Calls with nothing to commit are not timed.</li>
 *   <li>{@code <pipeline>.kafka.worker.commitFailures}, {@code <pipeline>.kafka.indexer.commitFailures} (Meter): offset
 *   commits that threw. A failed commit that is retried and fails again is counted again.</li>
 * </ul>
 */
public final class KafkaConsumerMetrics {

  public static final String WORKER_POLL_TIME_SUFFIX = ".kafka.worker.pollTime";
  public static final String WORKER_COMMIT_TIME_SUFFIX = ".kafka.worker.commitTime";
  public static final String WORKER_COMMIT_FAILURES_SUFFIX = ".kafka.worker.commitFailures";
  public static final String INDEXER_POLL_TIME_SUFFIX = ".kafka.indexer.pollTime";
  public static final String INDEXER_COMMIT_TIME_SUFFIX = ".kafka.indexer.commitTime";
  public static final String INDEXER_COMMIT_FAILURES_SUFFIX = ".kafka.indexer.commitFailures";

  private final Timer pollTimer;
  private final Timer commitTimer;
  private final Meter commitFailures;

  private KafkaConsumerMetrics(String prefix, String pollSuffix, String commitSuffix, String failuresSuffix) {
    // looked up through the registry, not constructed, so that a registry an application has installed under this name
    // supplies its own Timer and Meter implementations
    MetricRegistry metrics = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);
    this.pollTimer = metrics.timer(prefix + pollSuffix);
    this.commitTimer = metrics.timer(prefix + commitSuffix);
    this.commitFailures = metrics.meter(prefix + failuresSuffix);
  }

  /** The metrics of the Worker's source consumer for the given pipeline, registering them if they are not yet. */
  public static KafkaConsumerMetrics forWorker(String pipelineName) {
    return new KafkaConsumerMetrics(pipelineName, WORKER_POLL_TIME_SUFFIX, WORKER_COMMIT_TIME_SUFFIX,
        WORKER_COMMIT_FAILURES_SUFFIX);
  }

  /** The metrics of the standalone Indexer's destination consumer for the given pipeline, registering them if they are not yet. */
  public static KafkaConsumerMetrics forIndexer(String pipelineName) {
    return new KafkaConsumerMetrics(pipelineName, INDEXER_POLL_TIME_SUFFIX, INDEXER_COMMIT_TIME_SUFFIX,
        INDEXER_COMMIT_FAILURES_SUFFIX);
  }

  /**
   * A one-sentence summary of the Worker consumer's metrics for the given pipeline, for a status log line, or an empty
   * string if no Kafka worker messenger has registered them in this JVM (as in local mode).
   */
  public static String workerSummary(String pipelineName) {
    return summary(pipelineName, WORKER_POLL_TIME_SUFFIX, WORKER_COMMIT_TIME_SUFFIX, WORKER_COMMIT_FAILURES_SUFFIX);
  }

  /**
   * A one-sentence summary of the Indexer consumer's metrics for the given pipeline, for a status log line, or an empty
   * string if no Kafka indexer messenger has registered them in this JVM (as in local and hybrid mode).
   */
  public static String indexerSummary(String pipelineName) {
    return summary(pipelineName, INDEXER_POLL_TIME_SUFFIX, INDEXER_COMMIT_TIME_SUFFIX, INDEXER_COMMIT_FAILURES_SUFFIX);
  }

  private static String summary(String prefix, String pollSuffix, String commitSuffix, String failuresSuffix) {
    // read without registering, so that asking does not create metrics in a JVM that has no Kafka consumer
    MetricRegistry metrics = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);
    Timer poll = metrics.getTimers().get(prefix + pollSuffix);
    Timer commit = metrics.getTimers().get(prefix + commitSuffix);
    Meter failures = metrics.getMeters().get(prefix + failuresSuffix);
    if (poll == null || commit == null || failures == null) {
      return "";
    }
    return String.format("Kafka: %d polls, mean %.2f ms; %d commits, mean %.2f ms, %d failed.",
        poll.getCount(), poll.getSnapshot().getMean() / 1000000,
        commit.getCount(), commit.getSnapshot().getMean() / 1000000, failures.getCount());
  }

  /** Starts timing a poll. Close the returned context when the poll returns or throws. */
  Timer.Context timePoll() {
    return pollTimer.time();
  }

  /** Runs the given offset commit, timing it. If it throws, marks the failure meter and rethrows. */
  void commit(Runnable commit) {
    try (Timer.Context ignored = commitTimer.time()) {
      commit.run();
    } catch (RuntimeException e) {
      commitFailures.mark();
      throw e;
    }
  }
}
