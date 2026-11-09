package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.util.LogUtils;

/**
 * What the framework provides to an {@link Indexer} when it is constructed, apart from the Indexer's own config. Pass it
 * through to {@link Indexer#Indexer(com.typesafe.config.Config, IndexerContext)}.
 *
 * @param messenger The messenger the Indexer polls for documents and sends events to.
 * @param bypass Whether the Indexer should not contact its destination (test mode, or indexer.sendEnabled: false).
 * @param metricsPrefix The prefix for the names of the Indexer's metrics.
 * @param localRunId The run ID for a local run, null otherwise.
 * @param logSeconds How often the Indexer logs its status, from log.intervalSecs.
 */
public record IndexerContext(IndexerMessenger messenger, boolean bypass, String metricsPrefix, String localRunId, int logSeconds) {

  /**
   * Creates a context that uses the default interval for status logging.
   */
  public IndexerContext(IndexerMessenger messenger, boolean bypass, String metricsPrefix, String localRunId) {
    this(messenger, bypass, metricsPrefix, localRunId, LogUtils.DEFAULT_LOG_SECONDS);
  }
}
