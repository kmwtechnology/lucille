package com.kmwllc.lucille.core;

import com.kmwllc.lucille.indexer.SolrIndexer;
import com.kmwllc.lucille.message.HybridIndexerMessageManager;
import com.kmwllc.lucille.message.HybridWorkerMessageManager;
import com.kmwllc.lucille.message.LocalMessageManager;
import com.typesafe.config.Config;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Provides a way to launch a Worker-Indexer pair, where:
 *  1) the Worker reads documents from a source kafka topic
 *  2) the Worker publishes documents to an in-memory queue
 *  3) the Indexer retrieves documents from an in-memory queue
 *  4) the Indexer adds offsets of indexed documents to an in-memory hashmap
 *  5) the Worker reads offsets from the in-memory hashmap and commits them
 *  6) callbacks are disabled
 */
public class WorkerIndexer {

  private static final Logger log = LoggerFactory.getLogger(WorkerIndexer.class);

  public static void main(String[] args) throws Exception {
    Config config = ConfigUtils.loadConfig();
    String pipelineName = args.length > 0 ? args[0] : config.getString("worker.pipeline");
    log.debug("Starting WorkerIndexer for pipeline: " + pipelineName);

    // TODO: start a pool of worker-indexer pairs instead of just one pair

    LinkedBlockingQueue<KafkaDocument> pipelineDest =
      new LinkedBlockingQueue<>(LocalMessageManager.DEFAULT_QUEUE_CAPACITY);
    LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets =
      new LinkedBlockingQueue<>();

    HybridWorkerMessageManager workerMessageManager =
      new HybridWorkerMessageManager(config, pipelineName, pipelineDest, offsets);

    HybridIndexerMessageManager indexerMessageManager =
      new HybridIndexerMessageManager(pipelineDest, offsets);

    WorkerThread workerThread =
      Worker.startThread(config, workerMessageManager, pipelineName,"workerprefix");

    Indexer indexer = new SolrIndexer(config, indexerMessageManager, false, pipelineName);
    if (!indexer.validateConnection()) {
      log.error("Indexer could not connect");
      System.exit(1);
    }

    Thread indexerThread = new Thread(indexer);
    indexerThread.start();

    // TODO: add shutdown hook
  }

}
