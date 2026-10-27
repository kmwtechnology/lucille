package com.kmwllc.lucille.core;

import com.kmwllc.lucille.indexer.IndexerFactory;
import com.kmwllc.lucille.message.HybridIndexerMessenger;
import com.kmwllc.lucille.message.HybridWorkerMessenger;
import com.kmwllc.lucille.message.LocalMessenger;
import com.kmwllc.lucille.util.ThreadNameUtils;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Provides a way to launch a Worker-Indexer pair, where:
 *  1) the Worker reads documents from a source kafka topic
 *  2) the Worker publishes documents to an in-memory queue
 *  3) the Indexer retrieves documents from an in-memory queue
 *  4) the Indexer adds offsets of indexed documents to an in-memory queue
 *  5) the Worker reads offsets from the in-memory queue and commits them
 *  6) callbacks are disabled
 *
 *  TODO: document why pairing of one worker with one indexer is necessary for hybrid mode
 */
public class WorkerIndexer {

  private static final Logger log = LoggerFactory.getLogger(WorkerIndexer.class);

  private Indexer indexer;
  private Thread indexerThread;
  private WorkerThread workerThread;

  /**
   * Starts paired Worker and Indexer threads for the pipeline named by the first argument (or by
   * <code>worker.pipeline</code>). The process runs until it is told to stop (SIGINT or SIGTERM); if a thread stops on
   * its own, the rest are stopped and the process exits 1. The body of this entry point is
   * {@link Launch#workerIndexer}.
   */
  public static void main(String[] args) throws Exception {
    Launch.workerIndexer(args);
  }

  /**
   * @return Whether both the worker thread and the indexer thread are still running.
   */
  public boolean isAlive() {
    return workerThread != null && workerThread.isAlive() && indexerThread != null && indexerThread.isAlive();
  }


  public void start(Config config, String pipelineName, boolean bypassSearchEngine, Set<String> idSet) throws Exception {

    // TODO: make queue capacity configurable
    LinkedBlockingQueue<Document> pipelineDest =
        new LinkedBlockingQueue<>(LocalMessenger.DEFAULT_QUEUE_CAPACITY);

    // TODO: should we place a capacity on the offsets queue?
    LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets =
        new LinkedBlockingQueue<>();

    start(config, pipelineName, pipelineDest, offsets, bypassSearchEngine, idSet);
  }

  public void start(Config config, String pipelineName, LinkedBlockingQueue<Document> pipelineDest,
      LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets,
      boolean bypassSearchEngine,
      Set<String> idSet) throws Exception {

    log.info("Starting WorkerIndexer for pipeline: " + pipelineName);

    HybridWorkerMessenger workerMessageManager =
        new HybridWorkerMessenger(config, pipelineName, pipelineDest, offsets);

    HybridIndexerMessenger indexerMessageManager =
        new HybridIndexerMessenger(config, pipelineDest, offsets, idSet, pipelineName);

    indexer = IndexerFactory.fromConfig(config, indexerMessageManager, bypassSearchEngine, pipelineName, null);

    if (!bypassSearchEngine && !indexer.validateConnection()) {
      throw new IndexerException("Indexer could not connect");
    }

    indexerThread = new Thread(indexer);
    indexerThread.start();
    String name = ThreadNameUtils.createName("WorkerIndexer");
    // Since Kafka is in use, we do not provide (or even have for that matter) a runId to directly provide to Worker.
    Worker worker = new Worker(config, workerMessageManager, null, pipelineName, pipelineName);
    workerThread = Worker.startThread(worker, name);
  }

  public void stop() throws Exception {
    // it is important to terminate the indexer wait for its thread to stop
    // before we terminate the worker. This allows the worker to process
    // any offsets that the indexer added to the offset queue upon termination

    if (indexer != null) {
      indexer.terminate();
      log.info("Indexer shutting down");
      try {
        indexerThread.join();
      } catch (InterruptedException e) {
        log.error("Interrupted", e);
      }
    }

    if (workerThread != null) {
      workerThread.terminate();
      try {
        workerThread.join();
      } catch (InterruptedException e) {
        log.error("Interrupted", e);
      }
    }
  }

  public Indexer getIndexer() {
    return indexer;
  }

  public WorkerThread getWorker() {
    return workerThread;
  }


}
