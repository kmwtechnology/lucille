package com.kmwllc.lucille.core;

import com.kmwllc.lucille.indexer.IndexerFactory;
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

  public static void main(String[] args) throws Exception {
    Config config = ConfigUtils.loadConfig();
    String pipelineName = args.length > 0 ? args[0] : config.getString("worker.pipeline");
    WorkerIndexerPool pool = new WorkerIndexerPool(config, pipelineName, false, null);
    pool.start();

    Signal.handle(new Signal("INT"), signal -> {
      log.info("Workers shutting down");
      try {
        // stopping the WorkerIndexer should close the kafka client connection
        // via workerThread.terminate() -> worker.terminate() which causes
        // the worker's run method to break out of its while loop and
        // eventually call manager.close()
        pool.stop();
      } catch (Exception e) {
        log.error("Error stopping WorkerIndexer", e);
        System.exit(1);
      }
      System.exit(0);
    });
  }


  public void start(Config config, String pipelineName, boolean bypassSearchEngine, Set<String> idSet) throws Exception {

    // TODO: make queue capacity configurable
    LinkedBlockingQueue<Document> pipelineDest =
      new LinkedBlockingQueue<>(LocalMessageManager.DEFAULT_QUEUE_CAPACITY);

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

    HybridWorkerMessageManager workerMessageManager =
      new HybridWorkerMessageManager(config, pipelineName, pipelineDest, offsets);

    HybridIndexerMessageManager indexerMessageManager =
      new HybridIndexerMessageManager(pipelineDest, offsets, idSet);

    indexer = IndexerFactory.fromConfig(config, indexerMessageManager, bypassSearchEngine, pipelineName);

    if (!bypassSearchEngine && !indexer.validateConnection()) {
      throw new IndexerException("Indexer could not connect");
    }

    indexerThread = new Thread(indexer);
    indexerThread.start();

    workerThread =
      Worker.startThread(config, workerMessageManager, pipelineName, pipelineName);
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
