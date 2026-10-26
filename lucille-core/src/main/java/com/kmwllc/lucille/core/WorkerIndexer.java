package com.kmwllc.lucille.core;

import com.kmwllc.lucille.indexer.IndexerFactory;
import com.kmwllc.lucille.message.HybridIndexerMessenger;
import com.kmwllc.lucille.message.HybridWorkerMessenger;
import com.kmwllc.lucille.message.LocalMessenger;
import com.kmwllc.lucille.message.WorkerMessenger;
import com.kmwllc.lucille.util.ThreadNameUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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
 *  The Worker's messenger is a {@link HybridWorkerMessenger} unless <code>worker.messenger.class</code> names another
 *  {@link WorkerMessenger} implementation. See {@link #createWorkerMessenger}.
 *
 *  TODO: document why pairing of one worker with one indexer is necessary for hybrid mode
 */
public class WorkerIndexer {

  private static final Logger log = LoggerFactory.getLogger(WorkerIndexer.class);

  private Indexer indexer;
  private Thread indexerThread;
  private WorkerThread workerThread;

  public static void main(String[] args) throws Exception {
    Config config = ConfigFactory.load();
    String pipelineName = args.length > 0 ? args[0] : config.getString("worker.pipeline");
    WorkerIndexerPool pool = new WorkerIndexerPool(config, pipelineName, false, null);
    pool.start();

    Signal.handle(new Signal("INT"), signal -> {
      log.info("Workers shutting down");
      try {
        // stopping the WorkerIndexer should close the kafka client connection
        // via workerThread.terminate() -> worker.terminate() which causes
        // the worker's run method to break out of its while loop and
        // eventually call messenger.close()
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

    WorkerMessenger workerMessageManager = createWorkerMessenger(config, pipelineName, pipelineDest, offsets);

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

  /**
   * Creates the WorkerMessenger for a WorkerIndexer: a {@link HybridWorkerMessenger}, or an instance of the class named
   * by <code>worker.messenger.class</code> when that is set.
   *
   * <p>A configured class must implement {@link WorkerMessenger} and have a public constructor with the same arguments
   * as {@link HybridWorkerMessenger#HybridWorkerMessenger(Config, String, LinkedBlockingQueue, LinkedBlockingQueue)}:
   * <code>(Config config, String pipelineName, LinkedBlockingQueue&lt;Document&gt; pipelineDest,
   * LinkedBlockingQueue&lt;Map&lt;TopicPartition, OffsetAndMetadata&gt;&gt; offsets)</code>. It must send processed
   * documents to <code>pipelineDest</code> and commit the offsets the Indexer adds to <code>offsets</code>. Subclassing
   * HybridWorkerMessenger is the simplest way to change one part of its behaviour and keep the rest.
   *
   * @param config The Lucille config.
   * @param pipelineName The name of the pipeline the Worker runs.
   * @param pipelineDest The in-memory queue the Indexer reads processed documents from.
   * @param offsets The in-memory queue the Indexer adds the offsets of handled documents to.
   * @return The WorkerMessenger to use.
   * @throws IllegalArgumentException If <code>worker.messenger.class</code> can't be loaded, doesn't implement
   * WorkerMessenger, or doesn't have the required constructor.
   * @throws Exception If the messenger's constructor throws.
   */
  static WorkerMessenger createWorkerMessenger(Config config, String pipelineName,
      LinkedBlockingQueue<Document> pipelineDest,
      LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets) throws Exception {
    if (!config.hasPath("worker.messenger.class")) {
      return new HybridWorkerMessenger(config, pipelineName, pipelineDest, offsets);
    }

    String className = config.getString("worker.messenger.class");
    Constructor<?> constructor;
    try {
      Class<?> clazz = Class.forName(className);
      if (!WorkerMessenger.class.isAssignableFrom(clazz)) {
        throw new IllegalArgumentException("worker.messenger.class '" + className + "' does not implement "
            + WorkerMessenger.class.getName());
      }
      constructor = clazz.getConstructor(Config.class, String.class, LinkedBlockingQueue.class, LinkedBlockingQueue.class);
    } catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException("Problem initializing worker.messenger.class configuration of: '" + className + "'", e);
    }

    log.info("Using worker.messenger.class: " + className);
    try {
      return (WorkerMessenger) constructor.newInstance(config, pipelineName, pipelineDest, offsets);
    } catch (InvocationTargetException e) {
      // surface what the messenger's constructor threw, e.g. a Kafka configuration problem
      throw e.getCause() instanceof Exception ? (Exception) e.getCause() : e;
    }
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
