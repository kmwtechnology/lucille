package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Utilities for executing end-to-end runs of Lucille in a self-contained way.
 *
 * In a distributed deployment, there would be one or more Worker processes and one or more Indexer processes
 * that stay alive indefinitely and constantly poll for work. A Runner could then be launched to execute
 * a sequence of connectors which pump work into the system. The work would then be handled by the
 * existing Worker and Indexer and the Runner would terminate when all the work was complete.
 *
 * For development and testing purposes it is useful to have a way to execute an end-to-end run of a system
 * without requiring that the Worker and Indexer have been started as separate processes.
 * RunUtils provides a way to launch the Worker and Indexer as threads, then invoke the Runner, and then
 * stop the Worker and Indexer threads. This can be done in a fully local mode, where all message traffic
 * is stored in in-memory queues, or in Kafka mode, where message traffic flows through an external
 * deployment of Kafka.
 *
 * A local end-to-end run involves a total of 4 threads:
 *
 * 1) a Worker thread polls for documents to process and runs them through the pipeline
 * 2) an Indexer thread polls for processed documents and indexes them
 * 3) a Connector thread reads data from a source, generates documents, and publishes them;
 *    Connectors are run sequentially so there will be at most any Connector thread at any time
 * 4) the main thread uses the Publisher to poll for Events and wait for completion of the run
 */
public class RunUtils {

  public static void main(String[] args) throws Exception {
    runWithKafka("application.conf");
  }

  public static PersistingLocalMessageManager runLocal(String configName) throws Exception {
    Config config = ConfigFactory.load(configName);

    // create persisting message manager that saves all message traffic so we can review it
    // even after various simulated topics have been fully consumed/cleared;
    // this implements Worker/Indexer/PublisherMessageManager so it can be passed
    // to all three components
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();

    run(config, manager, manager, manager);

    return manager;
  }

  public static void runWithKafka(String configName) throws Exception {
    Config config = ConfigFactory.load(configName);
    WorkerMessageManager workerMessageManager = new KafkaWorkerMessageManager(config);
    IndexerMessageManager indexerMessageManager = new KafkaIndexerMessageManager(config);
    PublisherMessageManager publisherMessageManager = new KafkaPublisherMessageManager(config);
    run(config, workerMessageManager, indexerMessageManager, publisherMessageManager);
  }

  private static void run(Config config, WorkerMessageManager workerMessageManager,
                          IndexerMessageManager indexerMessageManager,
                          PublisherMessageManager publisherMessageManager) throws Exception {

    Worker worker = Worker.startThread(config, workerMessageManager);
    Indexer indexer = Indexer.startThread(config, indexerMessageManager);
    Runner runner = new RunnerImpl(config);
    Publisher publisher = new PublisherImpl(publisherMessageManager);
    runner.runConnectors(publisher);
    worker.terminate();
    indexer.terminate();
  }

}
