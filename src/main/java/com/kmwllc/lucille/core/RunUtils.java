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
 */
public class RunUtils {

  public static void main(String[] args) throws Exception {
    runWithKafka("application.conf");
  }

  public static PersistingLocalMessageManager runLocal(String configName) throws Exception {

    // create persisting message manager that saves all message traffic so we can review it
    // even after various simulated topics have been fully consumed/cleared
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();

    // acquire the config from the classpath
    Config config = ConfigFactory.load(configName);

    // spawn a Worker thread and an Indexer thread that use the local message manager
    Worker worker = Worker.startThread(config, manager);
    Indexer indexer = Indexer.startThread(config, manager);

    // create a Runner and run the connectors
    Runner runner = new RunnerImpl(config);
    Publisher publisher = new PublisherImpl(runner.getRunId(), manager);
    runner.runConnectors(publisher);

    // stop the Worker and Indexer threads
    worker.terminate();
    indexer.terminate();

    return manager;
  }

  public static void runWithKafka(String configName) throws Exception {
    // acquire the config from the classpath
    Config config = ConfigFactory.load(configName);

    // spawn a Worker thread and an Indexer thread that use Kafka-based message managers
    WorkerMessageManager workerMessageManager = new KafkaWorkerMessageManager(config);
    Worker worker = Worker.startThread(config, workerMessageManager);
    IndexerMessageManager indexerMessageManager = new KafkaIndexerMessageManager(config);
    Indexer indexer = Indexer.startThread(config, indexerMessageManager);

    // create a Runner and run the connectors
    Runner runner = new RunnerImpl(config);
    PublisherMessageManager publisherMessageManager = new KafkaPublisherMessageManager(config, runner.getRunId());
    Publisher publisher = new PublisherImpl(runner.getRunId(), publisherMessageManager);
    runner.runConnectors(publisher);

    // stop the Worker and Indexer threads
    worker.terminate();
    indexer.terminate();
  }

}
