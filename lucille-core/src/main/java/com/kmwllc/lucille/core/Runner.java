package com.kmwllc.lucille.core;

import static com.kmwllc.lucille.core.Document.RUNID_FIELD;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Slf4jReporter;
import com.kmwllc.lucille.indexer.IndexerFactory;
import com.kmwllc.lucille.message.*;
import com.kmwllc.lucille.util.LogUtils;
import com.kmwllc.lucille.util.ThreadNameUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.slf4j.MDC;

/**
 * Executes a Lucille run. A run is a sequential execution of one or more Connectors.
 * During a run, all the work generated by one Connector must be complete before the next Connector begins.
 * A run should stop if any Connector fails.
 * A Connector is considered to have failed if any of its lifecycle methods throw an Exception.
 * Importantly, a connector is NOT considered to have failed if one or more of the documents it publishes
 * encountere an error during pipeline exeuction or indexing.
 * <p>
 * In a distributed deployment, there will be one or more Worker processes and one or more Indexer processes
 * that stay alive indefinitely and constantly poll for work. A Runner can then execute
 * a sequence of connectors which pump work into the system. The work from this particular "run" should
 * then be handled by the existing Worker and Indexer. The Runner will terminate when all the work
 * it generated was complete, while the Worker and Indexer would stay alive.
 * <p>
 * For simpler deployments, Runner provides a way to execute an end-to-end run
 * without requiring that the Worker and Indexer have been started as separate processes.
 * Instead, the Worker(s) and Indexer are launched as threads in the Runner's own JVM.
 * This can be done in a fully local mode, where all message traffic
 * is stored in in-memory queues, or in Kafka mode, where message traffic flows through an external
 * deployment of Kafka.
 * <p>
 * A local end-to-end run involves a total of 4 threads for each connector:
 * <p>
 * 1) a Worker thread polls for documents to process and runs them through the pipeline
 * 2) an Indexer thread polls for processed documents and indexes them
 * 3) a Connector thread reads data from a source, generates documents, and publishes them;
 * Connectors are run sequentially so there will be at most one Connector thread at any time
 * 4) the main thread launches the other threads, and then uses the Publisher to poll for Events
 * and wait for completion of the run
 */
public class Runner {

  public static final int DEFAULT_CONNECTOR_TIMEOUT = 1000 * 60 * 60 * 24;

  public static final long DEFAULT_WORKER_INDEXER_JOIN_TIMEOUT = 3000;

  private static final Logger log = LoggerFactory.getLogger(Runner.class);

  public enum RunType {
    LOCAL, // launch Worker(s) and Indexer as threads; have all components communicate via in-memory queues
    TEST, // same as LOCAL, but bypass Solr, and store message traffic so it can be inspected after the run
    KAFKA_LOCAL, // launch Worker(s) and Indexer as threads; have all components communicate via Kafka
    KAFKA_DISTRIBUTED // assume Workers/Indexers were started separately (don't launch threads); have all components communicate via Kafka
  }

  // no need to instantiate Runner; all methods currently static
  private Runner() {
  }

  /**
   * Runs the configured connectors.
   * <p>
   * no args: pipelines workers and indexers will be executed in separate threads within the same JVM; communication
   * between components will take place in memory and Kafka will not be used
   * <p>
   * -usekafka: connectors will be run, sending documents and receiving events via Kafka. Pipeline workers
   * and indexers will not be run. The assumption is that these have been deployed as separate processes.
   * <p>
   * -local: modifies -usekafka so that workers and indexers are started as separate threads within the same JVM;
   * kafka is still used for communication between them.
   * <p>
   * -render: prints out the effective/actual config in the exact form it will be seen by Lucille during the run
   */
  public static void main(String[] args) throws Exception {
    Options cliOptions = new Options()
        .addOption(Option.builder("usekafka").hasArg(false)
            .desc("Use Kafka for inter-component communication and don't execute pipelines locally").build())
        .addOption(Option.builder("local").hasArg(false)
            .desc("Modifies usekafka mode to execute pipelines locally").build())
        .addOption(Option.builder("validate").hasArg(false)
            .desc("Validate the configuration and exit").build())
        .addOption(Option.builder("render").hasArg(false)
            .desc("Print out the configuration file with substitutions applied and exit").build());

    CommandLine cli = null;
    try {
      cli = new DefaultParser().parse(cliOptions, args);
    } catch (UnrecognizedOptionException | MissingOptionException e) {
      try (StringWriter writer = new StringWriter();
          PrintWriter printer = new PrintWriter(writer)) {

        String header = "Run a sequence of connectors";
        new HelpFormatter().printHelp(printer, 256, "Runner", header, cliOptions,
            2, 10, "", true);
        log.info(writer.toString());
      }
      System.exit(1);
    }

    Config config = ConfigFactory.load();

    // allow handling of both validate and render flags
    if (cli.hasOption("validate") || cli.hasOption("render")) {
      if (cli.hasOption("render")) {
        renderConfig(config);
      }
      if (cli.hasOption("validate")) {
        logValidation(validateConnectors(config), "Connector");
        logValidation(validatePipelines(config), "Pipeline");
      }
      return;
    }

    RunType runType = getRunType(cli.hasOption("useKafka"), cli.hasOption("local"));

    // Kick off the run with a log of the result
    RunResult result = runWithResultLog(config, runType);

    if (result.getStatus()) {
      System.exit(0);
    } else {
      System.exit(1);
    }
  }

  /** Utility method to generate a Run ID. */
  public static String generateRunId() {
    return UUID.randomUUID().toString();
  }

  /**
   * Run Lucille end-to-end using the sequence of Connectors and the pipeline defined in the given
   * config file. Stop the run if any of the connectors fails.
   * <p>
   * Run in a local mode where message traffic is kept in memory and there is no
   * communication with external systems like Kafka. Sending to solr is bypassed.
   * <p>
   * Return a TestMessenger for each connector that
   * can be used to review the messages that were sent between various components during that connector's execution.
   */
  public static Map<String, TestMessenger> runInTestMode(String configName) throws Exception {
    Config config = ConfigFactory.load(configName);
    return runInTestMode(config);
  }

  public static Map<String, TestMessenger> runInTestMode(Config config) throws Exception {
    RunResult result = run(config, RunType.TEST);
    return result.getHistory();
  }

  private static void logValidation(Map<String, List<Exception>> exceptions, String validationName) {
    log.info(stringifyValidation(exceptions, validationName));
  }

  /**
   * Returns a stringified version of the given map of exceptions. Uses the given validation name in logged messages
   * to clarify which element of the Lucille Config is being validated.
   */
  public static String stringifyValidation(Map<String, List<Exception>> exceptions, String validationName) {
    if (exceptions.entrySet().stream().allMatch(e -> e.getValue().isEmpty())) {
      return validationName + " Configuration is valid";
    } else {
      StringBuilder message = new StringBuilder(validationName + " Configuration is invalid. Printing the list of exceptions for each element\n");

      for (Map.Entry<String, List<Exception>> entry : exceptions.entrySet()) {
        message.append("\t" + validationName + ": ").append(entry.getKey()).append("\tError count: ").append(entry.getValue().size())
            .append("\n");
        int i = 1;

        for (Exception e : entry.getValue()) {
          message.append("\t\tException ").append(i++).append(": ").append(e.getMessage()).append("\n");
        }
      }
      return message.delete(message.length() - 1, message.length()).toString();
    }
  }

  public static Map<String, List<Exception>> runInValidationMode(String configName) throws Exception {
    return runInValidationMode(ConfigFactory.load(configName));
  }

  public static Map<String, List<Exception>> runInValidationMode(Config config) throws Exception {
    Map<String, List<Exception>> pipelineExceptions = validatePipelines(config);
    logValidation(pipelineExceptions, "Pipeline");

    Map<String, List<Exception>> connectorExceptions = validateConnectors(config);
    logValidation(connectorExceptions, "Connector");

    pipelineExceptions.putAll(connectorExceptions);
    return pipelineExceptions;
  }

  private static Map<String, List<Exception>> validateConnectors(Config rootConfig) {
    Map<String, List<Exception>> exceptionMap = new LinkedHashMap<>();

    // Resolve the config in case it is referenced as another file / has system properties
    rootConfig = rootConfig.resolve();

    for (Config connectorConfig : rootConfig.getConfigList("connectors")) {
      String name = connectorConfig.getString("name");

      if (!exceptionMap.containsKey(name)) {
        // Still uses a List so we can support extra exceptions for duplicate pipeline names
        exceptionMap.put(name, Connector.getConnectorConfigExceptions(connectorConfig));
      } else {
        exceptionMap.get(name).add(new Exception("There exists a connector with the same name"));
      }
    }

    return exceptionMap;
  }

  /**
   * Returns a mapping from pipline names to the list of exceptions produced when validating them.
   */
  private static Map<String, List<Exception>> validatePipelines(Config config) throws Exception {
    Map<String, List<Exception>> exceptionMap = new LinkedHashMap<>();
    for (Config pipelineConfig : config.getConfigList("pipelines")) {
      String name = pipelineConfig.getString("name");

      if (!exceptionMap.containsKey(name)) {
        exceptionMap.put(name, Pipeline.validateStages(config, name));
      } else {
        exceptionMap.get(name).add(new Exception("There exists a pipeline with the same name"));
      }
    }
    return exceptionMap;
  }

  public static void renderConfig(Config config) {
    ConfigRenderOptions renderOptions = ConfigRenderOptions.defaults()
        .setJson(true)
        .setComments(false)
        .setOriginComments(false);
    log.info(config.root().render(renderOptions));
  }

  /**
   * Derives the RunType for the new run from the 'useKafka' and 'local' parameters.
   */
  static RunType getRunType(boolean useKafka, boolean local) {
    if (useKafka) {
      if (local) {
        return RunType.KAFKA_LOCAL;
      } else {
        return RunType.KAFKA_DISTRIBUTED;
      }
    } else {
      return RunType.LOCAL;
    }
  }

  public static RunResult runWithResultLog(Config config, RunType runType) throws Exception {
    return runWithResultLog(config, runType, null);
  }
  
  /**
   * Kicks off a new Lucille run and logs information about the run to the console after completion.
   */
  public static RunResult runWithResultLog(Config config, RunType runType, String runId)
      throws Exception {
    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    RunResult result;

    try {
      result = run(config, runType, runId);

      // log detailed metrics
      Slf4jReporter.forRegistry(SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG))
          .outputTo(log).withLoggingLevel(getMetricsLoggingLevel(config)).build().report();
      // log run summary
      log.info(result.toString());
      return result;
    } finally {
      stopWatch.stop();
      log.info(String.format("Run took %.2f secs.",
          (double) stopWatch.getTime(TimeUnit.MILLISECONDS) / 1000));
    }
  }
  
  /**
   * Non managed Run with internal generated runId
   *
   * @param config
   * @param type
   * @return
   * @throws Exception
   */
  public static RunResult run(Config config, RunType type) throws Exception {
    return run(config, type, null);
  }

  /**
   * Generates a run ID if not supplied and performs an end-to-end run of the designated type.
   *
   * @param config
   * @param type
   * @param runId
   * @return
   * @throws Exception
   */
  public static RunResult run(Config config, RunType type, String runId) throws Exception {
    if (runId == null) {
      runId = Runner.generateRunId();
    }

    MDC.put(RUNID_FIELD, runId);
    log.info("Starting run with id " + runId);

    List<Connector> connectors = Connector.fromConfig(config);
    List<ConnectorResult> connectorResults = new ArrayList<>();

    boolean startWorkerAndIndexer = !type.equals(RunType.KAFKA_DISTRIBUTED);
    boolean bypassSolr = type.equals(RunType.TEST);

    Map<String, TestMessenger> history = type.equals(RunType.TEST) ? new HashMap<>() : null;

    for (Connector connector : connectors) {

      WorkerMessengerFactory workerMessengerFactory;
      IndexerMessengerFactory indexerMessengerFactory;
      PublisherMessengerFactory publisherMessengerFactory;

      if (RunType.TEST.equals(type)) {
        TestMessenger messenger = new TestMessenger();
        history.put(connector.getName(), messenger);
        workerMessengerFactory = WorkerMessengerFactory.getConstantFactory(messenger);
        indexerMessengerFactory = IndexerMessengerFactory.getConstantFactory(messenger);
        publisherMessengerFactory = PublisherMessengerFactory.getConstantFactory(messenger);
      } else if (RunType.LOCAL.equals(type)) {
        LocalMessenger messenger = new LocalMessenger(config);
        workerMessengerFactory = WorkerMessengerFactory.getConstantFactory(messenger);
        indexerMessengerFactory = IndexerMessengerFactory.getConstantFactory(messenger);
        publisherMessengerFactory = PublisherMessengerFactory.getConstantFactory(messenger);
      } else { // RunType.KAFKA_LOCAL.equals(type) || RunType.KAFKA_DISTRIBUTED.equals(type)
        workerMessengerFactory = WorkerMessengerFactory.getKafkaFactory(config, connector.getPipelineName());
        indexerMessengerFactory = IndexerMessengerFactory.getKafkaFactory(config, connector.getPipelineName());
        publisherMessengerFactory = PublisherMessengerFactory.getKafkaFactory(config);
      }

      ConnectorResult result =
          runConnectorWithComponents(config, runId, type, connector,
              workerMessengerFactory, indexerMessengerFactory, publisherMessengerFactory, startWorkerAndIndexer, bypassSolr);

      connectorResults.add(result);

      if (!result.getStatus()) {
        log.error("Aborting run because " + connector.getName() + " failed.");
        return new RunResult(false, connectors, connectorResults, history, runId);
      }
    }

    return new RunResult(true, connectors, connectorResults, history, runId);
  }

  /**
   * Runs the designated Connector. Returns a successful ConnectorResult only when 1) the Connector has
   * finished generating documents, and 2) all of the documents (and any generated children) have reached
   * an end state in the workflow: either being indexed or erroring-out. Returns a failing ConnectorResult
   * if any connector lifecycle method throws an exception or the publisher itself fails
   */
  public static ConnectorResult runConnector(Config config, String runId, Connector connector, Publisher publisher) {
    try {
      return runConnectorInternal(config, runId, connector, publisher);
    } finally {
      try {
        connector.close();
      } catch (Exception e) {
        log.error("Error closing connector", e);

        if (publisher != null) {
          try {
            publisher.close();
          } catch (Exception e2) {
            log.error("Error closing publisher", e2);
            return new ConnectorResult(connector, publisher, false, "Error closing connector and publisher");
          }
        }

        return new ConnectorResult(connector, publisher, false, "Error closing connector");
      }

      if (publisher != null) {
        try {
          publisher.close();
        } catch (Exception e) {
          log.error("Error closing publisher", e);
          return new ConnectorResult(connector, publisher, false, "Error closing publisher");
        }
      }
    }
  }

  /**
   * Helper used by runConnector(). Performs the following steps:
   * 1) call connector.preExecute()
   * 2) launch a thread that calls connector.execute() with the given publisher, and then publisher.flush()
   * 3) call publisher.waitForCompletion()
   * 4) call connector.postExecute()
   */
  private static ConnectorResult runConnectorInternal(Config config, String runId, Connector connector, Publisher publisher) {
    log.info("Running connector " + connector.getName() + " feeding to pipeline " +
        (connector.getPipelineName() == null ? "NOT CONFIGURED" : connector.getPipelineName()));
    StopWatch stopWatch = new StopWatch();
    stopWatch.start();

    try {
      connector.preExecute(runId);
    } catch (ConnectorException e) {
      log.error("Connector failed to perform pre execution actions.", e);
      return new ConnectorResult(connector, publisher, false, "preExecute failed.");
    }

    PublisherResult pubResult = null;

    // the publisher could be null if we are running a connector that has no associated pipeline and therefore
    // there's nothing to publish to; in this case, we call execute synchronously because we are not
    // pumping any work into the system and waiting for other components to complete it
    if (publisher == null) {
      try {
        connector.execute(null);
      } catch (ConnectorException e) {
        log.error("Connector execution failed.", e);
        return new ConnectorResult(connector, publisher, false, "execute failed.");
      }
    } else {
      try {
        ConnectorThread connectorThread = new ConnectorThread(connector, publisher, runId, ThreadNameUtils.createName("Connector", runId));
        connectorThread.start();
        final int connectorTimeout = config.hasPath("runner.connectorTimeout") ?
            config.getInt("runner.connectorTimeout") : DEFAULT_CONNECTOR_TIMEOUT;
        pubResult = publisher.waitForCompletion(connectorThread, connectorTimeout);
      } catch (Exception e) {
        log.error("waitForCompletion failed", e);
        return new ConnectorResult(connector, publisher, false, "waitForCompletion failed");
      }
    }

    if (pubResult == null || pubResult.getStatus()) {
      try {
        connector.postExecute(runId);
      } catch (ConnectorException e) {
        log.error("postExecute failed", e);
        return new ConnectorResult(connector, publisher, false, "postExecute failed");
      }
    }

    stopWatch.stop();
    double durationSecs = ((double) stopWatch.getTime(TimeUnit.MILLISECONDS)) / 1000;
    log.info(String.format("Connector %s feeding to pipeline %s complete. Time: %.2f secs.",
        connector.getName(), connector.getPipelineName(), durationSecs));

    boolean status = pubResult == null ? true : pubResult.getStatus();
    String msg = pubResult == null ? null : pubResult.getMessage();
    return new ConnectorResult(connector, publisher, status, msg, durationSecs);
  }

  /**
   * Wrapper around runConnector() that starts and stops the other components necessary for a complete connector
   * execution; specifically, a WorkerPool and an Indexer.
   */
  private static ConnectorResult runConnectorWithComponents(Config config,
      String runId,
      RunType runType,
      Connector connector,
      WorkerMessengerFactory workerMessengerFactory,
      IndexerMessengerFactory indexerMessengerFactory,
      PublisherMessengerFactory publisherMessengerFactory,
      boolean startWorkerAndIndexer,
      boolean bypassIndexer) throws Exception {
    String pipelineName = connector.getPipelineName();
    WorkerPool workerPool = null;
    Indexer indexer = null;
    Thread indexerThread = null;
    Publisher publisher = null;

    try {
      // If local/test we want to give WorkerPool/Indexer the run_id directly.
      // (Otherwise let Kafka messengers pass it thru in the documents.)
      String localRunId = (runType.equals(RunType.LOCAL) || runType.equals(RunType.TEST)) ? runId : null;

      // create a common metrics naming prefix to be used by all components that will be collecting metrics,
      // to ensure that metrics are collected separately for each connector/pipeline pair
      String metricsPrefix = runId + "." + connector.getName() + "." + connector.getPipelineName();

      if (startWorkerAndIndexer && connector.getPipelineName() != null) {
        workerPool = new WorkerPool(config, pipelineName, localRunId, workerMessengerFactory, metricsPrefix);

        try {
          workerPool.start();
        } catch (Exception e) {
          log.error("Error starting workers for pipeline " + pipelineName, e);
          return new ConnectorResult(connector, publisher, false, "Error starting workers for pipeline " + pipelineName);
        }

        try {
          IndexerMessenger indexerMessenger = indexerMessengerFactory.create();
          indexer = IndexerFactory.fromConfig(config, indexerMessenger, bypassIndexer, metricsPrefix, localRunId);
        } catch (Exception e) {
          log.error("Error creating indexer from config.", e);
          return new ConnectorResult(connector, publisher, false, "Error creating indexer from config.");
        }

        if (!indexer.validateConnection()) {
          String msg = "Indexer could not connect.";
          log.error(msg);
          // clean up indexer lifecycle which was created but never run in a thread.
          indexer.closeConnection();
          return new ConnectorResult(connector, publisher, false, msg);
        }

        indexerThread = new Thread(indexer, ThreadNameUtils.createName("Indexer", localRunId));
        indexerThread.start();
      }

      if (connector.getPipelineName() != null) {
        PublisherMessenger publisherMessenger = publisherMessengerFactory.create();
        publisher = new PublisherImpl(config, publisherMessenger, runId,
            connector.getPipelineName(), metricsPrefix, connector.requiresCollapsingPublisher());
      }

      return runConnector(config, runId, connector, publisher);

    } finally {
      if (workerPool != null) {
        workerPool.stop();
        workerPool.join(DEFAULT_WORKER_INDEXER_JOIN_TIMEOUT);
      }
      if (indexerThread != null) {
        indexer.terminate();
        indexerThread.join(DEFAULT_WORKER_INDEXER_JOIN_TIMEOUT);
      }
    }
  }

  private static Slf4jReporter.LoggingLevel getMetricsLoggingLevel(Config config) {
    try {
      return config.hasPath("runner.metricsLoggingLevel") ?
          Slf4jReporter.LoggingLevel.valueOf(config.getString("runner.metricsLoggingLevel")) :
          Slf4jReporter.LoggingLevel.DEBUG;
    } catch (Exception e) {
      log.error("Error obtaining metrics logging level", e);
    }
    return Slf4jReporter.LoggingLevel.DEBUG;
  }
}
