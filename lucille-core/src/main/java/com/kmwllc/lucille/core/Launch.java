package com.kmwllc.lucille.core;

import com.kmwllc.lucille.indexer.IndexerFactory;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.message.KafkaIndexerMessenger;
import com.kmwllc.lucille.message.WorkerMessengerFactory;
import com.kmwllc.lucille.util.ThreadNameUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.commons.cli.AlreadySelectedException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts and stops a Lucille process. The component entry points {@link Runner#main}, {@link Worker#main},
 * {@link Indexer#main} and {@link WorkerIndexer#main} each delegate to the corresponding method here, so that the
 * process-level behaviour they share lives in one place: how the components are started, how the process waits for
 * them, the single JVM shutdown hook, and how the process exits.
 *
 * <p>The shutdown hook runs on SIGINT (Ctrl-C) and SIGTERM (Kubernetes, docker stop) alike, so a rolling deploy gets
 * the same terminate-and-join sequence as an interactive interrupt, and the process exits with the JVM's signal exit
 * status (130 for SIGINT, 143 for SIGTERM) rather than 0. A component that ends on its own exits 0 on success and 1 on
 * failure. Library classes do not install signal handlers or call System.exit; only this class does.
 *
 * <p>This class has no public members yet. A <code>main</code> that selects a component by role (for example
 * <code>Launch worker my-pipeline</code>) may be added later; the component mains would then remain as aliases.
 */
public final class Launch {

  /** How long a shutdown hook waits for started threads to finish before letting the JVM exit. */
  private static final long SHUTDOWN_JOIN_TIMEOUT_MS = Runner.DEFAULT_WORKER_INDEXER_JOIN_TIMEOUT;

  /** How often a process checks whether the threads it started are still alive. */
  private static final long POLL_MILLIS = 500;

  private static final Logger log = LoggerFactory.getLogger(Launch.class);

  // the actions the single shutdown hook runs, in registration order
  private static final List<Runnable> shutdownActions = new ArrayList<>();
  private static boolean hookInstalled = false;
  // set by the hook; a wait loop that sees it returns instead of calling exit(), which would block during shutdown
  private static volatile boolean shuttingDown = false;

  private Launch() {
  }

  // ---------------------------------------------------------------------------------------------------------------
  // runner

  /**
   * Runs the configured connectors; see {@link Runner#main} for the options. Exits 0 when the run succeeds and 1 when
   * it fails, or returns without exiting after <code>-validate</code> or <code>-render</code>.
   */
  static void runner(String[] args) throws Exception {
    Options cliOptions = Runner.cliOptions();

    CommandLine cli;
    try {
      // option names are case insensitive; the values of any future option that takes one are left alone
      args = Arrays.stream(args).map(Launch::lowerCaseOptionName).toArray(String[]::new);
      cli = new DefaultParser().parse(cliOptions, args);

      if (!cli.getArgList().isEmpty()) {
        Runner.printHelp(cliOptions, "Unrecognized argument(s): " + cli.getArgList());
        SystemHelper.exit(1);
        return;
      }
    } catch (UnrecognizedOptionException | MissingOptionException | AlreadySelectedException e) {
      Runner.printHelp(cliOptions, e.getMessage());
      SystemHelper.exit(1);
      return;
    }

    Config config = ConfigFactory.load();

    // allow handling of both validate and render flags
    if (cli.hasOption("validate") || cli.hasOption("render")) {
      if (cli.hasOption("render")) {
        Runner.renderConfig(config);
      }
      if (cli.hasOption("validate")) {
        Runner.runInValidationMode(config);
      }
      return;
    }

    // make a RunnerState available for holding resources that will be instantiated later in the run, so that they can
    // be cleanly stopped and closed if the process is told to shut down during the run
    Runner.RunnerState state = new Runner.RunnerState();
    Runner.setState(state);
    installShutdownHook(() -> {
      log.info("Runner attempting clean shutdown after being told to stop");
      state.close();
    });

    Runner.RunType runType = Runner.getRunType(cli.hasOption("distributed"), cli.hasOption("external"));

    // Kick off the run with a log of the result
    RunResult result = Runner.runAndLogResult(config, runType, true);

    exitUnlessShuttingDown(result.getStatus() ? 0 : 1);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // worker

  /**
   * Runs a pool of workers reading from Kafka; see {@link Worker#main}. If a worker fails, the remaining workers are
   * stopped and the process exits 1 so that an orchestrator restarts it rather than leaving it running degraded.
   */
  static void worker(String[] args) throws Exception {
    Config config = ConfigFactory.load();
    String pipelineName = args.length > 0 ? args[0] : config.getString("worker.pipeline");
    log.info("Starting Workers for pipeline: {}", pipelineName);

    WorkerMessengerFactory workerMessengerFactory = WorkerMessengerFactory.getKafkaFactory(config, pipelineName);

    WorkerPool workerPool = new WorkerPool(config, pipelineName, null, workerMessengerFactory, pipelineName);
    installShutdownHook(() -> {
      log.info("Workers shutting down");
      workerPool.stop();
      try {
        workerPool.join(SHUTDOWN_JOIN_TIMEOUT_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    workerPool.start();

    while (!shuttingDown && workerPool.isAlive() && !workerPool.isFailed()) {
      Thread.sleep(POLL_MILLIS);
    }
    if (shuttingDown) {
      return;
    }
    if (workerPool.isFailed()) {
      log.error("A worker failed; stopping the remaining workers and exiting.", workerPool.getThrowable());
    } else {
      log.error("All worker threads have stopped without being asked to; exiting.");
    }
    workerPool.stop();
    workerPool.join(SHUTDOWN_JOIN_TIMEOUT_MS);
    exitUnlessShuttingDown(1);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // indexer

  /**
   * Runs an Indexer reading from Kafka; see {@link Indexer#main}. The indexer only stops when told to, so if its thread
   * ends on its own the process exits 1.
   */
  static void indexer(String[] args) throws Exception {
    Config config = ConfigFactory.load();
    String pipelineName = args.length > 0 ? args[0] : config.getString("indexer.pipeline");
    log.info("Starting Indexer for pipeline: {}", pipelineName);
    IndexerMessenger messenger = new KafkaIndexerMessenger(config, pipelineName);
    Indexer indexer = IndexerFactory.fromConfig(config, messenger, false, pipelineName, null);

    if (!indexer.validateConnection()) {
      log.error("Indexer could not connect");
      indexer.closeConnection();
      SystemHelper.exit(1);
      return;
    }

    Thread indexerThread = new Thread(indexer, ThreadNameUtils.createName("Indexer"));
    installShutdownHook(() -> {
      log.info("Indexer shutting down");
      indexer.terminate();
      try {
        indexerThread.join(SHUTDOWN_JOIN_TIMEOUT_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    indexerThread.start();

    indexerThread.join();
    if (shuttingDown) {
      return;
    }
    log.error("Indexer thread stopped without being asked to; exiting.");
    exitUnlessShuttingDown(1);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // worker-indexer

  /**
   * Runs paired Worker and Indexer threads (hybrid mode); see {@link WorkerIndexer#main}. If a thread stops on its own,
   * the rest are stopped and the process exits 1.
   */
  static void workerIndexer(String[] args) throws Exception {
    Config config = ConfigFactory.load();
    String pipelineName = args.length > 0 ? args[0] : config.getString("worker.pipeline");
    WorkerIndexerPool pool = new WorkerIndexerPool(config, pipelineName, false, null);
    installShutdownHook(() -> {
      log.info("WorkerIndexers shutting down");
      try {
        // stopping the WorkerIndexer closes the kafka client connection via workerThread.terminate(), which causes the
        // worker's run method to break out of its while loop and eventually call messenger.close()
        pool.stop();
      } catch (Exception e) {
        log.error("Error stopping WorkerIndexer", e);
      }
    });
    pool.start();

    while (!shuttingDown && pool.isAlive()) {
      Thread.sleep(POLL_MILLIS);
    }
    if (shuttingDown) {
      return;
    }
    log.error("A WorkerIndexer thread stopped without being asked to; stopping the rest and exiting.");
    pool.stop();
    exitUnlessShuttingDown(1);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // shutdown and exit

  /**
   * Registers an action for the single JVM shutdown hook, installing the hook on the first call. Actions run in the
   * order they were registered; a failing action does not prevent the others from running.
   */
  static synchronized void installShutdownHook(Runnable action) {
    shutdownActions.add(action);
    if (hookInstalled) {
      return;
    }
    hookInstalled = true;
    Runtime.getRuntime().addShutdownHook(new Thread(Launch::runShutdownActions, "lucille-shutdown"));
  }

  // package-private so that tests can run the hook's actions without stopping the JVM
  static void runShutdownActions() {
    shuttingDown = true;
    List<Runnable> actions;
    synchronized (Launch.class) {
      actions = new ArrayList<>(shutdownActions);
    }
    for (Runnable action : actions) {
      try {
        action.run();
      } catch (Exception e) {
        log.error("Error during shutdown", e);
      }
    }
  }

  /**
   * Exits with the given code, unless the JVM is already shutting down because of a signal, in which case the JVM's
   * own exit status (130 for SIGINT, 143 for SIGTERM) stands and this thread simply returns.
   */
  static void exitUnlessShuttingDown(int code) {
    if (!shuttingDown) {
      SystemHelper.exit(code);
    }
  }

  static boolean isShuttingDown() {
    return shuttingDown;
  }

  /**
   * Forgets the registered shutdown actions and clears the shutting-down flag. For tests only: the actions and the
   * flag are static, so a test that runs them would otherwise affect every later test in the same JVM.
   */
  static synchronized void resetShutdownState() {
    shutdownActions.clear();
    shuttingDown = false;
  }

  /** Lower-cases a command line option name, leaving the value of any future option that takes one alone. */
  static String lowerCaseOptionName(String arg) {
    return arg.startsWith("-") ? arg.toLowerCase(Locale.ROOT) : arg;
  }

  // utility class to allow mocking in a test context
  static class SystemHelper {
    static void exit(int code) {
      System.exit(code);
    }
  }
}
