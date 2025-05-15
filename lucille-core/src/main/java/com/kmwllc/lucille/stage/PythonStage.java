package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.stage.util.Py4jExecutorInterface;
import com.typesafe.config.Config;
import com.kmwllc.lucille.stage.util.StreamGobbler;
import py4j.GatewayServer;
import py4j.GatewayServerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import py4j.Py4JServerConnection;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Executes a Python script using Py4J as part of a pipeline stage.
 *
 * Config Parameters:
 * <p>
 * <b>scriptPath</b> (String, Required): Path to the Python script to execute.
 * <p>
 * <b>pythonExecutable</b> (String, Optional): Python executable to use.
 * Defaults to 'python3'.
 * <p>
 * <b>function_name</b> (String, Optional): Function in the script to call.
 * Defaults to 'process'.
 * <p>
 * <b>args</b> (List<Object>, Optional): Arguments to pass to the function.
 * Defaults to [].
 * <p>
 * <b>port</b> (int, Optional): Port for the GatewayServer. Defaults to
 * GatewayServer.DEFAULT_PORT.
 */
public class PythonStage extends Stage implements GatewayServerListener {

  private static final Logger log = LoggerFactory.getLogger(PythonStage.class);

  private final String scriptPath;
  private final String pythonExecutable;
  private final String functionName;
  private final List<Object> args;
  private final int port;
  private GatewayServer gateway;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private Process pythonProcess; // Transient handle to the launched Python process
  private Py4jClient py4jClient = null;
  private boolean ready = false;
  private ObjectMapper mapper = new ObjectMapper();
  private transient Py4jExecutorInterface handler;

  /**
   * A class to wait on a process signal and notify py4jClient is disconnected
   *
   */
  public class WaitForProcess extends Thread {
    public Integer exitCode;
    public Process process;
    public PythonStage pythonStage;

    public WaitForProcess(PythonStage pythonStage, Process process) {
      super(String.format("%s-process-signal", pythonStage.getName()));
      this.process = process;
      this.pythonStage = pythonStage;
    }

    @Override
    public void run() {
      try {
        exitCode = process.waitFor();
      } catch (InterruptedException e) {
        log.info("process shutting down {}", process);
      }
      log.warn("process %s terminated with exit code %d", process.toString(), exitCode);
    }
  }

  /**
   * POJO class to tie all the data elements of a external python process
   * together. Including the process handler, the std out, std err streams and
   * termination signal thread.
   * 
   * @author GroG
   *
   */
  class Py4jClient {
    // pythonStage connection
    public transient Py4JServerConnection connection;
    public transient StreamGobbler gobbler;
    public transient Process process;
    public transient PythonStage pythonStage;
    public transient Thread waitFor;

    public Py4jClient(PythonStage pythonStage, Process process) {
      this.process = process;
      this.pythonStage = pythonStage;
      this.gobbler = new StreamGobbler(String.format("Py4jClient-%s", getName()), process.getInputStream());
      this.gobbler.start();
      this.waitFor = new WaitForProcess(pythonStage, process);
      this.waitFor.start();

      log.info("process started {}", process);
    }
  }

  public PythonStage(Config config) {
    super(config, Spec.stage()
        .withRequiredProperties("scriptPath")
        .withOptionalProperties("pythonExecutable", "function_name", "args", "port"));
    log.info("PythonStage constructor");
    this.scriptPath = config.getString("scriptPath");
    this.pythonExecutable = config.hasPath("pythonExecutable") ? config.getString("pythonExecutable") : "python3";
    this.functionName = config.hasPath("function_name") ? config.getString("function_name") : "process";
    this.args = config.hasPath("args") ? (List<Object>) config.getAnyRefList("args") : java.util.Collections.emptyList();
    this.port = config.hasPath("port") ? config.getInt("port") : GatewayServer.DEFAULT_PORT;
  }

  private void checkPythonInstalled() throws StageException {
    try {
      Process process = new ProcessBuilder(pythonExecutable, "--version").redirectErrorStream(true).start();
      StringBuilder output = new StringBuilder();
      try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(process.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          output.append(line).append("\n");
        }
      }
      int exitCode = process.waitFor();
      String versionOutput = output.toString().trim();
      if (exitCode != 0 || versionOutput.isEmpty() || !versionOutput.toLowerCase().contains("python")) {
        log.error("[PythonStage] Python version check failed. Output:\n{}", versionOutput);
        throw new StageException("Python executable '" + pythonExecutable + "' not found or not working. Output: " + versionOutput);
      }
      log.info("[PythonStage] Detected Python version:\n{}", versionOutput);
    } catch (Exception e) {
      throw new StageException("Failed to check Python installation: " + e.getMessage(), e);
    }
  }

  private void checkPy4jInstalled() throws StageException {
    try {
      Process process = new ProcessBuilder(pythonExecutable, "-c", "import py4j; print(py4j.__version__)")
        .redirectErrorStream(true).start();
      StringBuilder output = new StringBuilder();
      try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(process.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          output.append(line).append("\n");
        }
      }
      int exitCode = process.waitFor();
      String py4jOutput = output.toString().trim();
      if (exitCode != 0 || py4jOutput.isEmpty() || py4jOutput.toLowerCase().contains("traceback")) {
        log.error("[PythonStage] py4j check failed. Output:\n{}", py4jOutput);
        throw new StageException("py4j is not installed in the Python environment for '" + pythonExecutable + "'. Output: " + py4jOutput);
      }
      log.info("[PythonStage] Detected py4j version:\n{}", py4jOutput);
    } catch (Exception e) {
      throw new StageException("Failed to check py4j installation: " + e.getMessage(), e);
    }
  }

  private void logPythonEnvironment() throws StageException {
    try {
      // Get the Python executable path using Python itself
      Process execPathProc = new ProcessBuilder(pythonExecutable, "-c", "import sys; print(sys.executable)")
        .redirectErrorStream(true).start();
      StringBuilder execPathOutput = new StringBuilder();
      try (java.io.BufferedReader execPathReader = new java.io.BufferedReader(new java.io.InputStreamReader(execPathProc.getInputStream()))) {
        String line;
        while ((line = execPathReader.readLine()) != null) {
          execPathOutput.append(line).append("\n");
        }
      }
      execPathProc.waitFor();
      log.info("[PythonStage] Python executable path (from sys.executable):\n{}", execPathOutput.toString().trim());

      // Log all sys.path entries
      // -u: unbuffered stdout
      Process sysPathProc = new ProcessBuilder(pythonExecutable, "-u", "-c", "import sys; [print(p) for p in sys.path]")
        .redirectErrorStream(true).start();
      StringBuilder sysPaths = new StringBuilder();
      try (java.io.BufferedReader sysPathReader = new java.io.BufferedReader(new java.io.InputStreamReader(sysPathProc.getInputStream()))) {
        String line;
        while ((line = sysPathReader.readLine()) != null) {
          sysPaths.append(line).append("\n");
        }
      }
      sysPathProc.waitFor();
      log.info("[PythonStage] Python sys.path entries:\n{}", sysPaths.toString().trim());
    } catch (Exception e) {
      log.error("[PythonStage] Error logging Python environment", e);
      throw new StageException("Failed to log Python environment: " + e.getMessage(), e);
    }
  }

  @Override
  public void start() throws StageException {
    log.info("[PythonStage] start() called");
    log.info("[PythonStage] scriptPath: {}", scriptPath);
    log.info("[PythonStage] pythonExecutable: {}", pythonExecutable);
    log.info("[PythonStage] functionName: {}", functionName);
    log.info("[PythonStage] port: {}", port);
    checkPythonInstalled();
    logPythonEnvironment();
    checkPy4jInstalled();
    if (started.compareAndSet(false, true)) {
      boolean portAvailable = true;
      try (ServerSocket ignored = new ServerSocket(port)) {
        log.info("[PythonStage] Port {} is available", port);
      } catch (IOException e) {
        log.error("port {} is already in use", port, e);
        portAvailable = false;
      }
      if (!portAvailable) {
        log.warn(
            "[PythonStage] WARNING: Port {} is already in use. Assuming a GatewayServer is already running and reconnecting to it.",
            port);
        // Do not start a new GatewayServer or Python process
        gateway = null;
        pythonProcess = null;
        return;
      }

      log.info("[PythonStage] Starting Py4J GatewayServer on port {}", port);
      gateway = new GatewayServer(this, port);
      gateway.start();
      log.info("[PythonStage] GatewayServer started");

      handler = (Py4jExecutorInterface) gateway.getPythonServerEntryPoint(new Class[] { Py4jExecutorInterface.class });

      try {
        // Use Py4jClient.py from main/resources
        String py4jClientScript = getClass().getClassLoader().getResource("Py4jClient.py").getPath();
        log.info("[PythonStage] Using Py4jClient.py script at {}", py4jClientScript);

        log.info("[PythonStage] Launching Python process: {} {} {} {}", pythonExecutable, py4jClientScript, scriptPath, port);
        pythonProcess = new ProcessBuilder(
                pythonExecutable,
                py4jClientScript,
                scriptPath,
                String.valueOf(port)
            )
            .redirectErrorStream(true)
            .start();
        log.info("[PythonStage] Python process started");

        py4jClient = new Py4jClient(this, pythonProcess);

        // FIXME - implement
        // handler.start(configJson);

      } catch (Exception e) {
        log.error("[PythonStage] Failed to launch Python process for Py4J integration", e);
        throw new StageException("Failed to launch Python process for Py4J integration", e);
      }
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    log.info("processDocument");
    try {      
      Map<String, Object> msg = new HashMap<>();
      // TODO - configurable method name
      msg.put("method", "process_document");
      msg.put("data", doc);
      String msgJson = mapper.writeValueAsString(msg);
      log.info("JSON encoded doc: {}", msgJson);
      // handler.exec can now be used to send docJson to Python if needed
      Object ret = handler.send(msgJson);
      log.info("Python returned: {}", ret);
      doc.setField("python_stage_executed", true);
      return null;  
    } catch (Exception e) {
      log.error("Failed to send json encoded Document to Python", e);
      throw new StageException("Failed to send json encoded Document to Python", e);
    }
  }

  @Override
  public void stop() throws StageException {
    if (gateway != null) {
      gateway.shutdown();
    }
    if (pythonProcess != null) {
      pythonProcess.destroy();
      pythonProcess = null;
    }
  }

  @Override
  public void connectionError(Exception e) {
    log.info("[PythonStage] Connection error: {}", e.getMessage());
    log.error("connectionError: {}", e.getMessage(), e);
  }

  @Override
  public void connectionStarted(Py4JServerConnection gatewayConnection) {
    log.info("[PythonStage] Connection established: {}", gatewayConnection);
    try {
      log.info("connectionStarted {}", gatewayConnection.toString());
      ready = true;
      // invoke("getClients");
    } catch (Exception e) {
      log.error("connectionStarted {}", e.getMessage(), e);
    }
  }

  @Override
  public void connectionStopped(Py4JServerConnection gatewayConnection) {
    log.info("[PythonStage] Connection lost: {}", gatewayConnection);
    py4jClient = null;
    ready = false;
  }

  @Override
  public void serverError(Exception e) {
    log.error("serverError: {}", e.getMessage(), e);
  }

  @Override
  public void serverPostShutdown() {
    log.info("serverPostShutdown");
  }

  @Override
  public void serverPreShutdown() {
    log.info("serverPreShutdown");
  }

  @Override
  public void serverStarted() {
    log.info("serverStarted");
  }

  @Override
  public void serverStopped() {
    log.info("serverStopped");
  }

  /**
   * Sink for standard output from Py4j-related subprocesses. This method
   * immediately publishes the output on {@link #publishStdOut(String)}.
   *
   * @param msg
   *          The output from a py4j related subprocess.
   */
  public void handleStdOut(String msg) {
    log.info("Py4jClient -> {}", msg);
  }


}
