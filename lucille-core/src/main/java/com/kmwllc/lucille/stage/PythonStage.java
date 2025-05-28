package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.stage.util.Py4jExecutorInterface;
import com.typesafe.config.Config;
import com.kmwllc.lucille.stage.util.StreamGobbler;
import java.util.Objects;
import py4j.GatewayServer;
import py4j.GatewayServerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;
import py4j.Py4JServerConnection;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A pipeline stage that executes a Python script using Py4J for cross-language
 * integration.
 * <p>
 * This stage launches a Python process, connects to it via Py4J, and allows
 * Java code to call Python functions
 * and exchange data (typically as JSON). The Python script can modify
 * documents, perform enrichment, or run
 * arbitrary logic as part of the Lucille pipeline.
 * </p>
 *
 * <h2>Configuration Parameters</h2>
 * <ul>
 * <li><b>scriptPath</b> (String, Required): Path to the user Python script to
 * execute.</li>
 * <li><b>pythonExecutable</b> (String, Optional): Python executable to use
 * (default: 'python3').</li>
 * <li><b>function_name</b> (String, Optional): Name of the function in the
 * script to call (default: 'process_document').</li>
 * <li><b>port</b> (int, Optional): Port for the Py4J GatewayServer (default:
 * GatewayServer.DEFAULT_PORT).</li>
 * <li><b>requirementsPath</b> (String, Optional): Path to the Python
 * requirements file (for pip install). If specified, the stage will ensure
 * the requirements are installed in the Python environment before execution.
 * </li>
 * </ul>
 *
 * <h2>Behavior</h2>
 * <ul>
 * <li>Checks that Python and py4j are installed (and attempts to install py4j
 * if missing).</li>
 * <li>Starts a Py4J GatewayServer and launches the Python process, passing
 * script path and port as arguments.</li>
 * <li>For each document, serializes it to JSON, sends it to Python, and updates
 * the document with the returned fields.</li>
 * <li>Captures and logs Python stdout/stderr in the Java logs.</li>
 * </ul>
 */
public class PythonStage extends Stage implements GatewayServerListener {

  private static final Logger log = LoggerFactory.getLogger(PythonStage.class);

  private final String scriptPath;
  private final String pythonExecutable;
  private final String requirementsPath;
  // Thread-safe static port management
  private static final Object portLock = new Object();
  private static final Set<Integer> usedPorts = new HashSet<>();
  private static int nextPort = 25333; // Default Py4J port
  private final int port;
  private GatewayServer gateway;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private transient Process pythonProcess; // Transient handle to the launched Python process
  private Py4jClient py4jClient = null;
  private volatile boolean ready = false;
  private ObjectMapper mapper = new ObjectMapper();
  private transient Py4jExecutorInterface handler;
  private String venvPythonPath = null;

  /**
   * Waits for the Python process to terminate and logs its exit code.
   * Notifies the parent PythonStage when the process ends.
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
      log.warn("process {} terminated with exit code {}", process.toString(), exitCode);
    }
  }

  /**
   * Holds references to the external Python process, its output gobbler, and the
   * process wait thread.
   * Used to manage the lifecycle and output of the Python subprocess.
   */
  class Py4jClient {
    // pythonStage connection
    public transient Py4JServerConnection connection;
    public transient StreamGobbler gobbler;
    public transient StreamGobbler errGobbler;
    public transient Process process;
    public transient PythonStage pythonStage;
    public transient Thread waitFor;

    public Py4jClient(PythonStage pythonStage, Process process) {
      this.process = process;
      this.pythonStage = pythonStage;
      // Gobble stdout
      this.gobbler = new StreamGobbler(String.format("Py4jClient-stdout-%s", getName()), process.getInputStream());
      this.gobbler.start();
      // Gobble stderr
      this.errGobbler = new StreamGobbler(String.format("Py4jClient-stderr-%s", getName()), process.getErrorStream());
      this.errGobbler.start();
      this.waitFor = new WaitForProcess(pythonStage, process);
      this.waitFor.start();

      log.info("process started {}", process);
    }
  }

  public PythonStage(Config config) {
    super(config, Spec.stage()
        .withRequiredProperties("scriptPath")
        .withOptionalProperties("pythonExecutable", "function_name", "port", "requirementsPath"));
    log.info("PythonStage constructor");
    this.scriptPath = config.getString("scriptPath");
    this.pythonExecutable = config.hasPath("pythonExecutable") ? config.getString("pythonExecutable") : "python3";
    this.requirementsPath = config.hasPath("requirementsPath") ? config.getString("requirementsPath") : null;
    if (config.hasPath("port")) {
      this.port = config.getInt("port");
      synchronized (portLock) {
        usedPorts.add(this.port);
        usedPorts.add(this.port + 1);
      }
    } else {
      // Use getNextAvailablePort for dynamic allocation, but default to 25333 if available
      int defaultPort = 25333;
      synchronized (portLock) {
        if (!usedPorts.contains(defaultPort) && !usedPorts.contains(defaultPort + 1)
            && isPortAvailable(defaultPort) && isPortAvailable(defaultPort + 1)) {
          this.port = defaultPort;
          usedPorts.add(this.port);
          usedPorts.add(this.port + 1);
        } else {
          this.port = getNextAvailablePort();
        }
      }
    }
  }

  /**
   * Ensures a Python venv exists in the current working directory and is viable. If not, creates it.
   * Sets venvPythonPath to the venv's python executable.
   * @throws StageException if venv creation or validation fails.
   */
  private void ensureVenv() throws StageException {
    java.nio.file.Path venvDir = java.nio.file.Paths.get("venv");
    java.nio.file.Path venvPython = venvDir.resolve("bin/python");
    venvPythonPath = venvPython.toAbsolutePath().toString();
    if (!java.nio.file.Files.exists(venvPython)) {
      log.info("Python venv not found, creating venv in cwd...");
      try {
        Process createVenv = new ProcessBuilder(pythonExecutable, "-m", "venv", "venv")
          .redirectErrorStream(true).start();
        StringBuilder output = new StringBuilder();
        try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(createVenv.getInputStream()))) {
          String line;
          while ((line = reader.readLine()) != null) {
            output.append(line).append("\n");
          }
        }
        int exitCode = createVenv.waitFor();
        log.info("venv creation output:\n{}", output.toString().trim());
        if (exitCode != 0) {
          throw new StageException("Failed to create venv. Output: " + output);
        }
      } catch (Exception e) {
        throw new StageException("Failed to create venv: " + e.getMessage(), e);
      }
    }
    // Check venv python is viable
    try {
      Process checkVenv = new ProcessBuilder(venvPythonPath, "--version").redirectErrorStream(true).start();
      StringBuilder output = new StringBuilder();
      try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(checkVenv.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          output.append(line).append("\n");
        }
      }
      int exitCode = checkVenv.waitFor();
      String versionOutput = output.toString().trim();
      if (exitCode != 0 || versionOutput.isEmpty() || !versionOutput.toLowerCase().contains("python")) {
        throw new StageException("venv python is not viable. Output: " + versionOutput);
      }
      log.info("venv python version: {}", versionOutput);
    } catch (Exception e) {
      throw new StageException("Failed to check venv python: " + e.getMessage(), e);
    }
  }

  /**
   * Checks if the configured Python executable is available and logs its version.
   * 
   * @throws StageException if Python is not found or not working.
   */
  private void checkPythonInstalled() throws StageException {
    try {
      Process process = new ProcessBuilder(pythonExecutable, "--version").redirectErrorStream(true).start();
      StringBuilder output = new StringBuilder();
      try (java.io.BufferedReader reader = new java.io.BufferedReader(
          new java.io.InputStreamReader(process.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          output.append(line).append("\n");
        }
      }
      int exitCode = process.waitFor();
      String versionOutput = output.toString().trim();
      if (exitCode != 0 || versionOutput.isEmpty() || !versionOutput.toLowerCase().contains("python")) {
        log.error("Python version check failed. Output:\n{}", versionOutput);
        throw new StageException(
            "Python executable '" + pythonExecutable + "' not found or not working. Output: " + versionOutput);
      }
      log.info("Detected Python version:\n{}", versionOutput);
    } catch (Exception e) {
      throw new StageException("Failed to check Python installation: " + e.getMessage(), e);
    }
  }

  /**
   * Checks if py4j is installed in the Python environment.
   * 
   * @return true if py4j is installed, false otherwise.
   * @throws StageException if the check fails for other reasons.
   */
  private boolean checkPy4jInstalled() throws StageException {
    try {
      Process process = new ProcessBuilder(venvPythonPath, "-c", "import py4j; print(py4j.__version__)")
          .redirectErrorStream(true).start();
      StringBuilder output = new StringBuilder();
      try (java.io.BufferedReader reader = new java.io.BufferedReader(
          new java.io.InputStreamReader(process.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          output.append(line).append("\n");
        }
      }
      int exitCode = process.waitFor();
      String py4jOutput = output.toString().trim();
      if (exitCode != 0 || py4jOutput.isEmpty() || py4jOutput.toLowerCase().contains("traceback")) {
        log.warn("py4j is not installed. Output:\n{}", py4jOutput);
        return false;
      }
      log.info("Detected py4j version:\n{}", py4jOutput);
      return true;
    } catch (Exception e) {
      throw new StageException("Failed to check py4j: " + e.getMessage(), e);
    }
  }

  /**
   * Attempts to install py4j using pip for the configured Python executable.
   * 
   * @throws StageException if installation fails.
   */
  private void installPy4j() throws StageException {
    try {
      log.info("Attempting to install py4j via pip...");
      Process installProc = new ProcessBuilder(venvPythonPath, "-m", "pip", "install", "py4j")
          .redirectErrorStream(true).start();
      StringBuilder installOutput = new StringBuilder();
      try (java.io.BufferedReader installReader = new java.io.BufferedReader(
          new java.io.InputStreamReader(installProc.getInputStream()))) {
        String line;
        while ((line = installReader.readLine()) != null) {
          installOutput.append(line).append("\n");
        }
      }
      int installExit = installProc.waitFor();
      log.info("py4j pip install output:\n{}", installOutput.toString().trim());
      if (installExit != 0) {
        throw new StageException("Failed to install py4j via pip. Output: " + installOutput);
      }
    } catch (Exception e) {
      throw new StageException("Failed to install py4j: " + e.getMessage(), e);
    }
  }

  /**
   * Logs the Python executable path and sys.path entries for debugging.
   * 
   * @throws StageException if logging fails.
   */
  private void logPythonEnvironment() throws StageException {
    try {
      // Get the Python executable path using venv python
      Process execPathProc = new ProcessBuilder(venvPythonPath, "-c", "import sys; print(sys.executable)")
          .redirectErrorStream(true).start();
      StringBuilder execPathOutput = new StringBuilder();
      try (java.io.BufferedReader execPathReader = new java.io.BufferedReader(
          new java.io.InputStreamReader(execPathProc.getInputStream()))) {
        String line;
        while ((line = execPathReader.readLine()) != null) {
          execPathOutput.append(line).append("\n");
        }
      }
      execPathProc.waitFor();
      log.info("Python executable path (from sys.executable):\n{}", execPathOutput.toString().trim());

      // Log all sys.path entries
      Process sysPathProc = new ProcessBuilder(venvPythonPath, "-u", "-c", "import sys; [print(p) for p in sys.path]")
          .redirectErrorStream(true).start();
      StringBuilder sysPaths = new StringBuilder();
      try (java.io.BufferedReader sysPathReader = new java.io.BufferedReader(
          new java.io.InputStreamReader(sysPathProc.getInputStream()))) {
        String line;
        while ((line = sysPathReader.readLine()) != null) {
          sysPaths.append(line).append("\n");
        }
      }
      sysPathProc.waitFor();
      log.info("Python sys.path entries:\n{}", sysPaths.toString().trim());
    } catch (Exception e) {
      log.error("Error logging Python environment", e);
      throw new StageException("Failed to log Python environment: " + e.getMessage(), e);
    }
  }

  /**
   * Installs Python requirements from requirementsPath using pip in the venv, if requirementsPath is set.
   * Logs output and throws StageException on failure.
   */
  private void installRequirementsIfNeeded() throws StageException {
    if (requirementsPath == null || requirementsPath.trim().isEmpty()) {
      log.info("No requirementsPath specified; skipping requirements installation.");
      return;
    }
    log.info("Installing Python requirements from {} using venv python {}", requirementsPath, venvPythonPath);
    try {
      Process installProc = new ProcessBuilder(
        venvPythonPath, "-m", "pip", "install", "-r", requirementsPath)
        .redirectErrorStream(true)
        .start();
      StringBuilder installOutput = new StringBuilder();
      try (java.io.BufferedReader installReader = new java.io.BufferedReader(
          new java.io.InputStreamReader(installProc.getInputStream()))) {
        String line;
        while ((line = installReader.readLine()) != null) {
          installOutput.append(line).append("\n");
        }
      }
      int installExit = installProc.waitFor();
      log.info("pip install -r output:\n{}", installOutput.toString().trim());
      if (installExit != 0) {
        throw new StageException("Failed to install requirements from " + requirementsPath + ". Output: " + installOutput);
      }
    } catch (Exception e) {
      throw new StageException("Failed to install requirements from " + requirementsPath + ": " + e.getMessage(), e);
    }
  }

  /**
   * Starts the PythonStage, launching the Python process and setting up Py4J
   * integration.
   * 
   * @throws StageException if setup fails.
   */
  @Override
  public void start() throws StageException {
    log.info("start() called");
    log.info("scriptPath: {}", scriptPath);
    log.info("pythonExecutable: {}", pythonExecutable);
    log.info("port: {}", port);
    checkPythonInstalled();
    ensureVenv();
    logPythonEnvironment();
    if (!checkPy4jInstalled()) {
      installPy4j();
      if (!checkPy4jInstalled()) {
        throw new StageException("py4j is still not installed after pip install.");
      }
    }
    installRequirementsIfNeeded();
    if (started.compareAndSet(false, true)) {
      boolean portAvailable = true;
      try (ServerSocket ignored = new ServerSocket(port)) {
        log.info("Port {} is available", port);
      } catch (IOException e) {
        log.error("port {} is already in use", port, e);
        portAvailable = false;
      }
      if (!portAvailable) {
        throw new StageException(String.format("Port %d is already in use. Assuming a GatewayServer is already running and reconnecting to it.", port));
      }

      log.info("Starting Py4J GatewayServer on port {}", port);
      gateway = new GatewayServer(this, port);
      gateway.start();
      gateway.addListener(this); // Register listener so connectionStarted and others are called
      log.info("GatewayServer started");

      handler = (Py4jExecutorInterface) gateway.getPythonServerEntryPoint(new Class[] { Py4jExecutorInterface.class });

      try {
        // Use Py4jClient.py from main/resources
        String py4jClientScript = Objects.requireNonNull(getClass().getClassLoader().getResource("Py4jClient.py"))
            .getPath();
        log.info("Using Py4jClient.py script at {}", py4jClientScript);

        log.info("Launching Python process: {} {} --script-path {} --port {}", venvPythonPath, py4jClientScript, scriptPath, port);
        pythonProcess = new ProcessBuilder(
            venvPythonPath,
            "-u", // do not buffer python stdout
            py4jClientScript,
            "--script-path", scriptPath,
            "--port", String.valueOf(port))
            .redirectErrorStream(true)
            .start();
        log.info("Python process started");

        py4jClient = new Py4jClient(this, pythonProcess);

        log.info("py4jclient created");

      } catch (Exception e) {
        log.error("Failed to launch Python process for Py4J integration", e);
        throw new StageException("Failed to launch Python process for Py4J integration", e);
      }
    }
  }

  /**
   * Processes a document by sending it to Python, updating it with the returned
   * fields.
   * 
   * @param doc the document to process
   * @return always null (no child documents)
   * @throws StageException if processing fails
   */
  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    log.info("processDocument");
    // Wait for connection to be ready, up to 5 seconds
    long start = System.currentTimeMillis();
    while (!ready && (System.currentTimeMillis() - start < 5000)) {
      try {
        Thread.sleep(500);
        log.info("Waiting for Py4J connection to be ready");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new StageException("Interrupted while waiting for Py4J connection to be ready", e);
      }
    }
    if (!ready) {
      throw new StageException("Py4J connection was not established within 5 seconds");
    }
    try {
      Map<String, Object> msg = new HashMap<>();
      msg.put("method", "process_document");
      msg.put("data", doc);
      String msgJson = mapper.writeValueAsString(msg);
      log.info("JSON encoded doc: {}", msgJson);
      // Call Python and get the updated document as a JSON string
      String jsonReturn = (String) handler.exec(msgJson); // Assumes send returns the result
      if (jsonReturn != null) {

        @SuppressWarnings("unchecked")
        Map<String, Object> updatedFields = mapper.readValue(jsonReturn, Map.class);

        Set<String> reserved = com.kmwllc.lucille.core.Document.RESERVED_FIELDS;
        for (String field : doc.getFieldNames()) {
          if (!reserved.contains(field)) {
            doc.removeField(field);
          }
        }

        for (Map.Entry<String, Object> entry : updatedFields.entrySet()) {
          if (!reserved.contains(entry.getKey())) {
            doc.setField(entry.getKey(), entry.getValue());
          }
        }
      }
      if (log.isDebugEnabled()) {
        log.debug("doc {}", doc);
      }
      return null;
    } catch (Exception e) {
      log.error("Failed to update Document from Python return", e);
      throw new StageException("Failed to update Document from Python return", e);
    }
  }

  /**
   * Stops the PythonStage, shutting down the GatewayServer and Python process.
   * 
   * @throws StageException if shutdown fails.
   */
  @Override
  public void stop() throws StageException {
    log.info("stop called - shutting down PythonStage gateway and python process");
    if (gateway != null) {
      gateway.shutdown();
      gateway = null;
    }
    if (pythonProcess != null) {
      pythonProcess.destroy();
      pythonProcess = null;
    }
    releasePort(this.port);
  }

  /**
   * Called when a connection error occurs with the Py4J GatewayServer.
   * 
   * @param e the exception that occurred
   */
  @Override
  public void connectionError(Exception e) {

    log.error("connectionError: {}", e.getMessage(), e);
  }

  /**
   * Called when a new connection is established with the Py4J GatewayServer.
   * 
   * @param gatewayConnection the connection object
   */
  @Override
  public void connectionStarted(Py4JServerConnection gatewayConnection) {
    log.info("Connection established: {}", gatewayConnection);
    try {
      log.info("connectionStarted {}", gatewayConnection.toString());
      ready = true;
      log.info("========== connection ready {}", gatewayConnection.toString()); 
    } catch (Exception e) {
      log.error("connectionStarted {}", e.getMessage(), e);
    }
  }

  /**
   * Called when a connection to the Py4J GatewayServer is stopped.
   * 
   * @param gatewayConnection the connection object
   */
  @Override
  public void connectionStopped(Py4JServerConnection gatewayConnection) {
    log.info("Connection lost: {}", gatewayConnection);
    py4jClient = null;
    ready = false;
  }

  /**
   * Called when a server error occurs in the Py4J GatewayServer.
   * 
   * @param e the exception that occurred
   */
  @Override
  public void serverError(Exception e) {
    log.error("serverError: {}", e.getMessage(), e);
  }

  /**
   * Called after the Py4J GatewayServer has shut down.
   */
  @Override
  public void serverPostShutdown() {
    log.info("serverPostShutdown");
  }

  /**
   * Called before the Py4J GatewayServer is about to shut down.
   */
  @Override
  public void serverPreShutdown() {
    log.info("serverPreShutdown");
  }

  /**
   * Called when the Py4J GatewayServer has started.
   */
  @Override
  public void serverStarted() {
    log.info("serverStarted");
  }

  /**
   * Called when the Py4J GatewayServer has stopped.
   */
  @Override
  public void serverStopped() {
    log.info("serverStopped");
  }

  /**
   * Sink for standard output from Py4j-related subprocesses. This method
   * immediately publishes the output on the logger.
   *
   * @param msg The output from a py4j related subprocess.
   */
  public void handleStdOut(String msg) {
    log.info("Py4jClient -> {}", msg);
  }

  /**
   * Returns the next available port pair (gateway and callback), checking for OS-level availability and thread safety.
   */
  private static int getNextAvailablePort() {
    synchronized (portLock) {
      int candidate = nextPort;
      while (true) {
        // Check both candidate (gateway) and candidate+1 (callback) for availability
        if (!usedPorts.contains(candidate) && !usedPorts.contains(candidate + 1)
            && isPortAvailable(candidate) && isPortAvailable(candidate + 1)) {
          usedPorts.add(candidate);
          usedPorts.add(candidate + 1);
          nextPort = candidate + 2;
          return candidate;
        }
        candidate += 2;
      }
    }
  }

  /**
   * Checks if a port is available on the system.
   */
  private static boolean isPortAvailable(int port) {
    try (ServerSocket ignored = new ServerSocket(port)) {
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Releases a port and its callback port when a stage is stopped.
   */
  private static void releasePort(int port) {
    synchronized (portLock) {
      usedPorts.remove(port);
      usedPorts.remove(port + 1);
    }
  }
}
