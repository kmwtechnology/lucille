package com.kmwllc.lucille.stage.util;

import com.kmwllc.lucille.core.StageException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.GatewayServer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;


public final class Py4JRuntime {

  private static final Logger log = LoggerFactory.getLogger(Py4JRuntime.class);

  public static final String PYTHON_CLIENT_RESOURCE_NAME = "Py4jClient.py";
  public static final String PYTHON_CLIENT_COPY_NAME = "Py4jClientCopy.py";

  /**
   * Defines methods that can be called on the python client via Py4J.
   *
   * Here, exec() represents Py4jClient.exec(self, json_msg) in Py4jClient.py
   */
  private interface PythonClient {
    Object exec(String json);
  }

  private final Set<Integer> usedPorts = new HashSet<>();
  private int nextPort = 25333; // Default Py4J port

  private final String pythonExecutable;
  private final String scriptPath;
  private final String requirementsPath;
  private final Integer requestedPort;
  private Integer boundPort = null;
  private String venvPythonPath;
  private GatewayServer gateway;
  private volatile PythonClient pythonClient;
  private volatile Process pythonProcess;
  private StreamConsumer pythonProcessOutputConsumer;

  public Py4JRuntime(String pythonExecutable, String scriptPath, String requirementsPath, Integer requestedPort) {
    this.pythonExecutable = pythonExecutable;
    this.scriptPath = scriptPath;
    this.requirementsPath = requirementsPath;
    this.requestedPort = requestedPort;
  }

  public String getPythonExecutable() {
    return pythonExecutable;
  }

  public String getScriptPath() {
    return scriptPath;
  }

  public String getRequirementsPath() {
    return requirementsPath;
  }

  public Integer getRequestedPort() {
    return requestedPort;
  }

  public void start() throws StageException {
    if (isReady()) {
      log.info("Py4J runtime already started");
      return;
    }

    checkPythonInstalled();
    ensureVenv();
    logPythonEnvironment();
    ensurePy4JInVenv();
    installRequirementsIfNeeded();

    boundPort = allocatePort(requestedPort);
    gateway = startGateway(boundPort);
    startPythonProcess(boundPort);
  }

  public void stop() throws StageException {
    pythonClient = null;
    if (gateway != null) {
      try {
        gateway.shutdown();
      } catch (Exception e) {
        log.warn("Error shutting down GatewayServer", e);
      } finally {
        gateway = null;
      }
    }

    if (pythonProcess != null) {
      pythonProcess.destroy();
      try {
        if (!pythonProcess.waitFor(1500, java.util.concurrent.TimeUnit.MILLISECONDS)) {
          pythonProcess.destroyForcibly();
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      } finally {
        pythonProcess = null;
      }
    }

    if (boundPort != null) {
      unmarkPort(boundPort);
      boundPort = null;
    }
  }

  public boolean isReady() {
    return pythonClient != null && pythonProcess != null && pythonProcess.isAlive();
  }

  public String exec(String requestJson) throws StageException {
    if (!isReady()) {
      throw new StageException("Py4J handler not ready");
    }

    try {
      Object res = pythonClient.exec(requestJson);
      return (res instanceof String) ? (String) res : (res == null ? null : String.valueOf(res));
    } catch (Exception e) {
      throw new StageException("Error calling Python via Py4J", e);
    }
  }

  private static GatewayServer startGateway(int port) throws StageException {
    try {
      GatewayServer gateway = new GatewayServer(
        null,
        port,
        port + 1,
        GatewayServer.defaultAddress(),
        GatewayServer.defaultAddress(),
        GatewayServer.DEFAULT_CONNECT_TIMEOUT,
        GatewayServer.DEFAULT_READ_TIMEOUT,
        null);
      gateway.start();
      return gateway;
    } catch (Exception e) {
      throw new StageException("Failed to start GatewayServer on port " + port, e);
    }
  }

  // Py4jClientCopy.py is recreated each run in case the python file is modified. Other files persist.
  private void startPythonProcess(int port) throws StageException {
    try (InputStream in = Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream(PYTHON_CLIENT_RESOURCE_NAME),
        PYTHON_CLIENT_RESOURCE_NAME + "not found on classpath resources")) {
      Path pythonDir = Paths.get("python").toAbsolutePath();
      Files.createDirectories(pythonDir);
      Path clientPath = pythonDir.resolve(PYTHON_CLIENT_COPY_NAME);

      synchronized (Py4JRuntime.class) {
        if (!Files.exists(clientPath)) {
          log.info("Copying python client script to: " + clientPath);
          Files.copy(in, clientPath, StandardCopyOption.REPLACE_EXISTING);
        } else {
          log.warn("Found python client script at " + clientPath + ". Keeping current version.");
        }
      }

      Path scriptPathObj = Paths.get(scriptPath);
      String scriptDir = scriptPathObj.getParent().toAbsolutePath().toString();
      String scriptName = scriptPathObj.getFileName().toString();

      ProcessBuilder processBuilder = new ProcessBuilder(
          venvPythonPath,
          "-u",
          clientPath.toAbsolutePath().toString(),
          "--script-name", scriptName,
          "--script-dir", scriptDir,
          "--port", String.valueOf(port)
      ).redirectErrorStream(true);

      log.info("Launching Python: {} {} --script-name {} --script-dir {} --port {}",
          venvPythonPath, clientPath.toAbsolutePath(), scriptName, scriptDir, port);

      pythonProcess = processBuilder.start();

      pythonProcessOutputConsumer = new StreamConsumer(pythonProcess.getInputStream(), "JavaGateway STARTED");
      pythonProcessOutputConsumer.start();

      Thread waiter = new Thread(() -> {
        try {
          int code = pythonProcess.waitFor();
          log.info("Python process exited with code {}", code);
        } catch (InterruptedException ignored) {
          Thread.currentThread().interrupt();
        }
      }, "Py4J-python-waiter");
      waiter.setDaemon(true);
      waiter.start();

      try {
        pythonClient = (PythonClient) gateway.getPythonServerEntryPoint(new Class[] {PythonClient.class });
      } catch (Exception e) {
        throw new RuntimeException("Failed to resolve Python server entry point", e);
      }

      while (!pythonProcessOutputConsumer.isMessageSeen()) {
        // if process is no longer alive and exited with code 1, stop waiting.
        if (!pythonProcess.isAlive() && pythonProcess.exitValue() == 1) {
          throw new StageException();
        }

        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    } catch (Exception e) {
      throw new StageException("Failed to launch Python process", e);
    }
  }

  private void checkPythonInstalled() throws StageException {
    int exitCode;
    String versionOutput;
    try {
      Process process = new ProcessBuilder(pythonExecutable, "--version").redirectErrorStream(true).start();
      StringBuilder output = new StringBuilder();
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          output.append(line).append("\n");
        }
      }
      exitCode = process.waitFor();
      versionOutput = output.toString().trim();
      log.info("Detected Python version: {}", versionOutput);
    } catch (Exception e) {
      throw new StageException("Failed to check Python installation: " + e.getMessage(), e);
    }

    if (exitCode != 0 || versionOutput.isEmpty() || !versionOutput.toLowerCase().contains("python")) {
      log.error("Python version check failed. Output:\n{}", versionOutput);
      throw new StageException(
          "Python executable '" + pythonExecutable + "' not found or not working. Output: " + versionOutput);
    }
  }

  // Note: python/venv is intentionally not removed by Maven clean.
  // Rebuilding the venv is slow and expensive with many libraries so only Py4jClientCopy.py is deleted.
  private void ensureVenv() throws StageException {
    Path pythonDir = Paths.get("python").toAbsolutePath();
    try {
      Files.createDirectories(pythonDir);
    } catch (IOException e) {
      throw new StageException("Failed to create python directory: " + pythonDir, e);
    }

    Path venvDir = pythonDir.resolve("venv");
    Path venvPython = venvDir.resolve("bin/python");
    venvPythonPath = venvPython.toAbsolutePath().toString();
    if (!Files.exists(venvPython)) {
      log.info("Python venv not found, creating venv in cwd...");
      int exitCode;
      StringBuilder output;
      try {
        Process createVenv = new ProcessBuilder(pythonExecutable, "-m", "venv", venvDir.toString())
            .redirectErrorStream(true).start();
        output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(createVenv.getInputStream()))) {
          String line;
          while ((line = reader.readLine()) != null) {
            output.append(line).append("\n");
          }
        }
        exitCode = createVenv.waitFor();
        log.info("venv creation output:\n{}", output.toString().trim());
      } catch (Exception e) {
        throw new StageException("Failed to create venv: " + e.getMessage(), e);
      }
      if (exitCode != 0) {
        throw new StageException("Failed to create venv. Output: " + output);
      }
    }
    // Check venv python is viable
    int exitCode;
    String versionOutput;
    try {
      Process checkVenv = new ProcessBuilder(venvPythonPath, "--version").redirectErrorStream(true).start();
      StringBuilder output = new StringBuilder();
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(checkVenv.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          output.append(line).append("\n");
        }
      }
      exitCode = checkVenv.waitFor();
      versionOutput = output.toString().trim();
      log.info("venv python version: {}", versionOutput);
    } catch (Exception e) {
      throw new StageException("Failed to check venv python: " + e.getMessage(), e);
    }
    if (exitCode != 0 || versionOutput.isEmpty() || !versionOutput.toLowerCase().contains("python")) {
      throw new StageException("venv python is not viable. Output: " + versionOutput);
    }
  }

  private void ensurePy4JInVenv() throws StageException {
    int pipCode;
    try {
      Process check = new ProcessBuilder(venvPythonPath, "-c", "import py4j,sys;print(py4j.__version__)")
          .redirectErrorStream(true).start();
      String out;
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(check.getInputStream(), StandardCharsets.UTF_8))) {
        out = reader.readLine();
      }
      int code = check.waitFor();
      if (code == 0 && out != null && !out.toLowerCase().contains("traceback")) {
        log.info("Detected py4j in venv: {}", out);
        return;
      }
      log.info("Installing py4j into venv...");
      Process pip = new ProcessBuilder(venvPythonPath, "-m", "pip", "install", "py4j")
          .redirectErrorStream(true).start();
      pipCode = pip.waitFor();
    } catch (Exception e) {
      throw new StageException("Failed to ensure py4j in venv: " + e.getMessage(), e);
    }
    if (pipCode != 0) {
      throw new StageException("pip install py4j failed (exit=" + pipCode + ")");
    }
  }

  private void installRequirementsIfNeeded() throws StageException {
    if (requirementsPath == null || requirementsPath.trim().isEmpty()) {
      log.info("No requirementsPath specified; skipping requirements installation.");
      return;
    }
    log.info("Installing Python requirements from {} using venv python {}", requirementsPath, venvPythonPath);
    int installExit;
    StringBuilder installOutput;
    try {
      Process installProc = new ProcessBuilder(
          venvPythonPath, "-m", "pip", "install", "-r", requirementsPath)
          .redirectErrorStream(true)
          .start();
      installOutput = new StringBuilder();
      try (BufferedReader installReader = new BufferedReader(new InputStreamReader(installProc.getInputStream()))) {
        String line;
        while ((line = installReader.readLine()) != null) {
          installOutput.append(line).append("\n");
        }
      }
      installExit = installProc.waitFor();
      log.info("pip install -r output:\n{}", installOutput.toString().trim());
    } catch (Exception e) {
      throw new StageException("Failed to install requirements from " + requirementsPath + ": " + e.getMessage(), e);
    }
    if (installExit != 0) {
      throw new StageException("Failed to install requirements from " + requirementsPath + ". Output: " + installOutput);
    }
  }

  private void logPythonEnvironment() throws StageException {
    try {
      // Get the Python executable path using venv python
      Process execPathProc = new ProcessBuilder(venvPythonPath, "-c", "import sys; print(sys.executable)")
          .redirectErrorStream(true).start();
      StringBuilder execPathOutput = new StringBuilder();
      try (BufferedReader execPathReader = new BufferedReader(new InputStreamReader(execPathProc.getInputStream()))) {
        String line;
        while ((line = execPathReader.readLine()) != null) {
          execPathOutput.append(line).append("\n");
        }
      }
      execPathProc.waitFor();
      log.info("Python executable path: {}", execPathOutput.toString().trim());

      // Log all sys.path entries
      Process sysPathProc = new ProcessBuilder(venvPythonPath, "-u", "-c", "import sys; [print(p) for p in sys.path]")
          .redirectErrorStream(true).start();
      StringBuilder sysPaths = new StringBuilder();
      try (BufferedReader sysPathReader = new BufferedReader(new InputStreamReader(sysPathProc.getInputStream()))) {
        String line;
        while ((line = sysPathReader.readLine()) != null) {
          sysPaths.append(line).append("\n");
        }
      }
      sysPathProc.waitFor();
      log.info("Python sys.path entries:\n{}", sysPaths.toString().trim());
    } catch (Exception e) {
      throw new StageException("Failed to log Python environment: " + e.getMessage(), e);
    }
  }

  private synchronized int allocatePort(Integer myRequestedPort) throws StageException {
    if (myRequestedPort != null && myRequestedPort > 0) {
      if (!isPortAvailable(myRequestedPort) || !isPortAvailable(myRequestedPort + 1)) {
        throw new StageException("Requested port range " + myRequestedPort + "-" + (myRequestedPort + 1) + " is not available");
      }

      usedPorts.add(myRequestedPort);
      usedPorts.add(myRequestedPort + 1);

      return myRequestedPort;
    }

    int candidatePort = nextPort;
    while (true) {
      if (!usedPorts.contains(candidatePort) && !usedPorts.contains(candidatePort + 1)
          && isPortAvailable(candidatePort) && isPortAvailable(candidatePort + 1)) {
        usedPorts.add(candidatePort);
        usedPorts.add(candidatePort + 1);
        nextPort = candidatePort + 2;
        return candidatePort;
      }
      candidatePort += 2;
    }
  }

  private static boolean isPortAvailable(int port) {
    try (ServerSocket ignored = new ServerSocket(port)) {
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private synchronized void unmarkPort(int port) {
    usedPorts.remove(port);
    usedPorts.remove(port + 1);
  }
}
