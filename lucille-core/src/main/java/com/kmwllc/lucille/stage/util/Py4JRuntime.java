package com.kmwllc.lucille.stage.util;

import com.kmwllc.lucille.core.StageException;
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

  @FunctionalInterface
  private interface PyExec {
    Object exec(String json);
  }

  private static final Set<Integer> usedPorts = new HashSet<>();
  private static int nextPort = 25333; // Default Py4J port

  private final String pythonExecutable;
  private final String scriptPath;
  private final String requirementsPath;

  private final Integer configuredPort;
  private Integer boundPort = null;

  private String venvPythonPath;
  private GatewayServer gateway;
  private volatile PyExec handler;
  private Process pythonProcess;
  private StreamGobbler procGobbler;

  public Py4JRuntime(String pythonExecutable, String scriptPath, String requirementsPath, Integer configuredPort) {
    this.pythonExecutable = pythonExecutable;
    this.scriptPath = scriptPath;
    this.requirementsPath = (requirementsPath != null && !requirementsPath.isBlank()) ? requirementsPath : null;
    this.configuredPort = configuredPort;
  }

  public void start() throws StageException {
    if (isReady()) {
      log.warn("Py4J runtime already started");
      return;
    }

    checkPythonInstalled();
    ensureVenv();
    logPythonEnvironment();
    ensurePy4JInVenv();
    installRequirementsIfNeeded();

    this.boundPort = allocatePort(configuredPort);
    startGateway(boundPort);
    startPythonProcess(boundPort);
  }

  public void stop() throws StageException {
    System.out.println("HERE");
    handler = null;
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
      releasePort(boundPort);
      boundPort = null;
    }
  }

  public boolean isReady() {
    return handler != null && pythonProcess != null && pythonProcess.isAlive();
  }

  public String exec(String requestJson) throws StageException {
    if (!isReady()) {
      throw new StageException("Py4J handler not ready");
    }

    try {
      Object res = handler.exec(requestJson);
      return (res instanceof String) ? (String) res : (res == null ? null : String.valueOf(res));
    } catch (Exception e) {
      throw new StageException("Error calling Python via Py4J", e);
    }
  }

  private void startGateway(int port) throws StageException {
    try {
      gateway = new GatewayServer(this, port);
      gateway.start();
      log.info("GatewayServer started on port {}", port);
    } catch (Exception e) {
      releasePort(port);
      throw new StageException("Failed to start GatewayServer on port " + port, e);
    }
  }

  private void startPythonProcess(int port) throws StageException {
    try (InputStream in = Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("Py4jClient.py"),
        "Py4jClient.py not found on classpath resources")) {
      Path clientPath = Paths.get("Py4jClient.py").toAbsolutePath();

      synchronized (Py4JRuntime.class) {
        if (!Files.exists(clientPath)) {
          Files.copy(in, clientPath, StandardCopyOption.REPLACE_EXISTING);
        }
      }

      ProcessBuilder pb = new ProcessBuilder(
          venvPythonPath,
          "-u",
          clientPath.toAbsolutePath().toString(),
          "--script-path", scriptPath,
          "--port", String.valueOf(port)
      ).redirectErrorStream(true);

      log.info("Launching Python: {} {} --script-path {} --port {}",
          venvPythonPath, clientPath.toAbsolutePath(), scriptPath, port);

      pythonProcess = pb.start();

      procGobbler = new StreamGobbler("Py4J-python", pythonProcess.getInputStream());
      procGobbler.start();

      Thread waiter = new Thread(() -> {
        try {
          int code = pythonProcess.waitFor();
          log.warn("Python process exited with code {}", code);
        } catch (InterruptedException ignored) {
          Thread.currentThread().interrupt();
        }
      }, "Py4J-python-waiter");
      waiter.setDaemon(true);
      waiter.start();

      Thread receiver = new Thread(() -> {
        try {
          handler = (PyExec) gateway.getPythonServerEntryPoint(new Class[] {PyExec.class });
          log.info("Resolved Python entry point");
        } catch (Exception e) {
          try {
            throw new StageException("Failed to resolve Python server entry point", e);
          } catch (StageException ex) {
            throw new RuntimeException(ex);
          }
        }
      }, "py4j-receiver");
      receiver.setDaemon(true);
      receiver.start();
      receiver.join();

      while (!procGobbler.isStartMessageSeen()) {
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

  private void ensureVenv() throws StageException {
    Path venvDir = java.nio.file.Paths.get("venv");
    Path venvPython = venvDir.resolve("bin/python");
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

  private void ensurePy4JInVenv() throws StageException {
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
      int pipCode = pip.waitFor();
      if (pipCode != 0) {
        throw new StageException("pip install py4j failed (exit=" + pipCode + ")");
      }
    } catch (Exception e) {
      throw new StageException("Failed to ensure py4j in venv: " + e.getMessage(), e);
    }
  }

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

  private static synchronized int allocatePort(Integer configured) throws StageException {
    if (configured != null && configured > 0) {
      if (!isPortAvailable(configured) || !isPortAvailable(configured + 1)) {
        throw new StageException("Requested port range " + configured + "-" + (configured + 1) + " is not available");
      }

      usedPorts.add(configured);
      usedPorts.add(configured + 1);

      return configured;
    }

    int candidate = nextPort;
    while (true) {
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

  private static boolean isPortAvailable(int port) {
    try (ServerSocket ignored = new ServerSocket(port)) {
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private static synchronized void releasePort(int port) {
    usedPorts.remove(port);
    usedPorts.remove(port + 1);
  }
}
