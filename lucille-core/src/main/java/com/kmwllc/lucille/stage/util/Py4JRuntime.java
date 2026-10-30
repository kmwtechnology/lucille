package com.kmwllc.lucille.stage.util;

import com.kmwllc.lucille.core.StageException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.SecureRandom;
import java.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.GatewayServer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.SystemUtils;


public final class Py4JRuntime {

  private static final Logger log = LoggerFactory.getLogger(Py4JRuntime.class);

  public static final String PYTHON_CLIENT_RESOURCE_NAME = "Py4jClient.py";
  public static final String PYTHON_CLIENT_COPY_NAME = "Py4jClientCopy.py";
  private static final String VENV_LOCK_NAME = ".venv.lock";

  /**
   * Overall timeout for Python process startup in milliseconds. Configurable for testing.
   * Default is 30 seconds.
   */
  public static volatile long PYTHON_STARTUP_TIMEOUT_MS = 30000;

  /**
   * Defines methods that can be called on the python client via Py4J.
   *
   * Here, exec() represents Py4jClient.exec(self, json_msg) in Py4jClient.py
   */
  private interface PythonClient {
    Object exec(String json);
  }

  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  // Venv setup writes to a directory shared by every runtime in this working directory (including parallel surefire forks),
  // so it is serialized by this monitor within a JVM and by a file lock across JVMs.
  private static final Object VENV_SETUP_LOCK = new Object();

  private final String pythonExecutable;
  private final String scriptPath;
  private final String requirementsPath;
  private final Integer requestedPort;
  private String venvPythonPath;
  private GatewayServer gateway;
  private volatile PythonClient pythonClient;
  private volatile Process pythonProcess;
  private StreamConsumer pythonProcessOutputConsumer;
  private Thread shutdownHook;

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

  // Null until started. Reports the port the OS assigned when no port was requested.
  Integer getGatewayPort() {
    return gateway == null ? null : gateway.getListeningPort();
  }

  // Null until started. Interpreter inside the managed venv (Scripts\python.exe on Windows, bin/python elsewhere).
  String getVenvPythonPath() {
    return venvPythonPath;
  }

  public void start() throws StageException {
    if (isReady()) {
      log.info("Py4J runtime already started");
      return;
    }

    synchronized (VENV_SETUP_LOCK) {
      try (FileChannel lockChannel = openVenvLock(); FileLock ignored = lockChannel.lock()) {
        checkPythonInstalled();
        ensureVenv();
        logPythonEnvironment();
        ensurePy4JInVenv();
        installRequirementsIfNeeded();
      } catch (IOException e) {
        throw new StageException("Failed to lock Python environment", e);
      }
    }

    int[] ports = allocatePorts();
    String gatewayToken = newAuthToken();
    String callbackToken = newAuthToken();
    try {
      gateway = startGateway(ports[0], ports[1], gatewayToken, callbackToken);
      startPythonProcess(gateway.getListeningPort(), ports[1], gatewayToken, callbackToken);
    } catch (StageException | RuntimeException e) {
      cleanupFailedStart();
      throw e;
    }

    if (shutdownHook == null) {
      shutdownHook = new Thread(() -> {
        try {
          stop();
        } catch (StageException e) {
          log.warn("Error stopping Py4J runtime during JVM shutdown", e);
        }
      }, "Py4J-shutdown");
      Runtime.getRuntime().addShutdownHook(shutdownHook);
    }
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

    removeShutdownHook();
  }

  private void cleanupFailedStart() {
    try {
      stop();
    } catch (StageException e) {
      log.warn("Error cleaning up after failed Py4J start", e);
    }
  }

  private void removeShutdownHook() {
    if (shutdownHook == null) {
      return;
    }
    try {
      Runtime.getRuntime().removeShutdownHook(shutdownHook);
    } catch (IllegalStateException e) {
      // The JVM is already shutting down and this call is the hook itself.
    }
    shutdownHook = null;
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

  // Port 0 asks the OS for a free gateway port, which is read back from the started server.
  // The callback port must be known before the server is built, so it is probed here.
  private int[] allocatePorts() throws StageException {
    if (requestedPort != null && requestedPort > 0) {
      if (!isPortAvailable(requestedPort) || !isPortAvailable(requestedPort + 1)) {
        throw new StageException("Requested port range " + requestedPort + "-" + (requestedPort + 1) + " is not available");
      }
      return new int[] {requestedPort, requestedPort + 1};
    }
    return new int[] {0, findFreePort()};
  }

  private static int findFreePort() throws StageException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new StageException("Failed to find a free port", e);
    }
  }

  private static boolean isPortAvailable(int port) {
    try (ServerSocket ignored = new ServerSocket(port)) {
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private static String newAuthToken() {
    byte[] bytes = new byte[32];
    SECURE_RANDOM.nextBytes(bytes);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
  }

  private static GatewayServer startGateway(int port, int callbackPort, String authToken, String callbackAuthToken)
      throws StageException {
    try {
      InetAddress loopback = InetAddress.getLoopbackAddress();
      GatewayServer gateway = new GatewayServer.GatewayServerBuilder()
          .javaPort(port)
          .javaAddress(loopback)
          .callbackClient(callbackPort, loopback, callbackAuthToken)
          .connectTimeout(GatewayServer.DEFAULT_CONNECT_TIMEOUT)
          .readTimeout(GatewayServer.DEFAULT_READ_TIMEOUT)
          .authToken(authToken)
          .build();
      gateway.start();
      return gateway;
    } catch (Exception e) {
      throw new StageException("Failed to start GatewayServer on port " + port, e);
    }
  }

  // Py4jClientCopy.py is recreated each run so the python client always matches the jar it was shipped with.
  private void startPythonProcess(int port, int callbackPort, String authToken, String callbackAuthToken)
      throws StageException {
    try (InputStream in = Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream(PYTHON_CLIENT_RESOURCE_NAME),
        PYTHON_CLIENT_RESOURCE_NAME + "not found on classpath resources")) {
      Path pythonDir = Paths.get("python").toAbsolutePath();
      Files.createDirectories(pythonDir);
      Path clientPath = pythonDir.resolve(PYTHON_CLIENT_COPY_NAME);

      synchronized (Py4JRuntime.class) {
        log.info("Copying python client script to: " + clientPath);
        Files.copy(in, clientPath, StandardCopyOption.REPLACE_EXISTING);
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
          "--port", String.valueOf(port),
          "--callback-port", String.valueOf(callbackPort)
      ).redirectErrorStream(true);
      // Tokens go in the environment, not argv, so they are not visible in the process list or logs.
      processBuilder.environment().put("PY4J_AUTH_TOKEN", authToken);
      processBuilder.environment().put("PY4J_CALLBACK_AUTH_TOKEN", callbackAuthToken);

      log.info("Launching Python: {} {} --script-name {} --script-dir {} --port {} --callback-port {}",
          venvPythonPath, clientPath.toAbsolutePath(), scriptName, scriptDir, port, callbackPort);

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

      long startTime = System.currentTimeMillis();
      while (!pythonProcessOutputConsumer.isMessageSeen()) {
        // if process is no longer alive stop waiting.
        if (!pythonProcess.isAlive()) {
          throw new StageException("Python process exited before reporting JavaGateway startup");
        }

        // Check overall timeout
        if (PYTHON_STARTUP_TIMEOUT_MS > 0) {
          long elapsed = System.currentTimeMillis() - startTime;
          if (elapsed > PYTHON_STARTUP_TIMEOUT_MS) {
            String capturedOutput = String.join("\n", pythonProcessOutputConsumer.getCapturedOutput());
            String errorMessage = "Python process startup exceeded timeout of " + PYTHON_STARTUP_TIMEOUT_MS
                + " ms. Process may be hung or slow to initialize.\nCaptured output:\n" + capturedOutput;
            throw new StageException(errorMessage);
          }
        }

        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new StageException("Python startup interrupted", e);
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

  private static FileChannel openVenvLock() throws IOException {
    Path pythonDir = Paths.get("python").toAbsolutePath();
    Files.createDirectories(pythonDir);
    return FileChannel.open(pythonDir.resolve(VENV_LOCK_NAME), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
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
    // venv puts the interpreter in Scripts\python.exe on Windows, bin/python everywhere else
    Path venvPython = SystemUtils.IS_OS_WINDOWS
        ? venvDir.resolve("Scripts").resolve("python.exe")
        : venvDir.resolve("bin").resolve("python");
    venvPythonPath = venvPython.toAbsolutePath().toString();
    // An interrupted earlier build can leave bin/python without a usable pip, so the venv is only trusted if pip runs.
    if (!Files.exists(venvPython) || !pipAvailable(venvPythonPath)) {
      log.info("Python venv not found or incomplete, creating venv in cwd...");
      int exitCode;
      StringBuilder output;
      try {
        Process createVenv = new ProcessBuilder(pythonExecutable, "-m", "venv", "--clear", venvDir.toString())
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
    String pipOutput;
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
      pipOutput = readOutput(pip);
      pipCode = pip.waitFor();
    } catch (Exception e) {
      throw new StageException("Failed to ensure py4j in venv: " + e.getMessage(), e);
    }
    if (pipCode != 0) {
      log.error("pip install py4j failed. Output:\n{}", pipOutput);
      throw new StageException("pip install py4j failed (exit=" + pipCode + "). Output: " + pipOutput);
    }
  }

  private static boolean pipAvailable(String python) {
    try {
      Process process = new ProcessBuilder(python, "-m", "pip", "--version").redirectErrorStream(true).start();
      readOutput(process);
      return process.waitFor() == 0;
    } catch (IOException e) {
      return false;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  private static String readOutput(Process process) throws IOException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
      return reader.lines().collect(Collectors.joining("\n"));
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

}
