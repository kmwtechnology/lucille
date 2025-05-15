package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import py4j.GatewayServer;

import java.net.ServerSocket;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Executes a Python script using Py4J as part of a pipeline stage.
 *
 * Config Parameters:
 * <p> <b>script</b> (String, Required): Python script to execute.
 * <p> <b>function_name</b> (String, Optional): Function in the script to call. Defaults to 'process'.
 * <p> <b>args</b> (List<Object>, Optional): Arguments to pass to the function. Defaults to [].
 * <p> <b>port</b> (int, Optional): Port for the GatewayServer. Defaults to GatewayServer.DEFAULT_PORT.
 */
public class PythonStage extends Stage {

  private final String script;
  private final String functionName;
  private final List<Object> args;
  private final int port;
  private GatewayServer gatewayServer;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private Process pythonProcess; // Transient handle to the launched Python process

  public PythonStage(Config config) {
    super(config, Spec.stage()
        .withRequiredProperties("script")
        .withOptionalProperties("function_name", "args", "port"));
    this.script = config.getString("script");
    this.functionName = config.hasPath("function_name") ? config.getString("function_name") : "process";
    this.args = config.hasPath("args") ? (List<Object>) config.getAnyRefList("args") : java.util.Collections.emptyList();
    this.port = config.hasPath("port") ? config.getInt("port") : GatewayServer.DEFAULT_PORT;
  }

  @Override
  public void start() throws StageException {
    if (started.compareAndSet(false, true)) {
      boolean portAvailable = true;
      try (ServerSocket ignored = new ServerSocket(port)) {
        // Port is available
      } catch (IOException e) {
        System.out.println("port " + port + " is already in use");
        e.printStackTrace();
        portAvailable = false;
      }
      if (!portAvailable) {
        System.err.println("[PythonStage] WARNING: Port " + port + " is already in use. Assuming a GatewayServer is already running and reconnecting to it.");
        // Do not start a new GatewayServer or Python process
        gatewayServer = null;
        pythonProcess = null;
        return;
      }

      // Start the Py4J GatewayServer (assumes Python process will connect)
      gatewayServer = new GatewayServer(this, port);
      gatewayServer.start();

      try {
        // Write the script to a temporary file
        java.nio.file.Path scriptPath = java.nio.file.Files.createTempFile("pythonstage_script", ".py");
        java.nio.file.Files.writeString(scriptPath, script);

        // Build the command to launch the Python process with Py4J
        String pythonCmd = "python3";
        String gatewayScript =
            "import sys\n" +
            "from py4j.java_gateway import JavaGateway, GatewayParameters\n" +
            "sys.path.append('" + scriptPath.toAbsolutePath().toString().replace("\\", "\\\\") + "')\n" +
            "import importlib.util\n" +
            "spec = importlib.util.spec_from_file_location('user_module', '" + scriptPath.toAbsolutePath().toString().replace("\\", "\\\\") + "')\n" +
            "user_module = importlib.util.module_from_spec(spec)\n" +
            "spec.loader.exec_module(user_module)\n" +
            "gateway = JavaGateway(gateway_parameters=GatewayParameters(port=" + port + "))\n" +
            "# Now you can call user_module.<function_name> from Java\n";

        // Write the gateway script to a temporary file
        java.nio.file.Path gatewayScriptPath = java.nio.file.Files.createTempFile("pythonstage_gateway", ".py");
        java.nio.file.Files.writeString(gatewayScriptPath, gatewayScript);

        // Launch the Python process
        pythonProcess = new ProcessBuilder(pythonCmd, gatewayScriptPath.toAbsolutePath().toString())
            .redirectErrorStream(true)
            .start();
      } catch (Exception e) {
        throw new StageException("Failed to launch Python process for Py4J integration", e);
      }
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    // This is a placeholder. In a real implementation, you would send the document
    // and config to the Python process via Py4J, call the function, and update the doc.
    // For now, just set a field to show it ran.
    doc.setField("python_stage_executed", true);
    return null;
  }

  @Override
  public void stop() throws StageException {
    if (gatewayServer != null) {
      gatewayServer.shutdown();
    }
    if (pythonProcess != null) {
      pythonProcess.destroy();
      pythonProcess = null;
    }
  }
}
