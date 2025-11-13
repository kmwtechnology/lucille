package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.stage.util.Py4JRuntime;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class PythonStage extends Stage {

  private static final Logger log = LoggerFactory.getLogger(PythonStage.class);

  private final String scriptPath;
  private final String pythonExecutable;
  private final String requirementsPath;
  private final String functionName;
  private final int port;
  private final int readinessTimeoutMs;

  private Py4JRuntime runtime;
  private final ObjectMapper mapper = new ObjectMapper();

  public PythonStage(Config config) {
    super(config, Spec.stage()
        .withRequiredProperties("scriptPath")
        .withOptionalProperties("pythonExecutable", "function_name", "port", "requirementsPath", "timeout_ms"));

    this.scriptPath = config.getString("scriptPath");
    this.pythonExecutable = config.hasPath("pythonExecutable") ? config.getString("pythonExecutable") : "python3";
    this.requirementsPath = config.hasPath("requirementsPath") ? config.getString("requirementsPath") : null;
    this.functionName = config.hasPath("function_name") ? config.getString("function_name") : "process_document";
    this.port = config.hasPath("port") ? config.getInt("port") : 0;
    this.readinessTimeoutMs = config.hasPath("timeout_ms") ? config.getInt("timeout_ms") : 5000;
  }

  @Override
  public void start() throws StageException {
    log.info("Starting PythonStage; scriptPath={}, pythonExecutable={}, port={}, timeout_ms={}",
        scriptPath, pythonExecutable, (port == 0 ? "(auto)" : port), readinessTimeoutMs);
    runtime = new Py4JRuntime(pythonExecutable, scriptPath, requirementsPath, port);
    runtime.start();
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    long startWait = System.currentTimeMillis();
    while (!runtime.isReady() && (System.currentTimeMillis() - startWait < readinessTimeoutMs)) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new StageException("Interrupted while waiting for Py4J connection to be ready", e);
      }
    }
    if (!runtime.isReady()) {
      throw new StageException("Py4J connection not ready within " + readinessTimeoutMs + " ms");
    }

    try {
      Map<String, Object> msg = new HashMap<>();
      msg.put("method", functionName);
      msg.put("data", doc);

      String requestJson = mapper.writeValueAsString(msg);
      String responseJson = runtime.exec(requestJson);

      if (responseJson != null && !responseJson.isEmpty()) {
        @SuppressWarnings("unchecked")
        Map<String, Object> updatedFields = mapper.readValue(responseJson, Map.class);

        Set<String> reserved = Document.RESERVED_FIELDS;
        for (Map.Entry<String, Object> e : updatedFields.entrySet()) {
          if (!reserved.contains(e.getKey())) {
            doc.setField(e.getKey(), e.getValue());
          }
        }
      }

      if (log.isDebugEnabled()) {
        log.debug("Processed doc {}", doc);
      }
      return null;

    } catch (StageException se) {
      throw se;
    } catch (Exception e) {
      throw new StageException("Failed to process document via Python", e);
    }
  }

  @Override
  public void stop() throws StageException {
    log.info("Stopping PythonStage");
    if (runtime != null) {
      runtime.stop();
      runtime = null;
    }
  }
}
