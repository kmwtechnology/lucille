package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.stage.util.Py4JRuntime;
import com.kmwllc.lucille.stage.util.Py4JRuntimeManager;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class PythonStage extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredString("scriptPath")
      .optionalString("pythonExecutable", "functionName", "port", "requirementsPath")
      .optionalNumber("port", "timeoutMs")
      .build();

  private static final Logger log = LoggerFactory.getLogger(PythonStage.class);

  private final String scriptPath;
  private final String pythonExecutable;
  private final String requirementsPath;
  private final String functionName;
  private final Integer port;
  private final int readinessTimeoutMs;

  private Py4JRuntime runtime;
  private final ObjectMapper mapper = new ObjectMapper();

  public PythonStage(Config config) {
    super(config);

    this.scriptPath = config.getString("scriptPath");
    this.pythonExecutable = config.hasPath("pythonExecutable") ? config.getString("pythonExecutable") : "python3";
    this.requirementsPath = config.hasPath("requirementsPath") ? config.getString("requirementsPath") : null;
    this.functionName = config.hasPath("functionName") ? config.getString("functionName") : "process_document";
    this.port = config.hasPath("port") ? config.getInt("port") : null;
    this.readinessTimeoutMs = config.hasPath("timeoutMs") ? config.getInt("timeoutMs") : 5000;
  }

  @Override
  public void start() throws StageException {
    log.info("Starting PythonStage; scriptPath={}, pythonExecutable={}, port={}, timeout_ms={}",
        scriptPath, pythonExecutable, (port == null ? "(auto)" : port), readinessTimeoutMs);
    runtime = Py4JRuntimeManager.getInstance()
        .aquire(pythonExecutable, scriptPath, requirementsPath, port);

    if (!runtime.isReady()) {
      throw new StageException("Py4J connection not ready after start");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    try {
      if (!runtime.isReady()) {
        throw new StageException("Py4J connection not ready");
      }

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
      Py4JRuntimeManager.getInstance().release();
      runtime = null;
    }
  }
}
