package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.stage.util.Py4JRuntime;
import com.kmwllc.lucille.stage.util.Py4JRuntimeManager;
import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delegates document processing to a user-provided Python script via a Py4JRuntime. The stage sends each
 * Document to a python function, waits for a JSON response, and applies any returned field updates to the
 * document.
 * <p>
 * Config Parameters:
 * <ul>
 *   <li>scriptPath (String, Required) : Path to the python script that contains the processing function. NOTE: The path must be
 *    within one of these specific directories: ./src/test/resources, ./src/main/resources, ./python, or ./src/test/resources/ExternalPythonTest.
 *   </li>
 *   <li>pythonExecutable (String, Optional) : Path of the base Python executable used to create and manage
 *   the virtual environment. Defaults to python3.</li>
 *   <li>requirementsPath (String, Optional) : Path to a requirements.txt file whose dependencies will be
 *   installed into the managed virtual environment before the Python script is started.</li>
 *   <li>functionName (String, Optional) : Name of the Python function to call for each document. Defaults to
 *   process_document.</li>
 *   <li>port (Integer, Optional) : Explicit base port to use for the Py4J gateway. If omitted, a free port
 *   pair is automatically selected starting from the default range.</li>
 * </ul>
 */
public final class ExternalPython extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredString("scriptPath")
      .optionalString("pythonExecutable", "functionName", "port", "requirementsPath")
      .optionalNumber("port")
      .build();

  private static final Logger log = LoggerFactory.getLogger(ExternalPython.class);

  private final String scriptPath;
  private final String pythonExecutable;
  private final String requirementsPath;
  private final String functionName;
  private final Integer port;
  private Py4JRuntime runtime;
  private final ObjectMapper mapper = new ObjectMapper();

  public ExternalPython(Config config) {
    super(config);

    this.scriptPath = config.getString("scriptPath");
    this.pythonExecutable = config.hasPath("pythonExecutable") ? config.getString("pythonExecutable") : "python3";
    this.requirementsPath = config.hasPath("requirementsPath") ? config.getString("requirementsPath") : null;
    this.functionName = config.hasPath("functionName") ? config.getString("functionName") : "process_document";
    this.port = config.hasPath("port") ? config.getInt("port") : null;
  }

  @Override
  public void start() throws StageException {
    runtime = Py4JRuntimeManager.getInstance().acquire(pythonExecutable, scriptPath, requirementsPath, port);

    if (!runtime.isReady()) {
      throw new StageException("Py4J connection not ready after start");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    try {
      Map<String, Object> msg = new HashMap<>();
      msg.put("method", functionName);
      msg.put("data", doc);

      String requestJson = mapper.writeValueAsString(msg);
      String responseJson = runtime.exec(requestJson);

      if (responseJson != null && !responseJson.isEmpty()) {
        JsonNode node = mapper.readTree(responseJson);

        // update doc with fields from response
        List<String> responseFieldNames = IteratorUtils.toList(node.fieldNames());
        for (String fieldName : responseFieldNames) {
          if (Document.RESERVED_FIELDS.contains(fieldName)) {
            continue;
          }
          JsonNode value = node.get(fieldName);
          if (value.isValueNode()) {
            doc.setField(fieldName, value);
          } else {
            doc.setNestedJson(fieldName, value);
          }
        }

        // remove fields not present in response
        for (String fieldName : doc.getFieldNames()) {
          if (Document.RESERVED_FIELDS.contains(fieldName)) {
            continue;
          }
          if (!responseFieldNames.contains(fieldName)) {
            doc.removeField(fieldName);
          }
        }
      }
      return null;
    } catch (Exception e) {
      throw new StageException("Failed to process document via Python", e);
    }
  }

  @Override
  public void stop() throws StageException {
    if (runtime != null) {
      Py4JRuntimeManager.getInstance().release();
      runtime = null;
    }
  }
}
