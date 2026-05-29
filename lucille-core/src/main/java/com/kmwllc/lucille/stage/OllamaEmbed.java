package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import io.github.ollama4j.OllamaAPI;
import io.github.ollama4j.models.embeddings.OllamaEmbedRequestBuilder;
import io.github.ollama4j.models.embeddings.OllamaEmbedRequestModel;
import io.github.ollama4j.models.embeddings.OllamaEmbedResponseModel;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates vector embeddings using a local Ollama server. Supports any embedding model
 * available in Ollama (e.g. mxbai-embed-large, nomic-embed-text, snowflake-arctic-embed2).
 *
 * <p>Requires a running Ollama server with the specified model pulled.
 *
 * <p>Config Parameters -
 * <ul>
 *   <li>hostURL (String) : URL of the Ollama server (e.g. "http://localhost:11434").</li>
 *   <li>modelName (String) : Name of the Ollama embedding model (e.g. "mxbai-embed-large").</li>
 *   <li>source (String) : Field to retrieve text from for embedding.</li>
 *   <li>dest (String, Optional) : Field to store the embedding vector. Defaults to "embeddings".</li>
 *   <li>timeout (Long, Optional) : Request timeout in seconds. Uses Ollama default (10s) if not set.</li>
 * </ul>
 */
public class OllamaEmbed extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredString("hostURL", "modelName", "source")
      .optionalString("dest")
      .optionalNumber("timeout")
      .build();

  private static final Logger log = LoggerFactory.getLogger(OllamaEmbed.class);

  private final String hostURL;
  private final String modelName;
  private final String source;
  private final String dest;
  private final Long timeout;

  private OllamaAPI ollamaAPI;

  public OllamaEmbed(Config config) {
    super(config);
    this.hostURL = config.getString("hostURL");
    this.modelName = config.getString("modelName");
    this.source = config.getString("source");
    this.dest = ConfigUtils.getOrDefault(config, "dest", "embeddings");
    this.timeout = config.hasPath("timeout") ? config.getLong("timeout") : null;
  }

  @Override
  public void start() throws StageException {
    this.ollamaAPI = new OllamaAPI(hostURL);
    if (timeout != null) {
      ollamaAPI.setRequestTimeoutSeconds(timeout);
    }

    try {
      if (!ollamaAPI.ping()) {
        log.warn("Could not connect to Ollama server at {}. The server must be running for this stage to work.", hostURL);
      } else if (!ollamaAPI.listModels().stream().anyMatch(m -> m.getName().startsWith(modelName))) {
        log.warn("Ollama server is reachable, but model '{}' was not found. Please ensure it has been pulled.", modelName);
      }
    } catch (Exception e) {
      log.warn("Failed to check Ollama connectivity during startup: {}. Will attempt to connect during processing.", e.getMessage());
    }

    log.info("OllamaEmbed initialized: model={}, source={}, dest={}", modelName, source, dest);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.hasNonNull(source) || StringUtils.isBlank(doc.getString(source))) {
      log.warn("doc id: {} does not have '{}' field or it is blank. Skipping embedding.", doc.getId(), source);
      return null;
    }

    String text = doc.getString(source);

    try {
      OllamaEmbedRequestModel request = OllamaEmbedRequestBuilder.getInstance(modelName, text).build();
      OllamaEmbedResponseModel response = ollamaAPI.embed(request);

      List<List<Double>> embeddings = response.getEmbeddings();
      if (embeddings == null || embeddings.isEmpty()) {
        log.warn("doc id: {} received empty embedding response.", doc.getId());
        return null;
      }

      // First embedding corresponds to our single input
      List<Double> vector = embeddings.get(0);
      for (Double value : vector) {
        doc.setOrAdd(dest, value.floatValue());
      }
    } catch (Exception e) {
      throw new StageException("Error getting embedding from Ollama for doc: " + doc.getId(), e);
    }

    return null;
  }
}
