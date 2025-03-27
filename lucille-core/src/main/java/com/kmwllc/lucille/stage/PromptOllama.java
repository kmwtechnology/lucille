package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import io.github.ollama4j.OllamaAPI;
import io.github.ollama4j.models.chat.OllamaChatMessageRole;
import io.github.ollama4j.models.chat.OllamaChatRequest;
import io.github.ollama4j.models.chat.OllamaChatRequestBuilder;
import io.github.ollama4j.models.chat.OllamaChatResult;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A stage for sending a Document to an LLM for general enrichment via the use of a system prompt, with particular support for
 * JSON-based responses.
 *
 * To use the stage, you'll specify some Ollama Server config (hostURL, modelName) and a systemPrompt, detailing to the LLM of your
 * choice what you would like it to do with the document you provide. (For example, ask it to extract all the person names in the Document,
 * or to provide a brief summary, etc.) If your system prompt has the LLM output JSON, the fields in that JSON will be integrated into
 * the Lucille Document. If not, and requireJSON is set to false, the LLM's response will be placed into the "ollamaResponse" field.
 *
 * It is highly recommended that you instruct your LLM to output only a JSON object, for two primary reasons:
 *   1. Many LLMs tend to respond better to system prompts involving JSON.
 *   2. Lucille can automatically add the fields from the response to your Document.
 *
 * Parameters:
 *
 *  hostURL (String): A URL to your ollama server.
 *  modelName (String): The name of the model you want to communicate with. See https://ollama.ai/library for available models/
 *  the appropriate names to use.
 *  timeout (Long, Optional): How long you want to wait for a request to be processed before failing. Passed directly to Ollama.
 *  Uses Ollama's default of 10 seconds when not specified.
 *
 *  systemPrompt (String): The system prompt you want to provide to your LLM.
 *  <b>Note:</b> It is recommended that you instruct your LLM to format its output as a JSON object, even if you are only
 *  asking for a single piece of information (like a summary).
 *
 *  fields (list of Strings, Optional): The fields in the document you want to be sent to the LLM. If the list is empty or not specified,
 *  defaults to sending the entire Document to the LLM for enriching.
 *
 *  requireJSON (Boolean, Optional): Whether you are requiring & expecting the LLM to output a JSON-only response. When true,
 *  Lucille will throw an Exception upon receiving a non-JSON response from the LLM. When false, Lucille will place the response's
 *  raw contents into the "ollamaResponse" field. Defaults to false.
 */
public class PromptOllama extends Stage {

  private static final Logger log = LoggerFactory.getLogger(PromptOllama.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  private final String hostURL;
  private final String modelName;
  private final Long timeout;

  private final String systemPrompt;
  private final List<String> fields;
  private final boolean requireJSON;

  private OllamaAPI ollamaAPI;
  private OllamaChatRequestBuilder chatBuilder;

  public PromptOllama(Config config) {
    super(config, new StageSpec()
        .withRequiredProperties("hostURL", "modelName", "systemPrompt")
        .withOptionalProperties("timeout", "fields", "requireJSON"));

    this.hostURL = config.getString("hostURL");
    this.modelName = config.getString("modelName");
    this.timeout = ConfigUtils.getOrDefault(config, "timeout", null);

    this.systemPrompt = config.getString("systemPrompt");
    this.fields = ConfigUtils.getOrDefault(config, "fields", List.of());
    this.requireJSON = ConfigUtils.getOrDefault(config, "requireJSON", false);
  }

  @Override
  public void start() throws StageException {
    this.ollamaAPI = new OllamaAPI(hostURL);
    this.chatBuilder = OllamaChatRequestBuilder
        .getInstance(modelName)
        .withMessage(OllamaChatMessageRole.SYSTEM, systemPrompt);

    if (timeout != null) {
      ollamaAPI.setRequestTimeoutSeconds(timeout);
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    OllamaChatRequest request = createRequestWithSpecifiedFields(doc);

    OllamaChatResult chatResult;
    try {
      log.debug("Sending request {}:", request);

      chatResult = ollamaAPI.chat(request);

      // How you get just the pure response from the LLM
      log.debug("Got response {}:", chatResult.getResponseModel().getMessage().getContent());
    } catch (Exception e) {
      throw new StageException("Error communicating with Ollama server.", e);
    }

    // Always try to get JSON from the response, even if requireJSON is false.
    try {
      JsonNode node = mapper.readTree(chatResult.getResponseModel().getMessage().getContent());

      node.fields().forEachRemaining(entry -> doc.setField(entry.getKey(), entry.getValue()));
    } catch (JsonProcessingException e) {
      if (requireJSON) {
        throw new StageException("Error getting JSON from response", e);
      } else {
        log.info("Didn't get JSON from Ollama response. The response was placed into ollamaResponse instead.");
        doc.setField("ollamaResponse", chatResult.getResponseModel().getMessage().getContent());
      }
    }

    return null;
  }

  // Creates an OllamaChatRequest using all the fields, if fields is null, or only the specified fields from the document, as Strings.
  private OllamaChatRequest createRequestWithSpecifiedFields(Document doc) {
    if (fields.isEmpty()) {
      return chatBuilder.withMessage(OllamaChatMessageRole.USER, doc.toString()).build();
    } else {
      ObjectNode requestNode = mapper.createObjectNode();

      for (String field : fields) {
        requestNode.set(field, doc.getJson(field));
      }

      return chatBuilder.withMessage(OllamaChatMessageRole.USER, requestNode.toString()).build();
    }
  }
}
