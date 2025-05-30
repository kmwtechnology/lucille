package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
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
 * <p> A stage for sending a Document to an LLM for general enrichment via the use of a system prompt, with particular support for
 * JSON-based responses.
 *
 * <p> To use the stage, you'll specify some Ollama Server config (hostURL, modelName) and a systemPrompt, detailing to the LLM of your
 * choice what you would like it to do with the document you provide. (For example, ask it to extract all the person names in the Document,
 * or to provide a brief summary, etc.) If your system prompt has the LLM output JSON, the fields in that JSON will be integrated into
 * the Lucille Document. If not, and requireJSON is set to false, the LLM's response will be placed into the "ollamaResponse" field.
 * Fields will be updated in accordance with the given update_mode, defaulting to overwriting any existing fields if they are present
 * on both the Document and the LLM's response.
 *
 * <p> It is recommended that you instruct your LLM to output JSON, for two primary reasons:
 *   <br> 1. Many LLMs tend to respond better to system prompts involving JSON.
 *   <br> 2. Lucille can automatically add the fields from the response to your Document.
 *
 * <p> Parameters:
 *
 *  <p> hostURL (String): A URL to your ollama server.
 *  <p> modelName (String): The name of the model you want to communicate with. See https://ollama.ai/library for available models/
 *  the appropriate names to use.
 *  <p> timeout (Long, Optional): How long you want to wait for a request to be processed before failing. Passed directly to Ollama.
 *  Uses Ollama's default of 10 seconds when not specified. You may want to increase the timeout if your Lucille Configuration
 *  uses multiple Worker threads (and you are working with Ollama locally).
 *
 *  <p> systemPrompt (String, Optional): The system prompt you want to provide to your LLM. Defaults to using no system prompt,
 *  as you may be using a model created from a Modelfile with a System Prompt already specified.
 *  <p> <b>Note:</b> It is recommended that you instruct your LLM to format its output as a JSON object, even if you are only
 *  asking for a single piece of information (like a summary).
 *
 *  <p> fields (list of Strings, Optional): The fields in the document you want to be sent to the LLM. If the list is empty or not specified,
 *  defaults to sending the entire Document to the LLM for enriching.
 *
 *  <p> requireJSON (Boolean, Optional): Whether you are requiring and expecting the LLM to output a JSON-only response. When true,
 *  Lucille will throw an Exception upon receiving a non-JSON response from the LLM. When false, Lucille will place the response's
 *  raw contents into the "ollamaResponse" field. Additionally, when set to true, your Ollama chat request will have <code>format: "json"</code>
 *  to prevent errors with Markdown formatting. Defaults to false.
 *
 *  <p> update_mode (String, Optional): How you want Lucille to update the fields in your Document, based on what it extracts from a JSON
 *  based response. Has no effect on a textual response placed in "ollamaResponse" - that will always overwrite any existing data.
 *  Should be one of "append", "overwrite", or "skip". Defaults to overwrite.
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

  private final UpdateMode updateMode;

  public PromptOllama(Config config) {
    super(config, Spec.stage()
        .withRequiredProperties("hostURL", "modelName")
        .withOptionalProperties("systemPrompt", "timeout", "fields", "requireJSON", "update_mode"));

    this.hostURL = config.getString("hostURL");
    this.modelName = config.getString("modelName");
    this.timeout = config.hasPath("timeout") ? config.getLong("timeout") : null;

    this.systemPrompt = ConfigUtils.getOrDefault(config, "systemPrompt", null);
    this.fields = ConfigUtils.getOrDefault(config, "fields", List.of());
    this.requireJSON = ConfigUtils.getOrDefault(config, "requireJSON", false);

    this.updateMode = UpdateMode.fromConfig(config);
  }

  @Override
  public void start() throws StageException {
    this.ollamaAPI = new OllamaAPI(hostURL);
    this.chatBuilder = OllamaChatRequestBuilder.getInstance(modelName);

    if (systemPrompt != null) {
      chatBuilder = chatBuilder.withMessage(OllamaChatMessageRole.SYSTEM, systemPrompt);
    }

    if (timeout != null) {
      ollamaAPI.setRequestTimeoutSeconds(timeout);
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    OllamaChatRequest request = createRequestWithSpecifiedFields(doc);
    request.setReturnFormatJson(requireJSON);

    OllamaChatResult chatResult;
    try {
      chatResult = ollamaAPI.chat(request);
    } catch (Exception e) {
      throw new StageException("Error communicating with Ollama server.", e);
    }

    // Always try to get JSON from the response, even if requireJSON is false.
    try {
      JsonNode node = mapper.readTree(chatResult.getResponseModel().getMessage().getContent());
      // put all the fields from the JSON onto the Lucille Document.
      node.fields().forEachRemaining(entry -> doc.update(entry.getKey(), updateMode, entry.getValue()));
    } catch (JsonProcessingException e) {
      if (requireJSON) {
        throw new StageException("Error getting JSON from response (requireJSON was set to true):", e);
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
      // If no fields specified, default to just using the entire Document.
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
