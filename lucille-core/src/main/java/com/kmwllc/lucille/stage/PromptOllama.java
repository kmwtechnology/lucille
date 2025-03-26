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
 * A stage for sending a Document to an LLM for general enrichment via the use of a system prompt. It is highly recommended that you
 * instruct your LLM to output only a JSON object, for two primary reasons:
 * 1. Many LLMs tend to respond better to system prompts involving JSON.
 * 2. Lucille can automatically add the fields from the response to your Document as is.
 *
 * If you do not use a system prompt that includes JSON, or the LLM outputs a malformed response, Lucille will place the entire
 * response in the "ollamaResponse" field.
 *
 * Parameters:
 *  hostURL (String): A URL to your ollama server.
 *  modelName (String): The name of the model you want to communicate with. See https://ollama.ai/library for available models/
 *  the appropriate names to use.
 *
 *  systemPrompt (String): The system prompt you want to provide to your LLM.
 *  <b>Note:</b> It is recommended that you instruct your LLM to format its output as a JSON object, even if you are only
 *  asking for a single piece of information (like a summary).
 *
 *  fields (list of Strings, Optional): The fields in the document you want to be sent to the LLM. Defaults to sending the entire
 *  Document to the LLM for enriching.
 */
public class PromptOllama extends Stage {

  private static final Logger log = LoggerFactory.getLogger(PromptOllama.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  private final String hostURL;
  private final String modelName;

  private final String systemPrompt;
  private final List<String> fields;

  private OllamaAPI ollamaAPI;
  private OllamaChatRequestBuilder chatBuilder;

  public PromptOllama(Config config) {
    super(config, new StageSpec().withRequiredProperties("hostURL", "modelName", "systemPrompt").withOptionalProperties("fields"));

    this.hostURL = config.getString("hostURL");
    this.modelName = config.getString("modelName");

    this.systemPrompt = config.getString("systemPrompt");
    this.fields = ConfigUtils.getOrDefault(config, "fields", null);
  }

  @Override
  public void start() throws StageException {
    this.ollamaAPI = new OllamaAPI(hostURL);
    this.chatBuilder = OllamaChatRequestBuilder
        .getInstance(modelName)
        .withMessage(OllamaChatMessageRole.SYSTEM, systemPrompt);
  }

  @Override
  public void stop() throws StageException {
    // TODO: Make sure there is nothing that can be closed, delete method if so.
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    OllamaChatRequest request = createRequestWithSpecifiedFields(doc);

    OllamaChatResult chatResult;
    try {
      log.info("Sending request {}:", request);

      chatResult = ollamaAPI.chat(request);

      // How you get just the pure response from the LLM
      log.info("Got response {}:", chatResult.getResponseModel().getMessage().getContent());
    } catch (Exception e) {
      throw new StageException("Error communicating with Ollama server.", e);
    }

    // TODO: Integrate the response / build a JSON from the response and add it to the Document.

    try {
      JsonNode node = mapper.readTree(chatResult.getResponseModel().getMessage().getContent());

      node.fields().forEachRemaining(entry -> doc.setField(entry.getKey(), entry.getValue()));
    } catch (JsonProcessingException e) {
      log.warn("An error occurred processing the JSON from the LLM response. The response's entire contents have been placed into ollamaResponse instead.", e);
      doc.setField("ollamaResponse", chatResult.getResponseModel().getMessage().getContent());
    }

    return null;
  }

  // Creates an OllamaChatRequest using all of the fields, if fields is null, or only the specified fields from the document, as Strings.
  private OllamaChatRequest createRequestWithSpecifiedFields(Document doc) {
    if (fields == null) {
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
