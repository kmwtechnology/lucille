package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import io.github.ollama4j.OllamaAPI;
import io.github.ollama4j.models.chat.OllamaChatMessageRole;
import io.github.ollama4j.models.chat.OllamaChatRequest;
import io.github.ollama4j.models.chat.OllamaChatRequestBuilder;
import io.github.ollama4j.types.OllamaModelType;
import java.util.Iterator;

/**
 * Parameters:
 *  hostURL (String): A URL to your ollama server.
 *  modelName (String): The name of the model you want to communicate with. See https://ollama.ai/library for available models/
 *  the appropriate names to use.
 *
 *  systemPrompt (String, Optional): The system prompt you want to provide to your LLM.
 *  <b>Note:</b> It is recommended that you instruct your LLM to format its output as a JSON object, even if you are only
 *  asking for a single piece of information (like a summary).
 *  fields (list of Strings, Optional): The fields in the document you want to send to the LLM. Defaults to sending the entire
 *  Document to the LLM for enriching.
 */
public class PromptOllama extends Stage {

  public PromptOllama(Config config) {
    super(config);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    new OllamaAPI();
    new OllamaAPI("");

    OllamaChatRequestBuilder builder = OllamaChatRequestBuilder.getInstance(OllamaModelType.LLAMA3);

    OllamaChatRequest request = builder.withMessage(OllamaChatMessageRole.getRole("user"));
    return null;
  }
}
