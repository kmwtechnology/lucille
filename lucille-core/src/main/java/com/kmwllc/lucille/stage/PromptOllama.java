package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import io.github.ollama4j.OllamaAPI;
import io.github.ollama4j.models.chat.OllamaChatMessageRole;
import io.github.ollama4j.models.chat.OllamaChatRequest;
import io.github.ollama4j.models.chat.OllamaChatRequestBuilder;
import io.github.ollama4j.types.OllamaModelType;
import java.util.Iterator;

/**
 * A stage for prompting an LLM via Ollama server, with the options to:
 *   - Use information from a Lucille Document as part of your prompt
 *   - Place the entire response on a Lucille Document
 *   - If using a system prompt that exclusively outputs JSON, specify a path to a field in the JSON
 *   you want to place on a Lucille Document
 *
 * Parameters:
 *  hostURL (String): A URL to your ollama server.
 *  modelName (String): The name of the model you want to communicate with. See https://ollama.ai/library for available models/
 *  the appropriate names to use.
 *
 *  prompt (String, Optional): The prompt you want to provide to the model. Defaults to just using the document represented
 *  as a JSON String.
 *  role (String, Optional): The role you want to use for your request. Defaults to "user".
 */
public class PromptOllama extends Stage {

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    new OllamaAPI();
    new OllamaAPI("");

    OllamaChatRequestBuilder builder = OllamaChatRequestBuilder.getInstance(OllamaModelType.LLAMA3);

    OllamaChatRequest request = builder.withMessage(OllamaChatMessageRole.getRole("user"));
    return null;
  }
}
