package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.OpenAIModels;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.knuddels.jtokkit.Encodings;
import com.knuddels.jtokkit.api.Encoding;
import com.knuddels.jtokkit.api.EncodingResult;
import com.knuddels.jtokkit.api.ModelType;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException.Null;
import dev.langchain4j.model.output.FinishReason;
import java.util.Iterator;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.openai.OpenAiEmbeddingModel;
import dev.langchain4j.model.output.Response;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This stage uses openAI embedding services to embed a text field in a Lucille document or its children document(s). Will truncate
 * to token limit before sending request. Retrieves API key from environment variables.
 *
 * Config Parameters:
 * - text_field (String) : field of which the embedding Stage will retrieve content from
 * - embed_document (Boolean) : Embeds the document's text_field if set to true.
 * - embed_children (Boolean): Embeds the document's children text_field if set to true.
 * - embedding_field (String, Optional) : name of the field that will hold the embeddings
 * - model_name (String, Optional) : the name of the OpenAI embedding model to use, set default to text-embedding-3-small
 *    1. text-embedding-3-small
 *    2. text-embedding-3-large
 *    3. text-embedding-ada-002
 *    more details: <a href="https://platform.openai.com/docs/guides/embeddings/embedding-models">...</a>
 * - dimensions (Integer, Optional) : number of dimensions the resulting embedding should have. Only supported in text-embedding-3
 * and later models. Default set to null, which will call the model's default dimensions.
 */

public class OpenAiEmbedding extends Stage {
  // this is the token limit for all embedding models from openai
  private static final int DEFAULT_OPENAI_TOKEN_LIMIT = 8191;
  private final String API_KEY;
  private EmbeddingModel model;
  private final String textField;
  private final boolean embedDocument;
  private final boolean embedChildren;
  private final String embeddingField;
  private final OpenAIModels modelName;
  private final Integer dimensions;
  private Encoding enc;

  private static final Logger log = LoggerFactory.getLogger(OpenAiEmbedding.class);

  public OpenAiEmbedding(Config config) throws StageException {
    super(config, new StageSpec().withRequiredProperties("text_field", "embed_document", "embed_children", "api_key"));
    textField = config.getString("text_field");
    embedDocument = config.getBoolean("embed_document");
    embedChildren = config.getBoolean("embed_children");
    API_KEY = config.getString("api_key");
    embeddingField = config.hasPath("embedding_field") ? config.getString("embedding_field") : "embeddings";
    modelName = OpenAIModels.fromConfig(config);
    dimensions = config.hasPath("dimensions") ? config.getInt("dimensions") : null;
    if (!embedDocument && !embedChildren) {
      throw new StageException("Both embed_document and embed_children are false.");
    }
    if (API_KEY.isEmpty()) {
      throw new StageException("API key is empty.");
    }
  }

  @Override
  public void start() throws StageException {
    model = OpenAiEmbeddingModel.builder()
        .modelName(modelName.getModelName())
        .dimensions(dimensions)
        .apiKey(API_KEY)
        .build();

    // retrieve encoding from modelName
    ModelType modelType = ModelType.fromName(modelName.getModelName()).orElse(ModelType.TEXT_EMBEDDING_3_SMALL);
    enc = Encodings.newDefaultEncodingRegistry().getEncodingForModel(modelType);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (embedDocument) {
      sendForEmbedding(doc);
    }
    if (embedChildren && doc.hasChildren()){
      List<Document> childDocs = doc.getChildren();
      for (Document childDoc : childDocs) {
        sendForEmbedding(childDoc);
      }
    }
    return null;
  }

  private void sendForEmbedding(Document doc) throws StageException {
    if (!doc.has(textField)) {
      log.warn("{} does not exist, skipping document: {}", textField, doc);
      return;
    }

    String content = doc.getString(textField);
    content = ensureContentIsWithinLimit(content);
    Response<Embedding> response;

    // model.embed will retry 3 times by default
    // catching runtimeException for invalid API key and other errors.
    try {
      response = model.embed(content);
    } catch (RuntimeException e) {
      log.warn("failed to get embedding", e);
      return;
    }

    Embedding embedding = response.content();
    float[] vectors = embedding.vector();
    for (float vector : vectors) {
      doc.setOrAdd(embeddingField, vector);
    }
  }

  private String ensureContentIsWithinLimit(String content) {
    int tokenUsage = enc.countTokens(content);
    String truncatedContent = null;
    if (tokenUsage > DEFAULT_OPENAI_TOKEN_LIMIT) {
      EncodingResult encoded = enc.encode(content, DEFAULT_OPENAI_TOKEN_LIMIT);
      truncatedContent = enc.decode(encoded.getTokens());
    }
    return truncatedContent == null ? content : truncatedContent;
  }
}
