package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.stage.util.OpenAIEmbeddingModel;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.knuddels.jtokkit.Encodings;
import com.knuddels.jtokkit.api.Encoding;
import com.knuddels.jtokkit.api.EncodingResult;
import com.knuddels.jtokkit.api.ModelType;
import com.typesafe.config.Config;
import dev.langchain4j.data.segment.TextSegment;
import java.util.ArrayList;
import java.util.Iterator;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.openai.OpenAiEmbeddingModel;
import dev.langchain4j.model.output.Response;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This stage uses openAI embedding services to embed a text field in a Lucille document or its children document(s). Will truncate
 * to token limit before sending request. Retrieves API key from config.
 *
 * Config Parameters:
 * - source (String) : field of which the embedding Stage will retrieve content from
 * - dest (String, Optional) : name of the field that will hold the embeddings, defaults to "embeddings"
 * - embed_document (Boolean) : Embeds the document's source if set to true.
 * - embed_children (Boolean): Embeds the document's children source if set to true.
 * - api_key (String) : API key used for OpenAI requests
 * - model_name (String, Optional) : the name of the OpenAI embedding model to use, set default to text-embedding-3-small
 *    1. text-embedding-3-small
 *    2. text-embedding-3-large
 *    3. text-embedding-ada-002
 *    more details: <a href="https://platform.openai.com/docs/guides/embeddings/embedding-models">...</a>
 * - dimensions (Integer, Optional) : number of dimensions the resulting embedding should have. Only supported in text-embedding-3
 * and later models. Default set to null, which will call the model's default dimensions.
 */

public class OpenAIEmbed extends Stage {

  public static Spec SPEC = Spec.stage()
      .reqStr("source", "api_key")
      .reqBool("embed_document", "embed_children")
      .optStr("dest", "model_name")
      .optNum("dimensions");

  // this is the token limit for all embedding models from openai
  private static final int DEFAULT_OPENAI_TOKEN_LIMIT = 8191;
  private final String API_KEY;
  private final String source;
  private final String dest;
  private EmbeddingModel model;
  private final OpenAIEmbeddingModel modelName;
  private final boolean embedDocument;
  private final boolean embedChildren;
  private final Integer dimensions;
  private Encoding enc;

  private static final Logger log = LoggerFactory.getLogger(OpenAIEmbed.class);

  public OpenAIEmbed(Config config) throws StageException {
    super(config);

    this.source = config.getString("source");
    this.embedDocument = config.getBoolean("embed_document");
    this.embedChildren = config.getBoolean("embed_children");
    this.API_KEY = config.getString("api_key");
    this.dest = config.hasPath("dest") ? config.getString("dest") : "embeddings";
    this.modelName = OpenAIEmbeddingModel.fromConfig(config);
    this.dimensions = config.hasPath("dimensions") ? config.getInt("dimensions") : null;
    if (!this.embedDocument && !this.embedChildren) {
      throw new StageException("Both embed_document and embed_children are false.");
    }
    if (StringUtils.isBlank(this.API_KEY)) {
      throw new StageException("API key is empty.");
    }
  }

  // Method exists for testing with mockito mocks
  void setModel(EmbeddingModel model) {
    this.model = model;
  }

  @Override
  public void start() throws StageException {
    log.info("using OpenAI model: {}", modelName.getModelName());

    // will throw exception if API_KEY is null or empty but is already checked in constructor
    model = OpenAiEmbeddingModel.builder()
        .modelName(modelName.getModelName())
        .dimensions(dimensions)
        .apiKey(API_KEY)
        .build();

    // retrieve modelType from modelName
    ModelType modelType = ModelType.fromName(modelName.getModelName()).orElse(ModelType.TEXT_EMBEDDING_3_SMALL);

    if (!modelName.getModelName().equals(modelType.getName())) {
      log.error("model_name: {} does not match model type: {}", modelName.getModelName(), modelType.getName());
      throw new StageException("Model used for embedding is different from model used for counting tokens.");
    }

    enc = Encodings.newDefaultEncodingRegistry().getEncodingForModel(modelType);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    List<Document> documentsToEmbed = new ArrayList<>();

    // send doc for embedding if it does contain textField with nonNull value and that textField is not empty
    if (embedDocument && isValidDocument(doc)) {
      documentsToEmbed.add(doc);
    }
    if (embedChildren && doc.hasChildren()){
      // only way to retrieve children is to call getChildren() which returns a deep copy of current children
      for (Document childDoc : doc.getChildren()) {
        if (isValidDocument(childDoc)) {
          documentsToEmbed.add(childDoc);
        }
      }
    }

    List<Document> processedDocs = sendForEmbedding(documentsToEmbed, doc);

    // currently do not support modifying children, so now explicitly cloning (getChildren()), processing and replacing old children
    if (embedChildren && doc.hasChildren()) {
      doc.removeChildren();
      for (Document processedDoc : processedDocs) {
        if (processedDoc.getId().equals(doc.getId())) {
          continue;
        }
        doc.addChild(processedDoc);
      }
    }

    return null;
  }

  private boolean isValidDocument(Document doc) {
    return doc.hasNonNull(source) && !StringUtils.isBlank(doc.getString(source));
  }

  private List<Document> sendForEmbedding(List<Document> docsToEmbed, Document parentDoc) throws StageException {
    // if there is no embedding done on this document, carry on with lucille-run with other documents
    if (docsToEmbed.isEmpty()) {
      log.warn("No documents to embed. Check your source field, embed_children and embed_document setting in your config file if you"
          + " expect docid {} or its children to be sent for embedding.", parentDoc.getId());
      return docsToEmbed;
    }

    // ensure all chunks are within OpenAI token limit
    List<TextSegment> textSegments = new ArrayList<>();
    for (Document doc : docsToEmbed) {
      String content = doc.getString(source);
      content = applyTokenLimit(content);
      textSegments.add(TextSegment.from(content));
    }

    // RuntimeException is thrown for any request errors, and will only return if request is successful
    // model.embed will retry 3 times before RuntimeException is thrown
    Response<List<Embedding>> response;
    try {
      response = model.embedAll(textSegments);
    } catch (Exception e) { // catch all exceptions thrown by OpenAI/LangChain4J
      throw new StageException("failed to get embedding for childDocs: ", e);
    }

    // checking that response is same size as number of documents sent for embedding
    List<Embedding> embeddings = response.content();
    if (embeddings.size() != docsToEmbed.size()) {
      throw new StageException("embedding count mismatch after embedding");
    }

    // add embeddings to document
    for (int i = 0; i < embeddings.size(); i++) {
      float[] vectors = embeddings.get(i).vector();
      Document doc = docsToEmbed.get(i);
      for (Float vector : vectors) {
        doc.setOrAdd(dest, vector);
      }
    }

    return docsToEmbed;
  }

  private String applyTokenLimit(String content) {
    int tokenUsage = enc.countTokens(content);

    if (tokenUsage > DEFAULT_OPENAI_TOKEN_LIMIT) {
      EncodingResult encoded = enc.encode(content, DEFAULT_OPENAI_TOKEN_LIMIT);
      return enc.decode(encoded.getTokens());
    }

    return content;
  }
}
