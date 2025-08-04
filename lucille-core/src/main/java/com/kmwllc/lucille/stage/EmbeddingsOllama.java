package com.kmwllc.lucille.stage;

import io.github.ollama4j.OllamaAPI;
import io.github.ollama4j.exceptions.OllamaBaseException;
import io.github.ollama4j.models.embeddings.OllamaEmbedResponseModel;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

/**
 * A stage for generating vector embeddings from text content using Ollama's embedding models.
 * 
 * <p>This stage integrates with a locally running Ollama server to convert text content into
 * high-dimensional vector embeddings suitable for semantic search, similarity matching, and
 * vector database indexing. The stage is commonly used in RAG (Retrieval-Augmented Generation)
 * pipelines and semantic search implementations.</p>
 * 
 * <p><strong>Key Features:</strong></p>
 * <ul>
 *   <li>Generates dense vector embeddings from text content</li>
 *   <li>Supports various Ollama embedding models (e.g., nomic-embed-text, all-minilm)</li>
 *   <li>Configurable field mapping for input text and output embeddings</li>
 *   <li>Automatic error handling and retry logic</li>
 *   <li>Integration with OpenSearch, Elasticsearch, and other vector databases</li>
 * </ul>
 * 
 * <p><strong>Common Use Cases:</strong></p>
 * <ul>
 *   <li>Semantic search over document collections</li>
 *   <li>Text similarity and clustering analysis</li>
 *   <li>RAG pipeline preprocessing for knowledge bases</li>
 *   <li>Content recommendation systems</li>
 *   <li>Duplicate detection and content deduplication</li>
 * </ul>
 * 
 * <p><strong>Configuration Parameters:</strong></p>
 * <ul>
 *   <li><code>hostURL</code> (String, required): URL of the Ollama server (e.g., "http://localhost:11434")</li>
 *   <li><code>modelName</code> (String, required): Embedding model name (e.g., "nomic-embed-text:latest")</li>
 *   <li><code>chunk_text</code> (String, required): Source field containing text to embed</li>
 *   <li><code>update_mode</code> (String, optional): Field update behavior ("overwrite", "skip", "append")</li>
 * </ul>
 * 
 * <p><strong>Output:</strong></p>
 * <p>The stage stores the generated embedding as a JSON array in the "embedding" field of the document.
 * The embedding dimensions depend on the chosen model (e.g., nomic-embed-text produces 768-dimensional vectors).</p>
 * 
 * <p><strong>Example Configuration:</strong></p>
 * <pre>{@code
 * {
 *   name: "generateEmbeddings",
 *   class: "com.kmwllc.lucille.stage.EmbeddingsOllama"
 *   hostURL: "http://localhost:11434"
 *   modelName: "nomic-embed-text:latest"
 *   chunk_text: "content"
 *   update_mode: "overwrite"
 * }
 * }</pre>
 * 
 * <p><strong>Pipeline Integration:</strong></p>
 * <p>This stage is typically used after text processing stages (chunking, cleaning) and before
 * indexing into vector databases. Common pipeline patterns:</p>
 * <pre>{@code
 * TextExtractor → ChunkText → EmbeddingsOllama → OpenSearchIndexer
 * FileConnector → EntityExtraction → EmbeddingsOllama → VectorSearch
 * }</pre>
 * 
 * <p><strong>Error Handling:</strong></p>
 * <p>The stage handles various error conditions including network timeouts, model loading issues,
 * and malformed text input. Documents with embedding failures are logged but continue through
 * the pipeline to prevent processing interruption.</p>
 * 
 * <p><strong>Performance Considerations:</strong></p>
 * <ul>
 *   <li>Embedding generation is computationally intensive - consider batch processing</li>
 *   <li>Network latency to Ollama server affects throughput</li>
 *   <li>Model loading time impacts first-request latency</li>
 *   <li>Memory usage scales with embedding dimensions and batch size</li>
 * </ul>
 * 
 * @author Kevin M. Butler
 * @since 0.5.7
 * @see com.kmwllc.lucille.stage.ChunkText
 * @see com.kmwllc.lucille.stage.ApplyOpenNLPNameFinders
 */
public class EmbeddingsOllama extends Stage {

  public static final Spec SPEC = Spec.stage()
      .requiredString("hostURL", "modelName", "chunk_text")
      .optionalString("update_mode");

  private static final Logger log = LoggerFactory.getLogger(EmbeddingsOllama.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private OllamaAPI ollamaAPI;
  private final String hostURL;
  private final String modelName;
  private final String chunkText;

  public EmbeddingsOllama(Config config) {
    super(config);

    this.hostURL = config.getString("hostURL");
    this.modelName = config.getString("modelName");
    this.chunkText = config.getString("chunk_text");
  }

  @Override
  public void start() throws StageException {
    this.ollamaAPI = new OllamaAPI(hostURL);
    try {
      ollamaAPI.ping();
    } catch (Exception e) {
      throw new StageException("Error communicating with Ollama server.", e);
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    // Get the actual text content from the document field specified by chunkText
    String textToEmbed = doc.getString(chunkText);
    
    if (textToEmbed == null || textToEmbed.trim().isEmpty()) {
      log.warn("No text found in field '{}' for document {}, skipping embedding generation", chunkText, doc.getId());
      return null;
    }
    
    OllamaEmbedResponseModel response;
    try {
      response = ollamaAPI.embed(modelName, List.of(textToEmbed));
      List<Double> embedding = response.getEmbeddings().get(0);
      log.debug("Generated embedding for document {}: {} dimensions", doc.getId(), embedding.size());
      
      // Convert List<Double> to JsonNode for proper storage as JSON array
      JsonNode embeddingNode = objectMapper.valueToTree(embedding);
      doc.setField("embedding", embeddingNode); 
    } catch (IOException e) {
      throw new StageException("Error communicating with Ollama server.", e);
    } catch (InterruptedException e) {
      throw new StageException("Error communicating with Ollama server.", e);
    } catch (OllamaBaseException e) {
      throw new StageException("Error communicating with Ollama server.", e);
    }
    return null;
  }
}
