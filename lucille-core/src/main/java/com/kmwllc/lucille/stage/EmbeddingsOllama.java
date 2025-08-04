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
 * <p> A stage for sending a Document to an LLM for genrerating vector embeddings.
 *
 * <p> To use the stage, you'll specify some Ollama Server config (hostURL, modelName) along with the chunk_text from a document. 
 * The chunk_text will then be converted to a vector embedding and returned as json.
 *
 * <p> Parameters:
 *
 *  <p> hostURL (String): A URL to your ollama server.
 *  <p> modelName (String): The name of the model you want to communicate with. See https://ollama.ai/library for available models/
 *  the appropriate names to use.
 *
 *  <p> chunk_text (String): The text you want to convert to a vector embedding.
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
