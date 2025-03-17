package com.kmwllc.lucille.example;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;

/**
 * This stage uses Google's Gemini API to generate vector embeddings for text content in Lucille documents.
 * It sends HTTP requests to the Gemini API and stores the resulting embeddings in the document.
 *
 * Config Parameters:
 * - source (String) : The field containing text content to embed
 * - dest (String, Optional) : The field that will store the embeddings, defaults to "chunk_vector"
 * - embed_document (Boolean) : Whether to embed the document's source field
 * - embed_children (Boolean) : Whether to embed the children documents' source fields
 * - api_key (String) : Google Gemini API key
 * - model_name (String, Optional) : Gemini embedding model name, default is "text-embedding-004"
 * - dimensions (Integer, Optional) : Number of dimensions for embeddings, default is 768
 * - request_timeout_millis (Integer, Optional) : HTTP request timeout in milliseconds, default is 30000
 */
public class GeminiEmbed extends Stage {
    
    private static final Logger log = LoggerFactory.getLogger(GeminiEmbed.class);
    private static final String DEFAULT_MODEL = "text-embedding-004";
    private static final int DEFAULT_DIMENSIONS = 768;
    private static final int DEFAULT_TIMEOUT_MILLIS = 30000;
    private static final String GEMINI_API_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models/";
    private static final String GEMINI_EMBED_ENDPOINT = ":embedContent";
    private static final int MAX_TEXT_LENGTH = 65000; // Gemini API limit is 65536 characters
    
    private final String apiKey;
    private final String source;
    private final String dest;
    private final String modelName;
    private final int dimensions;
    private final boolean embedDocument;
    private final boolean embedChildren;
    private final int requestTimeoutMillis;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    
    /**
     * Constructor for the GeminiEmbed stage.
     * 
     * @param config Configuration for this stage
     * @throws StageException if the configuration is invalid
     */
    public GeminiEmbed(Config config) throws StageException {
        super(config, new StageSpec()
                .withRequiredProperties("source", "embed_document", "embed_children", "api_key")
                .withOptionalProperties("dest", "model_name", "dimensions", "request_timeout_millis"));
                
        this.source = config.getString("source");
        this.embedDocument = config.getBoolean("embed_document");
        this.embedChildren = config.getBoolean("embed_children");
        this.apiKey = config.getString("api_key");
        this.dest = config.hasPath("dest") ? config.getString("dest") : "chunk_vector";
        this.modelName = config.hasPath("model_name") ? config.getString("model_name") : DEFAULT_MODEL;
        this.dimensions = config.hasPath("dimensions") ? config.getInt("dimensions") : DEFAULT_DIMENSIONS;
        this.requestTimeoutMillis = config.hasPath("request_timeout_millis") ? 
                config.getInt("request_timeout_millis") : DEFAULT_TIMEOUT_MILLIS;
        
        if (!this.embedDocument && !this.embedChildren) {
            throw new StageException("At least one of embed_document or embed_children must be true");
        }
        
        if (StringUtils.isBlank(this.apiKey)) {
            throw new StageException("API key is required and cannot be empty");
        }
        
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(this.requestTimeoutMillis))
                .build();
                
        this.objectMapper = new ObjectMapper();
    }
    
    @Override
    public void start() throws StageException {
        super.start();
        log.info("Initializing Google Gemini embedding stage with model: {} ({}D)", modelName, dimensions);
    }
    
    @Override
    public Iterator<Document> processDocument(Document doc) throws StageException {
        try {
            // Check if the document is valid for embeddings
            if (!isValidDocument(doc)) {
                log.debug("Document {} does not meet embedding criteria, skipping", doc.getId());
                return null;
            }
            
            List<Document> docsToEmbed = new ArrayList<>();
            
            // Add the parent document if needed
            if (embedDocument) {
                docsToEmbed.add(doc);
            }
            
            // Add children documents if needed
            if (embedChildren && doc.hasChildren()) {
                doc.getChildren().forEach(docsToEmbed::add);
            }
            
            // Send for embedding
            if (!docsToEmbed.isEmpty()) {
                sendForEmbedding(docsToEmbed, doc);
            }
            
        } catch (Exception e) {
            log.error("Error processing document {}: {}", doc.getId(), e.getMessage());
            throw new StageException("Failed to process document: " + doc.getId(), e);
        }
        
        return null;
    }
    
    /**
     * Checks if a document is valid for embedding.
     * 
     * @param doc The document to check
     * @return true if the document is valid for embedding, false otherwise
     */
    private boolean isValidDocument(Document doc) {
        // Check if parent document has the source field when embed_document is true
        if (embedDocument && !doc.has(source)) {
            log.warn("Document {} is missing source field: {}", doc.getId(), source);
            return false;
        }
        
        // Check if children documents have the source field when embed_children is true
        if (embedChildren && doc.hasChildren()) {
            boolean validChildren = doc.getChildren().stream().allMatch(child -> child.has(source));
            if (!validChildren) {
                log.warn("One or more children of document {} are missing source field: {}", 
                        doc.getId(), source);
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Sends documents for embedding and updates them with the resulting embeddings.
     * 
     * @param docsToEmbed List of documents to embed
     * @param parentDoc The parent document
     * @throws StageException if there is an error generating embeddings
     */
    private void sendForEmbedding(List<Document> docsToEmbed, Document parentDoc) throws StageException {
        for (Document doc : docsToEmbed) {
            try {
                String text = doc.getString(source);
                if (StringUtils.isBlank(text)) {
                    log.warn("Empty text for document {}, field: {}", doc.getId(), source);
                    continue;
                }
                
                // Truncate text if it exceeds the API limit
                if (text.length() > MAX_TEXT_LENGTH) {
                    log.warn("Text length exceeds limit of {} characters. Truncating text from {} characters.", 
                             MAX_TEXT_LENGTH, text.length());
                    text = text.substring(0, MAX_TEXT_LENGTH);
                }
                
                // Generate embedding
                float[] embedding = generateEmbedding(text);
                
                // Store embedding values in the document
                for (float value : embedding) {
                    doc.setOrAdd(dest, value);
                }
                
                log.debug("Successfully embedded document {} with {} dimensions", doc.getId(), embedding.length);
                
            } catch (Exception e) {
                log.error("Error embedding document {}: {}", doc.getId(), e.getMessage());
                throw new StageException("Failed to get embedding for document: " + doc.getId(), e);
            }
        }
    }
    
    /**
     * Generates embeddings for the given text using the Google Gemini API.
     * 
     * @param text The text to embed
     * @return A float array containing the embedding values
     * @throws StageException if there is an error generating the embedding
     */
    private float[] generateEmbedding(String text) throws StageException {
        try {
            // Trim text if it exceeds maximum length
            if (text.length() > MAX_TEXT_LENGTH) {
                log.warn("Text length exceeds API limit of {} characters. Truncating text from {} characters.", 
                         MAX_TEXT_LENGTH, text.length());
                text = text.substring(0, MAX_TEXT_LENGTH);
            }
            
            // Create the API URL with API key as query parameter
            String apiUrl = GEMINI_API_BASE_URL + modelName + GEMINI_EMBED_ENDPOINT + "?key=" + apiKey;
            
            // Create the request body according to Gemini API format
            ObjectNode requestBody = objectMapper.createObjectNode();
            
            // Set the model property
            requestBody.put("model", "models/" + modelName);
            
            // Create the content object with parts array
            ObjectNode contentNode = objectMapper.createObjectNode();
            ArrayNode partsNode = objectMapper.createArrayNode();
            ObjectNode textPartNode = objectMapper.createObjectNode();
            textPartNode.put("text", text);
            partsNode.add(textPartNode);
            contentNode.set("parts", partsNode);
            
            // Add the content to the request body
            requestBody.set("content", contentNode);
            
            // Add output dimensionality if specified
            if (dimensions > 0 && !modelName.equals("embedding-001")) {
                requestBody.put("outputDimensionality", dimensions);
            }
            
            String jsonPayload = requestBody.toString();
            log.debug("Sending request to Gemini API with {} characters of text", text.length());
            
            // Create and send the HTTP request
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(apiUrl))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                    .timeout(Duration.ofMillis(requestTimeoutMillis))
                    .build();
            
            CompletableFuture<HttpResponse<String>> responseFuture = 
                    httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString());
            
            HttpResponse<String> response = responseFuture.get(requestTimeoutMillis, TimeUnit.MILLISECONDS);
            
            // Handle response
            int statusCode = response.statusCode();
            String responseBody = response.body();
            
            if (statusCode != 200) {
                log.error("API request failed with status code {}: {}", statusCode, responseBody);
                throw new StageException("API request failed with status code " + statusCode + ": " + responseBody);
            }
            
            log.debug("Received successful response from Gemini API");
            
            // Parse the response to extract embeddings
            JsonNode responseJson = objectMapper.readTree(responseBody);
            JsonNode embeddingNode = responseJson.path("embedding").path("values");
            
            if (embeddingNode.isArray()) {
                // Convert JSON array to float array
                float[] embeddings = new float[embeddingNode.size()];
                for (int i = 0; i < embeddingNode.size(); i++) {
                    embeddings[i] = (float) embeddingNode.get(i).asDouble();
                }
                return embeddings;
            } else {
                log.error("Invalid response format, embedding values not found: {}", responseBody);
                throw new StageException("Invalid response format, embedding values not found");
            }
            
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Request timed out or was interrupted: {}", e.getMessage());
            throw new StageException("Request timed out or was interrupted", e);
        } catch (IOException e) {
            log.error("Error parsing JSON response: {}", e.getMessage());
            throw new StageException("Error parsing JSON response", e);
        } catch (Exception e) {
            log.error("Unexpected error: {}", e.getMessage());
            throw new StageException("Unexpected error during API request", e);
        }
    }
}
