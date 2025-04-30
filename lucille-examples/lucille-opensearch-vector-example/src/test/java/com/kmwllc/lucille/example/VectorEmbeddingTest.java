package com.kmwllc.lucille.example;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Simple test class to verify GeminiEmbed can generate embeddings correctly
 */
public class VectorEmbeddingTest {

  @Test
  public void testVectorEmbedding() throws Exception {
    // Skip this test unless explicitly enabled with -DrunGeminiTest=true
    if (!Boolean.parseBoolean(System.getProperty("runGeminiTest", "false"))) {
      System.out.println("Skipping Gemini embedding test (set -DrunGeminiTest=true to run)");
      return;
    }

    // Make sure we have an API key
    String apiKey = System.getProperty("gemini.apiKey");
    if (apiKey == null || apiKey.isEmpty()) {
      System.out.println("Skipping Gemini embedding test (set -Dgemini.apiKey=YOUR_API_KEY to run)");
      return;
    }

    // Create minimal config for testing
    Config config = ConfigFactory.empty()
        .withValue("source", ConfigValueFactory.fromAnyRef("chunk_text"))
        .withValue("dest", ConfigValueFactory.fromAnyRef("chunk_vector"))
        .withValue("embed_document", ConfigValueFactory.fromAnyRef(true))
        .withValue("embed_children", ConfigValueFactory.fromAnyRef(false))
        .withValue("api_key", ConfigValueFactory.fromAnyRef(apiKey))
        .withValue("model_name", ConfigValueFactory.fromAnyRef("text-embedding-004"))
        .withValue("dimensions", ConfigValueFactory.fromAnyRef(768));

    // Create a test document programmatically
    Document doc = Document.create("test1");
    doc.setField("chunk_text", "This is a sample text to test vector embeddings with Google Gemini API");

    // Initialize the embedding stage
    Stage embedder = new GeminiEmbed(config);
    embedder.start();

    // Process the document to add embeddings
    embedder.processDocument(doc);

    // Verify we have a vector
    assertTrue("Document should have a vector field", doc.has("chunk_vector"));
    
    // Get the vector and check its properties
    List<Float> vector = doc.getFloatList("chunk_vector");
    assertNotNull("Vector should not be null", vector);
    assertEquals("Vector should have 768 dimensions", 768, vector.size());
    
    // Print some statistics about the vector
    float sum = 0;
    float min = Float.MAX_VALUE;
    float max = Float.MIN_VALUE;
    
    for (Float val : vector) {
      sum += val;
      min = Math.min(min, val);
      max = Math.max(max, val);
    }
    
    float avg = sum / vector.size();
    System.out.println("Vector statistics:");
    System.out.println("- Dimension: " + vector.size());
    System.out.println("- Average value: " + avg);
    System.out.println("- Min value: " + min);
    System.out.println("- Max value: " + max);
    
    // Calculate L2 norm
    float sumSquares = 0;
    for (Float val : vector) {
      sumSquares += val * val;
    }
    float l2Norm = (float) Math.sqrt(sumSquares);
    System.out.println("- L2 norm: " + l2Norm);
    
    // Gemini embeddings should be normalized or close to normalized
    assertTrue("L2 norm should be close to 1.0", Math.abs(l2Norm - 1.0) < 0.1);
  }
}
