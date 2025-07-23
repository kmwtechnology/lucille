package com.kmwllc.lucille.example;

import io.github.ollama4j.OllamaAPI;
import io.github.ollama4j.models.embeddings.OllamaEmbedResponseModel;
import java.util.List;

public class OllamaAPITest {
    /**
     * Main method to test Ollama API for generating embeddings.
     * It checks if the Ollama server is reachable and then generates embeddings
     * for a sample text using a specified model.
     */
    public static void main(String[] args) {
        // Initialize the Ollama API client
        OllamaAPI ollamaAPI = new OllamaAPI();
        boolean isOllamaServerReachable = false;

        // Check if the Ollama server is reachable
        try {
            isOllamaServerReachable = ollamaAPI.ping();
            System.out.println("Connected to Ollama server");
        } catch (Exception e) {
            System.err.println("Cannot proceed: Ollama server is not reachable");
        }
        if (isOllamaServerReachable) {
            // Call with explicit parameters
            generateEmbeddings(ollamaAPI, "nomic-embed-text:latest",
                    "This is a sample text to generate embeddings for.");
        }
    }

    /**
     * Generates and prints embeddings for the given text using the specified model.
     */
    public static void generateEmbeddings(OllamaAPI ollamaAPI, String model, String text) {
        try {
            System.out.println("Generating embeddings for text: " + text);
            OllamaEmbedResponseModel response = ollamaAPI.embed(model, List.of(text));
            List<Double> embedding = response.getEmbeddings().get(0);
            System.out.println("Generated embedding: " + embedding);
            System.out.println("Total embedding dimensions: " + embedding.size());
        } catch (Exception e) {
            System.err.println("Error generating embeddings: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
