package com.kmwllc.lucille.pinecone.util;

import io.pinecone.clients.Pinecone;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PineconeManager {
  private static final String DEFAULT_NAMESPACE = "default";
  private static final Map<String, Pinecone> clients = new ConcurrentHashMap<>();

  private PineconeManager() {
  }

  public static Pinecone getClient(String apiKey) {
    // if mapping produces null or exception, will not add entry to map
    return clients.computeIfAbsent(apiKey, k -> new Pinecone.Builder(apiKey).build());
  }

  public static String getDefaultNamespace() {
    return DEFAULT_NAMESPACE;
  }
}
