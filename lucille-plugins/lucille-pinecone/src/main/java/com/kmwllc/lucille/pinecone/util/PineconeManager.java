package com.kmwllc.lucille.pinecone.util;

import io.pinecone.clients.Pinecone;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PineconeManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String DEFAULT_NAMESPACE = "default";
  private static final Map<String, Pinecone> clients = new ConcurrentHashMap<>();
  private final Pinecone client;

  private PineconeManager(String apiKey) {
    this.client = new Pinecone.Builder(apiKey).build();
  }

  public static Pinecone getClientInstance(String apiKey) {
    return clients.computeIfAbsent(apiKey, k -> new PineconeManager(apiKey).getClient());
  }

  private Pinecone getClient() {
    return client;
  }

  public static String getDefaultNamespace() {
    return DEFAULT_NAMESPACE;
  }
}
