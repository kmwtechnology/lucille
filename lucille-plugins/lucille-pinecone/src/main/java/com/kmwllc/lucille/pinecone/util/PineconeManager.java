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
