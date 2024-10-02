package com.kmwllc.lucille.pinecone.util;

import io.pinecone.clients.Pinecone;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.openapitools.control.client.model.IndexModelStatus.StateEnum;

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

  public static boolean isStable(String apiKey, String indexName) {
    if (clients.containsKey(apiKey)) {
      Pinecone client = clients.get(apiKey);
      StateEnum state = client.describeIndex(indexName).getStatus().getState();
      return state != StateEnum.INITIALIZING &&
          state != StateEnum.INITIALIZATIONFAILED &&
          state != StateEnum.TERMINATING;
    }
    return false;
  }
}
