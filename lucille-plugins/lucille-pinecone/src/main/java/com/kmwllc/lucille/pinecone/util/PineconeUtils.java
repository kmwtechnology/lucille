package com.kmwllc.lucille.pinecone.util;

import io.pinecone.clients.Pinecone;

public class PineconeUtils {
  // An empty namespace is Pinecone's default namespace, the one used by requests that don't name a namespace.
  private static final String DEFAULT_NAMESPACE = "";

  private PineconeUtils() {
  }

  public static String getDefaultNamespace() {
    return DEFAULT_NAMESPACE;
  }

  public static boolean isClientStable(Pinecone client, String indexName) {
    if (client == null) return false;

    String state = client.describeIndex(indexName).getStatus().getState();
    return !"Initializing".equals(state) &&
        !"InitializationFailed".equals(state) &&
        !"Terminating".equals(state);
  }
}
