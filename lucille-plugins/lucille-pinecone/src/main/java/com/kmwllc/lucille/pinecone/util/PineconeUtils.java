package com.kmwllc.lucille.pinecone.util;

import io.pinecone.clients.Pinecone;
import java.util.Set;

public class PineconeUtils {
  // An empty namespace is Pinecone's default namespace, the one used by requests that don't name a namespace.
  private static final String DEFAULT_NAMESPACE = "";

  private static final Set<String> UNSTABLE_STATES = Set.of("Initializing", "InitializationFailed", "Terminating");

  private PineconeUtils() {
  }

  public static String getDefaultNamespace() {
    return DEFAULT_NAMESPACE;
  }

  public static boolean isClientStable(Pinecone client, String indexName) {
    if (client == null) return false;

    return !UNSTABLE_STATES.contains(client.describeIndex(indexName).getStatus().getState());
  }
}
