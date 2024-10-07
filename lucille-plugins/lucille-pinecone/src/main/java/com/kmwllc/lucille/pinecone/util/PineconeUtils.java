package com.kmwllc.lucille.pinecone.util;

import io.pinecone.clients.Pinecone;
import org.openapitools.control.client.model.IndexModelStatus.StateEnum;

public class PineconeUtils {
  private static final String DEFAULT_NAMESPACE = "default";

  public static String getDefaultNamespace() {
    return DEFAULT_NAMESPACE;
  }

  public static boolean isClientStable(Pinecone client, String indexName) {
    if (client == null) return false;

    StateEnum state = client.describeIndex(indexName).getStatus().getState();
    return state != StateEnum.INITIALIZING &&
        state != StateEnum.INITIALIZATIONFAILED &&
        state != StateEnum.TERMINATING;
  }
}
