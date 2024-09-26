package com.kmwllc.lucille.pinecone.util;

import io.pinecone.clients.Index;
import io.pinecone.clients.Pinecone;
import java.lang.invoke.MethodHandles;
import org.openapitools.control.client.model.IndexModelStatus.StateEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PineconeManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String defaultNamespace = "default";
  private static PineconeManager instance;
  private final Pinecone client;
  private final Index index;
  private final String indexName;

  private PineconeManager(String apiKey, String indexName) {
    this.indexName = indexName;
    this.client = new Pinecone.Builder(apiKey).build();
    this.index = this.client.getIndexConnection(this.indexName);
  }

  // getting instance of PineconeManager with thread safety
  public static synchronized PineconeManager getInstance(String apiKey, String indexName) {
    if (instance == null) {
      instance = new PineconeManager(apiKey, indexName);
    }
    return instance;
  }

  public Index getIndex() {
    return index;
  }

  public String getIndexName() {
    return indexName;
  }

  public String getDefaultNamespace() {
    return defaultNamespace;
  }

  public synchronized boolean isClientStable() {
    try {
      StateEnum state = this.client.describeIndex(this.indexName).getStatus().getState();
      return state != StateEnum.INITIALIZING &&
          state != StateEnum.INITIALIZATIONFAILED &&
          state != StateEnum.TERMINATING;
    } catch (Exception e) {
      log.error("Error checking Pinecone client state", e);
      return false;
    }
  }
}
