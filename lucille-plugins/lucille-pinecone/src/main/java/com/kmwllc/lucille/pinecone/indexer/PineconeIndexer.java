package com.kmwllc.lucille.pinecone.indexer;

import io.grpc.ConnectivityState;
import io.pinecone.exceptions.PineconeException;
import io.pinecone.proto.UpdateResponse;
import io.pinecone.unsigned_indices_model.VectorWithUnsignedIndices;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.zookeeper.data.Stat;
import org.openapitools.client.model.IndexModelStatus.StateEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.typesafe.config.Config;
import io.pinecone.clients.Index;
import io.pinecone.clients.Pinecone;
import static io.pinecone.commons.IndexInterface.buildUpsertVectorWithUnsignedIndices;
import org.openapitools.client.model.IndexModel;

public class PineconeIndexer extends Indexer {

  private static final Logger log = LoggerFactory.getLogger(PineconeIndexer.class);

  private final Pinecone client;
  private final Index index;
  private final String indexName;
  private final Map<String, Object> namespaces;
  private final Set<String> metadataFields;
  private final String mode;
  private final String defaultEmbeddingField;

  public PineconeIndexer(Config config, IndexerMessenger messenger, String metricsPrefix) {
    super(config, messenger, metricsPrefix);
    this.indexName = config.getString("pinecone.index");
    this.namespaces = config.hasPath("pinecone.namespaces") ? config.getConfig("pinecone.namespaces").root().unwrapped() : null;
    this.metadataFields = new HashSet<>(config.getStringList("pinecone.metadataFields"));
    this.mode = config.hasPath("pinecone.mode") ? config.getString("pinecone.mode") : "upsert";
    this.defaultEmbeddingField = ConfigUtils.getOrDefault(config, "pinecone.defaultEmbeddingField", null);
//    PineconeConfig configuration = new PineconeConfig(config.getString("pinecone.apiKey"))
//        .withEnvironment(config.getString("pinecone.environment")).withProjectName(config.getString("pinecone.projectName"))
//        .withServerSideTimeoutSec(config.getInt("pinecone.timeout"));
    this.client = new Pinecone.Builder(config.getString("pinecone.apiKey")).build();
    this.index = this.client.getIndexConnection(this.indexName);

    if (namespaces == null && defaultEmbeddingField == null) {
      throw new IllegalArgumentException(
          "at least one of a defaultEmbeddingField or a non-empty namespaces mapping is required");
    }

    if (namespaces != null && namespaces.isEmpty()) {
      throw new IllegalArgumentException("namespaces mapping must be non-empty if provided");
    }
  }

  public PineconeIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix) {
    this(config, messenger, metricsPrefix);
  }

  @Override
  public boolean validateConnection() {
    StateEnum state = this.client.describeIndex(this.indexName).getStatus().getState();
    return state != StateEnum.INITIALIZING && state != StateEnum.INITIALIZATIONFAILED && state != StateEnum.TERMINATING;
  }

  @Override
  protected void sendToIndex(List<Document> documents) {
    if (namespaces != null) {
      for (Map.Entry<String, Object> entry : namespaces.entrySet()) {
        uploadDocuments(documents, (String) entry.getValue(), entry.getKey());
      }
    } else {
      uploadDocuments(documents, defaultEmbeddingField, "");
    }
  }

  private void uploadDocuments(List<Document> documents, String embeddingField, String namespace) {

    List<VectorWithUnsignedIndices> upsertVectors = documents.stream()
        .map(doc -> buildUpsertVectorWithUnsignedIndices(
            doc.getId(),
            doc.getFloatList(embeddingField),
            null,
            null,
            Struct.newBuilder()
                .putAllFields(doc.asMap().entrySet().stream().filter(entry -> metadataFields.contains(entry.getKey()))
                .collect(Collectors.toUnmodifiableMap(entry -> entry.getKey(),
                    entry -> Value.newBuilder().setStringValue(entry.getValue().toString()).build())))
            .build()))
        .collect(Collectors.toList());

    if (mode.equalsIgnoreCase("upsert")) {
      this.index.upsert(upsertVectors, namespace);
    }

    if (mode.equalsIgnoreCase("update")) {
      documents.forEach(doc -> {
        UpdateResponse response = this.index.update(doc.getId(), doc.getFloatList(embeddingField), namespace);
      });
    }
  }

  @Override
  public void closeConnection() {
    this.index.close();
  }
}
