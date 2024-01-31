package com.kmwllc.lucille.pinecone.indexer;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.typesafe.config.Config;
import io.grpc.ConnectivityState;
import io.pinecone.PineconeClient;
import io.pinecone.PineconeClientConfig;
import io.pinecone.PineconeConnection;
import io.pinecone.proto.UpdateRequest;
import io.pinecone.proto.UpsertRequest;
import io.pinecone.proto.Vector;

public class PineconeIndexer extends Indexer {

  private static final Logger log = LoggerFactory.getLogger(PineconeIndexer.class);

  private final PineconeClient client;
  private final String index;
  private final Map<String, Object> namespaces;
  private final Set<String> metadataFields;
  private final String mode;
  private PineconeConnection connection;
  private final String defaultEmbeddingField;

  public PineconeIndexer(Config config, IndexerMessenger messenger, String metricsPrefix) {
    super(config, messenger, metricsPrefix);
    this.index = config.getString("pinecone.index");
    this.namespaces = config.hasPath("pinecone.namespaces") ? config.getConfig("pinecone.namespaces").root().unwrapped() : null;
    this.metadataFields = new HashSet<>(config.getStringList("pinecone.metadataFields"));
    this.mode = config.hasPath("pinecone.mode") ? config.getString("pinecone.mode") : "upsert";
    this.defaultEmbeddingField = ConfigUtils.getOrDefault(config, "pinecone.defaultEmbeddingField", null);
    PineconeClientConfig configuration = new PineconeClientConfig().withApiKey(config.getString("pinecone.apiKey"))
        .withEnvironment(config.getString("pinecone.environment")).withProjectName(config.getString("pinecone.projectName"))
        .withServerSideTimeoutSec(config.getInt("pinecone.timeout"));
    this.client = new PineconeClient(configuration);

    if(namespaces == null && defaultEmbeddingField == null) {
      throw new IllegalArgumentException(
          "either a mapping of namespaces to embedding fields or a defaultEmbeddingField must be provided");
    }

    if(namespaces != null && namespaces.isEmpty()) {
      throw new IllegalArgumentException("namespaces mapping must not be empty if provided");
    }
  }

  public PineconeIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix) {
    this(config, messenger, metricsPrefix);
  }

  @Override
  public boolean validateConnection() {
    if (connection == null) {
      connection = this.client.connect(this.index);
    }
    ConnectivityState state = connection.getChannel().getState(true);
    return state != ConnectivityState.TRANSIENT_FAILURE && state != ConnectivityState.SHUTDOWN;
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
    List<Vector> upsertVectors = documents.stream()
        .map(doc -> Vector.newBuilder().addAllValues(doc.getFloatList(embeddingField))
            .setMetadata(Struct.newBuilder()
                .putAllFields(doc.asMap().entrySet().stream().filter(entry -> metadataFields.contains(entry.getKey()))
                    .collect(Collectors.toUnmodifiableMap(entry -> entry.getKey(),
                        entry -> Value.newBuilder().setStringValue(entry.getValue().toString()).build())))
                .build())
            .setId(doc.getId()).build())
        .collect(Collectors.toList());

    if (mode.equalsIgnoreCase("upsert")) {
      UpsertRequest request = UpsertRequest.newBuilder().addAllVectors(upsertVectors).setNamespace(namespace).build();
      connection.getBlockingStub().upsert(request);
    }

    if (mode.equalsIgnoreCase("update")) {
      documents.forEach(doc -> {
        UpdateRequest request = UpdateRequest.newBuilder().addAllValues(doc.getFloatList(embeddingField)).setId(doc.getId())
            .setNamespace(namespace).build();
        connection.getBlockingStub().update(request);
      });
    }
  }

  @Override
  public void closeConnection() {
    if (connection != null) {
      connection.close();
    }
  }
}
