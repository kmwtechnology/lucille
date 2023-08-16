package com.kmwllc.lucille.pinecone.indexer;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.message.IndexerMessageManager;
import com.typesafe.config.Config;
import io.grpc.ConnectivityState;
import io.pinecone.PineconeClient;
import io.pinecone.PineconeClientConfig;
import io.pinecone.PineconeConnection;
import io.pinecone.proto.UpdateRequest;
import io.pinecone.proto.UpsertRequest;
import io.pinecone.proto.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class PineconeIndexer extends Indexer {

  private static final Logger log = LoggerFactory.getLogger(PineconeIndexer.class);

  private final PineconeClient client;
  private final String index;
  private final List<String> namespaces;
  private final List<String> embeddingFields;
  private final Set<String> metadataFields;
  private final String mode;
  private PineconeConnection connection;

  public PineconeIndexer(Config config, IndexerMessageManager manager, String metricsPrefix) {
    super(config, manager, metricsPrefix);
    this.index = config.getString("pinecone.index");
    this.namespaces = config.getStringList("pinecone.namespaces");
    this.embeddingFields = config.getStringList("pinecone.embeddingFields");
    this.metadataFields = new HashSet<>(config.getStringList("pinecone.metadataFields"));
    this.mode = config.hasPath("pinecone.mode") ? config.getString("pinecone.mode") : "upsert";
    PineconeClientConfig configuration = new PineconeClientConfig()
      .withApiKey(config.getString("pinecone.apiKey"))
      .withEnvironment(config.getString("pinecone.environment"))
      .withProjectName(config.getString("pinecone.projectName"))
      .withServerSideTimeoutSec(config.getInt("pinecone.timeout"));
    this.client = new PineconeClient(configuration);
  }

  public PineconeIndexer(Config config, IndexerMessageManager manager, boolean bypass, String metricsPrefix) {
    this(config, manager, metricsPrefix);
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

    for (int i = 0; i < namespaces.size(); i++) {
      final int index = i;
      List<Vector> upsertVectors = documents.stream()
        .map(doc -> Vector.newBuilder()
          .addAllValues(doc.getFloatList(this.embeddingFields.get(index)))
          .setMetadata(Struct.newBuilder().putAllFields(doc.asMap().entrySet().stream()
              .filter(entry -> metadataFields.contains(entry.getKey()))
              .collect(Collectors.toUnmodifiableMap(entry -> entry.getKey(), entry -> Value.newBuilder().setStringValue(entry.getValue().toString()).build())))
            .build())
          .setId(doc.getId())
          .build())
        .collect(Collectors.toList());

      if (mode.equalsIgnoreCase("upsert")) {
        UpsertRequest request = UpsertRequest.newBuilder()
          .addAllVectors(upsertVectors)
          .setNamespace(this.namespaces.get(i))
          .build();
        connection.getBlockingStub().upsert(request);
      }

      if (mode.equalsIgnoreCase("update")) {
        documents.forEach(doc -> {
          UpdateRequest request = UpdateRequest.newBuilder()
            .addAllValues(doc.getFloatList(this.embeddingFields.get(index)))
            .setId(doc.getId())
            .setNamespace(this.namespaces.get(index))
            .build();
          connection.getBlockingStub().update(request);
        });


      }
    }

  }

  @Override
  public void closeConnection() {
    if (connection != null) {
      connection.close();
    }
  }
}
