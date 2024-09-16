package com.kmwllc.lucille.pinecone.indexer;

import com.kmwllc.lucille.core.IndexerException;
import io.pinecone.proto.UpsertResponse;
import io.pinecone.unsigned_indices_model.VectorWithUnsignedIndices;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.openapitools.control.client.model.IndexModelStatus.StateEnum;
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

/**
 *
 * Config Parameters:
 * - index (String) : index which PineconeIndexer will request from
 * - namespaces (Map<String, Object>, Optional) : mapping of your namespace to the field of the document to retrieve the vectors from
 * - apiKey (String) : API key used for requests
 * - mode (String, Optional) : the type of request you want PineconeIndexer to send to your index
 *    1. upsert (will replace if vector id exists in namespace or index if no namespace is given)
 *    2. update (will only update embeddings)
 *    3. delete (deletes all vectors with using prefix of all document ids that go through the pipeline
 *      - e.g. if id of doc is "doc1", then all vectors containing "doc1" as prefix will be deleted. "doc1#chunk1, doc1#chunk2,..."
 *      - serverless architecture currently does not support deletion by metadata, more info below
 *      <a href="https://docs.pinecone.io/reference/api/2024-07/data-plane/delete">...</a>
 * - defaultEmbeddingField (String, required if namespaces is not set) : the field to retrieve embeddings from
 * - deletionPrefix (String, optional) : adds on to all document ids used for prefix deletion
 *  - e.g. if id of doc is "doc1" & deletionPrefix is "v1", then all vectors containing "doc1v1" in id as prefix will be deleted.
 */
public class PineconeIndexer extends Indexer {

  private static final Logger log = LoggerFactory.getLogger(PineconeIndexer.class);

  private final Pinecone client;
  private final Index index;
  private final String indexName;
  private final Map<String, Object> namespaces;
  private final Set<String> metadataFields;
  private final String mode;
  private final String defaultEmbeddingField;
  private final String deletionPrefix;

  public PineconeIndexer(Config config, IndexerMessenger messenger, String metricsPrefix) {
    super(config, messenger, metricsPrefix);
    this.indexName = config.getString("pinecone.index");
    this.namespaces = config.hasPath("pinecone.namespaces") ? config.getConfig("pinecone.namespaces").root().unwrapped() : null;
    this.metadataFields = new HashSet<>(config.getStringList("pinecone.metadataFields"));
    this.mode = config.hasPath("pinecone.mode") ? config.getString("pinecone.mode") : "upsert";
    this.defaultEmbeddingField = ConfigUtils.getOrDefault(config, "pinecone.defaultEmbeddingField", null);
    this.client = new Pinecone.Builder(config.getString("pinecone.apiKey")).build();
    this.index = this.client.getIndexConnection(this.indexName);
    this.deletionPrefix = config.hasPath("pinecone.deletionPrefix") ? config.getString("pinecone.deletionPrefix") : "";
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
  protected void sendToIndex(List<Document> documents) throws IndexerException {
    if (documents.isEmpty()) {
      log.warn("No documents in batch to index. Waiting for next batch...");
      return;
    }

    if (namespaces != null) {
      for (Map.Entry<String, Object> entry : namespaces.entrySet()) {
        uploadDocuments(documents, (String) entry.getValue(), entry.getKey());
      }
    } else {
      uploadDocuments(documents, defaultEmbeddingField, "");
    }
  }

  private void uploadDocuments(List<Document> documents, String embeddingField, String namespace) throws IndexerException {

    if (mode.equalsIgnoreCase("upsert")) {
      List<VectorWithUnsignedIndices> upsertVectors = documents.stream()
          .filter(doc -> doc.hasNonNull(embeddingField)) // buildUpsertVector would throw an error if embeddings is null
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
      upsertDocuments(upsertVectors, namespace);
    }

    if (mode.equalsIgnoreCase("update")) {
      updateDocuments(documents, embeddingField, namespace);
    }

    if (mode.equalsIgnoreCase("delete")) {
      deleteDocuments(documents, namespace, deletionPrefix);
    }
  }

  private void deleteDocuments(List<Document> documents, String namespace, String additionalPrefix) throws IndexerException {
    List<String> idsToDelete = documents.stream()
        .map(doc -> doc.getId() + additionalPrefix)
        .collect(Collectors.toList());

    try {
      index.deleteByIds(idsToDelete, namespace);
    } catch (Exception e) {
      throw new IndexerException("Error while deleting vectors", e);
    }
  }

  private void upsertDocuments(List<VectorWithUnsignedIndices> upsertVectors, String namespace) throws IndexerException {
    // max upsertSize is 2MB or 1000 records, whichever is reached first, so stopping lucille run if batch size set to more than 1000
    // larger dimensions will mean smaller batch size limit, letting API throw the error if encountered.
    if (upsertVectors.size() > 1000) {
      throw new IndexerException("max upsert size of each batch is 1000, reduce the batch size indexer configuration.");
    }

    // scenario where there exists documents to send to Index, but has either no embedding field or embeddings
    if (upsertVectors.isEmpty()) {
      log.warn("no vectors to upsert in this batch. If not intended, "
          + "ensure that documents contain embeddings within field given in configuration.");
      return;
    }

    try {
      UpsertResponse response = index.upsert(upsertVectors, namespace);

      // check response for upsertedCount to be equal to upsertVectors
      if (response.getUpsertedCount() != upsertVectors.size()) {
        throw new IndexerException("Number of upserted vectors to pinecone does not match the number of upserted vectors requested to upsert.");
      }
    } catch (Exception e) {
      throw new IndexerException("Error while upserting vectors", e);
    }
  }

  private void updateDocuments(List<Document> documents, String embeddingField, String namespace) throws IndexerException {
    try {
      documents.forEach(doc -> {
        log.debug("updating docId: {} namespace: {} embedding: {}", doc.getId(), namespace, doc.getFloatList(embeddingField));
        // does not validate the existence of IDs within the index, if no records are affected, a 200 OK status is returned
        // will only throw error if doc.getId() is null
        index.update(doc.getId(), doc.getFloatList(embeddingField), namespace);
      });
    } catch (Exception e) {
      throw new IndexerException("Error while updating vectors", e);
    }
  }

  @Override
  public void closeConnection() {
    if (index != null) {
      index.close();
    }
  }
}
