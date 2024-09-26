package com.kmwllc.lucille.pinecone.indexer;

import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.pinecone.util.PineconeManager;
import io.pinecone.proto.UpsertResponse;
import io.pinecone.unsigned_indices_model.VectorWithUnsignedIndices;
import java.util.ArrayList;
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
import io.pinecone.clients.Index;
import static io.pinecone.commons.IndexInterface.buildUpsertVectorWithUnsignedIndices;

/**
 *
 * Config Parameters:
 * - index (String) : index which PineconeIndexer will request from
 * - namespaces (Map<String, Object>, Optional) : mapping of your namespace to the field of the document to retrieve the vectors from,
 *   using the same namespace(s) in deletion request if a document is marked for deletion.
 * - apiKey (String) : API key used for requests
 * - mode (String, Optional) : the type of request you want PineconeIndexer to send to your index
 *    1. upsert (will replace if vector id exists in namespace or index if no namespace is given)
 *    2. update (will only update embeddings)
 *    for deletion requests, take a look at application-example.conf under Indexer configs. Only support
 *    deletionMarkerField, and deletionMarkerFieldValue as serverless index currently does not support delete via query/metadata.
 * - defaultEmbeddingField (String, required if namespaces is not set) : the field to retrieve embeddings from
 */
public class PineconeIndexer extends Indexer {

  private static final Logger log = LoggerFactory.getLogger(PineconeIndexer.class);
  private final PineconeManager pineconeManager;
  private final Map<String, Object> namespaces;
  private final Set<String> metadataFields;
  private final String mode;
  private final String defaultEmbeddingField;

  public PineconeIndexer(Config config, IndexerMessenger messenger, String metricsPrefix) {
    super(config, messenger, metricsPrefix);
    this.pineconeManager = PineconeManager.getInstance(config.getString("pinecone.index"), config.getString("pinecone.apiKey"));
    this.namespaces = config.hasPath("pinecone.namespaces") ? config.getConfig("pinecone.namespaces").root().unwrapped() : null;
    this.metadataFields = config.hasPath("pinecone.metadataFields")
        ? new HashSet<>(config.getStringList("pinecone.metadataFields")) : new HashSet<>();
    this.mode = config.hasPath("pinecone.mode") ? config.getString("pinecone.mode") : "upsert";
    this.defaultEmbeddingField = ConfigUtils.getOrDefault(config, "pinecone.defaultEmbeddingField", null);
    if (namespaces != null && namespaces.isEmpty()) {
      throw new IllegalArgumentException("Namespaces mapping must be non-empty if provided.");
    }
    // max upload batch is 2MB or 1000 records, whichever is reached first, so stopping lucille run if batch size set to more than 1000
    // larger dimensions will mean smaller batch size limit, letting API throw the error if encountered.
    if (DEFAULT_BATCH_SIZE > 1000) {
      throw new IllegalArgumentException("Maximum batch size is 1000, and lower if vectors have higher dimensions.");
    }
  }

  public PineconeIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix) {
    this(config, messenger, metricsPrefix);
  }

  @Override
  public boolean validateConnection() {
    return pineconeManager.isClientStable();
  }

  @Override
  protected void sendToIndex(List<Document> documents) throws IndexerException {
    // retrieve documents to delete & upload
    List<Document> deleteList = new ArrayList<>();
    List<Document> uploadList = new ArrayList<>();
    for (Document doc : documents) {
      if (isDeletion(doc)) {
        // if doc is in uploadList and is now marked for deletion, then remove that document in uploadList and add it to deleteList
        uploadList = uploadList.stream().filter(other -> !other.getId().equals(doc.getId())).collect(Collectors.toList());
        deleteList.add(doc);
      } else {
        // if doc is already in deleteList, then continue adding to uploadList as uploading is performed after deletion
        // note: updating/deleting a non-existent ID will return an OK response
        uploadList.add(doc);
      }
    }

    // if there exists documents to delete
    if (!deleteList.isEmpty()) {
      if (namespaces != null) {
        for (Map.Entry<String, Object> entry : namespaces.entrySet()) {
          deleteDocuments(deleteList, entry.getKey());
        }
      } else {
        deleteDocuments(deleteList, pineconeManager.getDefaultNamespace());
      }
    }

    // if there exists documents to upload
    if (!uploadList.isEmpty()) {
      // check that both namespaces and defaultEmbeddingField is not null only when uploading
      validateUploadRequirements();
      if (namespaces != null) {
        for (Map.Entry<String, Object> entry : namespaces.entrySet()) {
          uploadDocuments(uploadList, (String) entry.getValue(), entry.getKey());
        }
      } else {
        uploadDocuments(uploadList, defaultEmbeddingField, pineconeManager.getDefaultNamespace());
      }
    }
  }

  private void validateUploadRequirements() {
    if (namespaces == null && defaultEmbeddingField == null) {
      throw new IllegalArgumentException(
          "At least one of a defaultEmbeddingField or a non-empty namespaces mapping is required when uploading documents.");
    }
  }

  private void uploadDocuments(List<Document> documents, String embeddingField, String namespace) throws IndexerException {
    if (mode.equalsIgnoreCase("upsert")) {
      List<VectorWithUnsignedIndices> upsertVectors = documents.stream()
          // buildUpsertVector would throw an error if embeddings is null,
          // should add dropDocument stage with stage conditions in pipeline before indexing
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
    } else if (mode.equalsIgnoreCase("update")) {
      updateDocuments(documents, embeddingField, namespace);
    }
  }

  private void deleteDocuments(List<Document> documents, String namespace) throws IndexerException {
    List<String> idsToDelete = documents.stream()
          .map(Document::getId)
          .collect(Collectors.toList());

    try {
      pineconeManager.getIndex().deleteByIds(idsToDelete, namespace);
    } catch (Exception e) {
      throw new IndexerException("Error while deleting vectors", e);
    }
  }

  private void upsertDocuments(List<VectorWithUnsignedIndices> upsertVectors, String namespace) throws IndexerException {
    try {
      UpsertResponse response = pineconeManager.getIndex().upsert(upsertVectors, namespace);

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
        pineconeManager.getIndex().update(doc.getId(), doc.getFloatList(embeddingField), namespace);
      });
    } catch (Exception e) {
      throw new IndexerException("Error while updating vectors", e);
    }
  }

  private boolean isDeletion(Document doc) {
    return this.deletionMarkerField != null
        && this.deletionMarkerFieldValue != null
        && doc.hasNonNull(this.deletionMarkerField)
        && doc.getString(this.deletionMarkerField).equals(this.deletionMarkerFieldValue);
  }

  @Override
  public void closeConnection() {
    Index index = pineconeManager.getIndex();
    if (index != null) {
      index.close();
    }
  }
}
