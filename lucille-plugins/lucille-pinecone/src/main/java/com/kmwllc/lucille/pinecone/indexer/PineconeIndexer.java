package com.kmwllc.lucille.pinecone.indexer;

import com.kmwllc.lucille.core.IndexerException;
import io.pinecone.clients.Pinecone.Builder;
import io.pinecone.proto.ListItem;
import io.pinecone.proto.ListResponse;
import io.pinecone.proto.UpsertResponse;
import io.pinecone.unsigned_indices_model.VectorWithUnsignedIndices;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
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
 *    3. delete (deletes all vectors given document ids)
 *      - will only delete the exact id of documents that go through the pipeline, for id prefix deletion, look at deletionPrefix
 * - defaultEmbeddingField (String, required if namespaces is not set) : the field to retrieve embeddings from
 * - deleteByPrefix (boolean, optional) : sends a deletion by prefix request instead of a usual request
 * - addDeletionPrefix (String, optional) : String that adds on to all document ids used for prefix deletion
 *  - e.g. if id of doc is "doc1" & deletionPrefix is "-", then all vectors containing "doc1-" in id as prefix will be deleted.
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
  private final String addDeletionPrefix;
  private final boolean deleteByPrefix;

  public PineconeIndexer(Config config, IndexerMessenger messenger, String metricsPrefix) {
    super(config, messenger, metricsPrefix);
    this.indexName = config.getString("pinecone.index");
    this.namespaces = config.hasPath("pinecone.namespaces") ? config.getConfig("pinecone.namespaces").root().unwrapped() : null;
    this.metadataFields = config.hasPath("pinecone.metadataFields")
        ? new HashSet<>(config.getStringList("pinecone.metadataFields")) : new HashSet<>();
    this.mode = config.hasPath("pinecone.mode") ? config.getString("pinecone.mode") : "upsert";
    this.defaultEmbeddingField = ConfigUtils.getOrDefault(config, "pinecone.defaultEmbeddingField", null);
    this.client = new Pinecone.Builder(config.getString("pinecone.apiKey")).build();
    this.index = this.client.getIndexConnection(this.indexName);
    this.addDeletionPrefix = config.hasPath("pinecone.addDeletionPrefix") ? config.getString("pinecone.addDeletionPrefix") : "";
    this.deleteByPrefix = config.hasPath("pinecone.deleteByPrefix") ? config.getBoolean("pinecone.deleteByPrefix") : false;
    if (namespaces != null && namespaces.isEmpty()) {
      throw new IllegalArgumentException("namespaces mapping must be non-empty if provided");
    }
    // max upsertSize is 2MB or 1000 records, whichever is reached first, so stopping lucille run if batch size set to more than 1000
    // larger dimensions will mean smaller batch size limit, letting API throw the error if encountered.
    if (mode.equalsIgnoreCase("upsert") && DEFAULT_BATCH_SIZE > 1000) {
      throw new IllegalArgumentException("maximum batch size for upsert is 1000, and lower if vectors have higher dimensions");
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
    // retrieve documents to delete & upload
    List<Document> deleteList = new ArrayList<>();
    List<Document> uploadList = new ArrayList<>();
    for (Document doc : documents) {
      if (isDeletion(doc)) {
        deleteList.add(doc);
      } else {
        uploadList.add(doc);
      }
    }

    // if there exists documents to delete
    if (!deleteList.isEmpty()) {
      if (namespaces != null) {
        for (Map.Entry<String, Object> entry : namespaces.entrySet()) {
          deleteDocuments(deleteList, addDeletionPrefix, entry.getKey());
        }
      } else {
        deleteDocuments(deleteList, addDeletionPrefix,"default");
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
        uploadDocuments(uploadList, defaultEmbeddingField, "default");
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

  private void deleteDocuments(List<Document> documents, String additionalPrefix, String namespace) throws IndexerException {
    List<String> idsToDelete;
    if (deleteByPrefix) {
      idsToDelete = getVectorsByPrefix(documents, additionalPrefix, namespace);
    } else {
      idsToDelete = documents.stream()
          .map(Document::getId)
          .collect(Collectors.toList());
    }

    try {
      index.deleteByIds(idsToDelete, namespace);
    } catch (Exception e) {
      throw new IndexerException("Error while deleting vectors", e);
    }
  }

  private List<String> getVectorsByPrefix(List<Document> documents, String additionalPrefix, String namespace)
      throws IndexerException {
    List<ListItem> idsToDelete = new ArrayList<>();
    for (Document doc : documents) {
      String prefix = doc.getId() + additionalPrefix;
      try {
        // by default, list will list up to 100 vectors
        ListResponse listResponse = index.list(namespace, prefix);
        idsToDelete.addAll(listResponse.getVectorsList());

        // if number of vectors exceed 100
        while (listResponse.hasPagination()) {
          String paginationToken = listResponse.getPagination().getNext();
          listResponse = index.list(namespace, prefix, paginationToken);
          idsToDelete.addAll(listResponse.getVectorsList());
        }
      } catch (Exception e) {
        throw new IndexerException("Error listing vectors to delete", e);
      }
    }
    return idsToDelete.stream().map(ListItem::getId).collect(Collectors.toList());
  }

  private void upsertDocuments(List<VectorWithUnsignedIndices> upsertVectors, String namespace) throws IndexerException {
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

  private boolean isDeletion(Document doc) {
    return this.deletionMarkerField != null
        && this.deletionMarkerFieldValue != null
        && doc.hasNonNull(this.deletionMarkerField)
        && doc.getString(this.deletionMarkerField).equals(this.deletionMarkerFieldValue);
  }

  @Override
  public void closeConnection() {
    if (index != null) {
      index.close();
    }
  }
}
