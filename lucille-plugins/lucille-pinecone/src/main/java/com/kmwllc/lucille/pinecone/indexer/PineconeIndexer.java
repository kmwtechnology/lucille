package com.kmwllc.lucille.pinecone.indexer;

import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.pinecone.util.PineconeUtils;
import io.pinecone.clients.Pinecone;
import io.pinecone.proto.UpsertResponse;
import io.pinecone.unsigned_indices_model.VectorWithUnsignedIndices;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
 * PineconeIndexer requires a field in the document to contain embeddings. For documents that do not have embeddings nor
 * embedding field, set up a drop stage with stage conditions in the pipeline before indexing.
 *
 * Config Parameters:
 * - index (String) : index which PineconeIndexer will request from
 * - namespaces (Map&lt;String, Object&gt;, Optional) : mapping of your namespace to the field of the document to retrieve the vectors from,
 *   using the same namespace(s) in deletion request if a document is marked for deletion.
 * - apiKey (String) : API key used for requests
 * - metadataFields (List&lt;String&gt;, Optional): list of fields you want to be uploaded as metadata along with the embeddings
 * - mode (String, Optional) : the type of request you want PineconeIndexer to send to your index when uploading documents
 *    1. upsert (will replace if vector id exists in namespace, will use default namespace if no namespace is given)
 *    2. update (will only update embeddings)
 *    for deletion requests, take a look at application-example.conf under Indexer configs. Only support
 *    deletionMarkerField, and deletionMarkerFieldValue as Pinecone serverless index currently does not support delete via query/metadata.
 * - defaultEmbeddingField (String, required if namespaces is not set) : the field to retrieve embeddings from
 */
public class PineconeIndexer extends Indexer {

  private static final Logger log = LoggerFactory.getLogger(PineconeIndexer.class);
  private static final Integer MAX_PINECONE_BATCH_SIZE = 1000;
  private final Pinecone client;
  private final Index index;
  private final String indexName;
  private final Map<String, Object> namespaces;
  private final Set<String> metadataFields;
  private final String mode;
  private final String defaultEmbeddingField;

  public PineconeIndexer(Config config, IndexerMessenger messenger, String metricsPrefix, String localRunId) throws IndexerException {
    super(config, messenger, metricsPrefix, localRunId, Spec.indexer()
        .withRequiredProperties("apiKey", "index")
        .withOptionalParents("namespaces")
        .withOptionalProperties("metadataFields", "mode", "defaultEmbeddingField"));

    this.client = new Pinecone.Builder(config.getString("pinecone.apiKey")).build();
    this.indexName = config.getString("pinecone.index");
    this.index = client.getIndexConnection(indexName);
    this.namespaces = config.hasPath("pinecone.namespaces") ? config.getConfig("pinecone.namespaces").root().unwrapped() : null;
    this.metadataFields = config.hasPath("pinecone.metadataFields")
        ? new HashSet<>(config.getStringList("pinecone.metadataFields")) : new HashSet<>();
    this.mode = config.hasPath("pinecone.mode") ? config.getString("pinecone.mode") : "upsert";
    this.defaultEmbeddingField = ConfigUtils.getOrDefault(config, "pinecone.defaultEmbeddingField", null);
    if (namespaces != null && namespaces.isEmpty()) {
      throw new IndexerException("Namespaces mapping must be non-empty if provided.");
    }
    // max upload batch is 2MB or 1000 records, whichever is reached first, so stopping run if batch size is set to more than 1000
    // larger dimensions will mean smaller batch size limit, letting API throw the error if encountered.
    if (this.getBatchCapacity() > MAX_PINECONE_BATCH_SIZE) {
      throw new IndexerException(
          "Maximum batch size for Pinecone is 1000, and lower if vectors have higher dimensions. Set indexer batchSize config to"
              + "1000 or lower.");
    }
  }

  public PineconeIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix, String localRunId) throws IndexerException {
    this(config, messenger, metricsPrefix, localRunId);
  }

  // Convenience Constructors
  public PineconeIndexer(Config config, IndexerMessenger messenger, String metricsPrefix) throws IndexerException {
    this(config, messenger, metricsPrefix, null);
  }

  public PineconeIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix) throws IndexerException {
    this(config, messenger, metricsPrefix, null);
  }

  @Override
  public String getIndexerConfigKey() { return "pinecone"; }

  @Override
  public boolean validateConnection() {
    try {
      return PineconeUtils.isClientStable(client, indexName);
    } catch (Exception e) {
      log.error("Error checking Pinecone client state.", e);
      return false;
    }
  }

  @Override
  protected void sendToIndex(List<Document> documents) throws IndexerException {
    // retrieve documents to delete & upload, mapping id to document
    Map<String, Document> deleteMap = new LinkedHashMap<>();
    Map<String, Document> uploadMap = new LinkedHashMap<>();
    for (Document doc : documents) {
      String id = doc.getId();
      if (isDeletion(doc)) {
        // always add to deleteMap;
        // in the case where doc is in uploadMap and is now marked for deletion, then remove that document in uploadMap
        uploadMap.remove(id);
        deleteMap.put(id, doc);
      } else {
        // always add non deletion documents to uploadMap;
        // in the case that doc is also in deleteMap, we remove it from the deleteMap to avoid sending unnecessary deletion request
        deleteMap.remove(id);
        uploadMap.put(id, doc);
      }
    }

    // if there exists documents to delete
    if (!deleteMap.isEmpty()) {
      if (namespaces != null) {
        for (Map.Entry<String, Object> entry : namespaces.entrySet()) {
          deleteDocuments(new ArrayList<>(deleteMap.values()), entry.getKey());
        }
      } else {
        deleteDocuments(new ArrayList<>(deleteMap.values()), PineconeUtils.getDefaultNamespace());
      }
    }

    // if there exists documents to upload
    if (!uploadMap.isEmpty()) {
      // check that both namespaces and defaultEmbeddingField is not null only when uploading
      validateUploadRequirements();
      if (namespaces != null) {
        for (Map.Entry<String, Object> entry : namespaces.entrySet()) {
          uploadDocuments(new ArrayList<>(uploadMap.values()), (String) entry.getValue(), entry.getKey());
        }
      } else {
        uploadDocuments(new ArrayList<>(uploadMap.values()), defaultEmbeddingField, PineconeUtils.getDefaultNamespace());
      }
    }
  }

  private void validateUploadRequirements() throws IndexerException {
    if (namespaces == null && defaultEmbeddingField == null) {
      throw new IndexerException(
          "Both defaultEmbeddingField and namespaces cannot be null when uploading documents.");
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
      index.deleteByIds(idsToDelete, namespace);
    } catch (Exception e) {
      throw new IndexerException("Error while deleting vectors.", e);
    }
  }

  private void upsertDocuments(List<VectorWithUnsignedIndices> upsertVectors, String namespace) throws IndexerException {
    try {
      UpsertResponse response = index.upsert(upsertVectors, namespace);

      // check response for upsertedCount to be equal to upsertVectors
      if (response.getUpsertedCount() != upsertVectors.size()) {
        throw new IndexerException("Number of upserted vectors to pinecone does not match the number of upserted vectors requested to upsert.");
      }
    } catch (Exception e) {
      throw new IndexerException("Error while upserting vectors.", e);
    }
  }

  private void updateDocuments(List<Document> documents, String embeddingField, String namespace) throws IndexerException {
    try {
      documents.forEach(doc -> {
        log.debug("Updating docId: {} namespace: {} embedding: {}", doc.getId(), namespace, doc.getFloatList(embeddingField));
        // does not validate the existence of IDs within the index, if no records are affected, a 200 OK status is returned
        // will only throw error if doc.getId() is null
        index.update(doc.getId(), doc.getFloatList(embeddingField), namespace);
      });
    } catch (Exception e) {
      throw new IndexerException("Error while updating vectors.", e);
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
