package com.kmwllc.lucille.opensearch.indexer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.auto.service.AutoService;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.IndexerRetryableException;
import com.kmwllc.lucille.core.JsonDocumentWriter;
import com.kmwllc.lucille.core.JsonDocumentWriter.ValueWriter;
import com.kmwllc.lucille.core.KafkaDocument;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.indexer.IndexerType;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.opensearch.util.OpenSearchUtils;
import com.typesafe.config.Config;
import jakarta.json.JsonException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.json.JsonpSerializable;
import org.opensearch.client.json.jackson.JacksonJsonpGenerator;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.BulkByScrollFailure;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch._types.VersionType;
import org.opensearch.client.opensearch._types.query_dsl.BoolQuery;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.DeleteByQueryRequest;
import org.opensearch.client.opensearch.core.DeleteByQueryResponse;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.transport.httpclient5.ResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Indexes documents to OpenSearch using the Java client.
 * Additional parameters are made available by the {@link com.kmwllc.lucille.core.Indexer} abstract class.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>index (String, Required) : Target OpenSearch index name.</li>
 *   <li>url (String or List&lt;String&gt;, Required) : OpenSearch HTTP endpoint (e.g., https://localhost:9200), or a list of
 *   endpoints for the same cluster. With a list, the client round-robins requests across the hosts and fails over between them.</li>
 *   <li>update (Boolean, Optional) : Use partial update API instead of index/replace. Defaults to false.</li>
 *   <li>acceptInvalidCert (Boolean, Optional) : Allow invalid TLS certificates. Defaults to false.</li>
 *   <li>useCompression (Boolean, Optional) : Whether to use compression in the underlying OpenSearch HTTP client. Defaults to false.</li>
 *   <li>connectTimeoutMs (Long, Optional) : Maximum time to establish a connection. Defaults to 5000.</li>
 *   <li>socketTimeoutMs (Long, Optional) : Maximum time to wait for data on an open connection, including a bulk or
 *   delete-by-query response. A timeout is a transport failure, retried when indexer.maxRetries is set. Defaults to
 *   30000; raise it for long deletes, or set it to -1 for no limit.</li>
 *   <li>connectionTimeToLiveMs (Long, Optional) : Maximum lifetime of a pooled connection, so long-running indexers
 *   behind a load balancer or Kubernetes Service reconnect and reach new nodes. Defaults to 300000 (5 minutes).</li>
 *   <li>childDocumentsField (String, Optional) : Field name under which attached child documents are nested in the
 *   indexed document. If not set, child documents are not indexed. For child queries to work correctly, this field should be mapped
 *   as type "nested" in the index mapping.</li>
 *   <li>versionConflictIsSuccess (Boolean, Optional) : Treat a 409 version conflict on an upload or a versioned delete by id
 *   as success, since with external versioning it means the index already holds a version at least as new. Requires
 *   indexer.versionType External or ExternalGte. Defaults to false.</li>
 *   <li>splitOnRequestTooLarge (Boolean, Optional) : When OpenSearch rejects a bulk upload with HTTP 413 (a body over
 *   http.max_content_length), split it in half and send each half on its own. A single document that still gets a 413
 *   fails permanently. Defaults to false.</li>
 *   <li>indexer.routingField (String, Optional) : Document field that supplies the routing key.</li>
 *   <li>indexer.versionType (String, Optional) : Versioning type when using external versions. With External or ExternalGte,
 *   an upload carries the document's indexer.versionField value, or else its Kafka offset.</li>
 *   <li>indexer.versionField (String, Optional) : Document field holding the external version. Requires
 *   indexer.versionType. With External or ExternalGte, a delete by id carries this field's value too; a delete without it
 *   is unversioned, and never uses the Kafka offset.</li>
 * </ul>
 * <p>
 * When indexer.maxRetries is set, a retry of a batch doesn't send again the uploads OpenSearch accepted or the ones that
 * failed permanently, and once part of a batch is accepted, an exception from a later request fails only the documents
 * in that request.
 * <p>
 * A JsonDocument's body is written straight from its tree into the client's Jackson generator, with the same JSON as a
 * Map of the document, so the batch isn't copied before it is sent. See {@link JsonDocumentWriter}.
 */
@AutoService(Indexer.class)
@IndexerType("OpenSearch")
public class OpenSearchIndexer extends Indexer {

  public static final Spec SPEC = SpecBuilder.indexer()
      .requiredString("index")
      .requiredStringOrList("url")
      .optionalBoolean("update", "acceptInvalidCert", "useCompression", "versionConflictIsSuccess",
          "splitOnRequestTooLarge")
      .optionalNumber("maxConnectionsPerRoute", "maxConnectionsTotal",
          "connectTimeoutMs", "socketTimeoutMs", "connectionTimeToLiveMs")
      .optionalString("childDocumentsField").build();

  private static final Logger log = LoggerFactory.getLogger(OpenSearchIndexer.class);

  private static final int HTTP_CONFLICT = 409;
  private static final int HTTP_REQUEST_TOO_LARGE = 413;

  private final OpenSearchClient client;
  private final String index;

  private final String routingField;

  private final VersionType versionType;
  private final String versionField;

  //flag for using partial update API when sending documents to opensearch
  private final boolean update;

  private final String childDocumentsField;

  // treat a 409 version conflict on an upload or delete by id as success
  private final boolean versionConflictIsSuccess;

  // split a bulk upload OpenSearch rejects with HTTP 413 in half, and retry each half
  private final boolean splitOnRequestTooLarge;

  // the attempts at the batch this thread is sending, when indexer.maxRetries is set. See sendAttempt.
  private final ThreadLocal<BatchAttempt> batchAttempt = new ThreadLocal<>();

  public OpenSearchIndexer(Config config, IndexerMessenger messenger, boolean bypass,
      String metricsPrefix,String localRunId, OpenSearchClient client) {
    super(config, messenger, bypass, metricsPrefix, localRunId);

    this.client = client;
    this.index = OpenSearchUtils.getOpenSearchIndex(config);
    this.routingField = ConfigUtils.getStringOrDefault(config, "indexer.routingField", null);
    this.update = ConfigUtils.getBooleanOrDefault(config, "opensearch.update", false);
    this.versionType =
        config.hasPath("indexer.versionType") ? parseVersionType(config.getString("indexer.versionType")) : null;
    this.childDocumentsField = ConfigUtils.getStringOrDefault(config, "opensearch.childDocumentsField", null);
    this.versionField = ConfigUtils.getStringOrDefault(config, "indexer.versionField", null);

    // validate config indexer.versionType that must be set if config indexer.versionField is set
    if (this.versionField != null && this.versionType == null) {
      throw new IllegalArgumentException("indexer.versionType must be set if indexer.versionField is set");
    }

    this.versionConflictIsSuccess = ConfigUtils.getOrDefault(config, "opensearch.versionConflictIsSuccess", false);
    if (versionConflictIsSuccess && versionType != VersionType.External && versionType != VersionType.ExternalGte) {
      throw new IllegalArgumentException(
          "opensearch.versionConflictIsSuccess requires indexer.versionType to be External or ExternalGte");
    }
    this.splitOnRequestTooLarge = ConfigUtils.getOrDefault(config, "opensearch.splitOnRequestTooLarge", false);
  }

  public OpenSearchIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix, String localRunId) throws IndexerException {
    this(config, messenger, bypass, metricsPrefix, localRunId, getClient(config, bypass));
  }

  // Convenience Constructors
  public OpenSearchIndexer(Config config, IndexerMessenger messenger, String metricsPrefix, OpenSearchClient client) {
    this(config, messenger, false, metricsPrefix, null, client);
  }

  @Override
  protected String getIndexerConfigKey() { return "opensearch"; }

  // The OpenSearch Java client is thread-safe over a shared connection pool, so concurrent sends through the single
  // shared client are safe. The pool is sized for the configured concurrency by AsyncConnectionPoolUtils.
  @Override
  protected boolean supportsConcurrentSends() {
    return true;
  }

  private static OpenSearchClient getClient(Config config, boolean bypass) throws IndexerException {
    try {
      return bypass ? null : OpenSearchUtils.getOpenSearchRestClient(config);
    } catch (Exception e) {
      throw new IndexerException("Couldn't create OpenSearch client", e);
    }
  }

  @Override
  public boolean validateConnection() {
    if (bypass) {
      return true;
    }
    boolean response;
    try {
      response = client.ping().value();
    } catch (Exception e) {
      log.error("Couldn't ping OpenSearch ", e);
      return false;
    }
    if (!response) {
      log.error("Non true response when pinging OpenSearch: {}", response);
      return false;
    }
    return true;
  }

  @Override
  public void closeConnection() {
    if (!bypass && client != null && client._transport() != null) {
      try {
        client._transport().close();
      } catch (Exception e) {
        log.error("Error closing Opensearchclient", e);
      }
    }
  }

  @Override
  protected Set<Pair<Document, Exception>> sendToIndex(List<Document> documents) throws Exception {
    // skip indexing if there is no indexer client
    if (bypass) {
      return Set.of();
    }

    Map<Pair<String, String>, Document> documentsToUpload = new LinkedHashMap<>();
    // the document that marked each (index, id) for deletion, so deleteById can send its version
    Map<Pair<String, String>, Document> idsToDelete = new LinkedHashMap<>();
    Map<Pair<String, String>, List<String>> termsToDeleteByQuery = new LinkedHashMap<>();
    // the documents whose values are in termsToDeleteByQuery
    List<Document> docsToDeleteByQuery = new ArrayList<>();
    Set<Pair<Document, Exception>> failedDocs = new HashSet<>();

    // populate which collection each document belongs to
    // upload if document is not marked for deletion
    // else if document is marked for deletion ONLY, then only add to idsToDelete
    // else document is marked for deletion AND contains deleteByFieldField and deleteByFieldValue, only add to termsToDeleteByQuery
    for (Document doc : documents) {
      // use doc id override if specified, otherwise delete will try to delete wrong id
      String id = Optional.ofNullable(getDocIdOverride(doc)).orElse(doc.getId());

      // if an index override field has been specified, use its value as the index to send to opensearch,
      final String indexToSend = Optional.ofNullable(getIndexOverride(doc)).orElse(index);

      Pair<String, String> indexAndId = Pair.of(indexToSend, id);

      if (!isMarkedForDeletion(doc)) {
        idsToDelete.remove(indexAndId);
        documentsToUpload.put(indexAndId, doc);
      } else {
        documentsToUpload.remove(indexAndId);
        if (!hasDeleteByFieldValues(doc)) {
          idsToDelete.put(indexAndId, doc);
        } else {
          //indexer.deleteByFieldField gives you the field in the document that holds which field whose value is queried for
          // deletion, so we need to do doc.getString(deleteByFieldField).
          String field = doc.getString(deleteByFieldField);
          String value = doc.getString(deleteByFieldValue);
          // a null field or value can't go in a terms query, and would fail the whole batch when the query is built
          if (field == null || value == null) {
            failedDocs.add(Pair.of(doc, new IndexerException("Document " + doc.getId() + " is marked for delete by query "
                + "but has a null " + (field == null ? deleteByFieldField : deleteByFieldValue))));
            continue;
          }
          Pair<String, String> indexAndDeleteByField = Pair.of(indexToSend, field);
          if (!termsToDeleteByQuery.containsKey(indexAndDeleteByField)) {
            termsToDeleteByQuery.put(indexAndDeleteByField, new ArrayList<>());
          }
          termsToDeleteByQuery.get(indexAndDeleteByField).add(value);
          docsToDeleteByQuery.add(doc);
        }
      }
    }

    // Without retries this sends everything and fails the whole batch on a thrown exception, as it always has.
    BatchAttempt attempt = startAttempt(documents);
    if (attempt == null) {
      failedDocs.addAll(uploadDocuments(documentsToUpload));
      deleteById(idsToDelete);
      deleteByQuery(termsToDeleteByQuery);
      return failedDocs;
    }

    Throwable thrown = null;
    try {
      sendAttempt(attempt, documentsToUpload, idsToDelete, termsToDeleteByQuery, docsToDeleteByQuery, failedDocs);
      return failedDocs;
    } catch (Throwable t) {
      thrown = t;
      throw t;
    } finally {
      finishAttempt(attempt, thrown, failedDocs);
    }
  }

  /**
   * One attempt at sending a batch when indexer.maxRetries is set. The base Indexer retries a batch by calling
   * sendToIndex again with the same List, so this remembers, across the attempts at one batch, which uploads OpenSearch
   * accepted and which failed in a way a retry can't fix:
   * <ul>
   *   <li>An upload accepted on an earlier attempt is not sent again, and counts as succeeded. Re-sending it is not
   *   harmless: with ExternalGte, an equal version is a real write, and it can re-create a document that a newer delete
   *   removed once that delete's tombstone has expired (index.gc_deletes).</li>
   *   <li>An upload that failed with a non-retryable error is not sent again, and its failure is reported again.</li>
   *   <li>Once any upload in the batch is accepted, an exception thrown by the uploads or either delete is returned as a
   *   failure of each document that request was sending, instead of failing the whole batch, accepted documents
   *   included. A retryable exception still makes the base Indexer retry the batch.</li>
   * </ul>
   * Deletes are sent again on every attempt.
   */
  private void sendAttempt(BatchAttempt attempt, Map<Pair<String, String>, Document> documentsToUpload,
      Map<Pair<String, String>, Document> idsToDelete, Map<Pair<String, String>, List<String>> termsToDeleteByQuery,
      List<Document> docsToDeleteByQuery, Set<Pair<Document, Exception>> failedDocs) throws Exception {
    Map<Pair<String, String>, Document> pendingUploads = new LinkedHashMap<>();
    for (Map.Entry<Pair<String, String>, Document> entry : documentsToUpload.entrySet()) {
      Document doc = entry.getValue();
      if (attempt.permanentFailures.containsKey(doc)) {
        failedDocs.add(Pair.of(doc, attempt.permanentFailures.get(doc)));
      } else if (!attempt.accepted.contains(doc)) {
        pendingUploads.put(entry.getKey(), doc);
      }
    }

    Set<Pair<Document, Exception>> uploadFailures;
    try {
      uploadFailures = uploadDocuments(pendingUploads);
    } catch (IndexerException e) {
      if (attempt.accepted.isEmpty()) {
        throw e;
      }
      uploadFailures = failEach(pendingUploads.values(), e);
    }
    Set<Document> failedUploads = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Pair<Document, Exception> failure : uploadFailures) {
      failedUploads.add(failure.getLeft());
      if (!isRetryable(failure.getRight())) {
        attempt.permanentFailures.put(failure.getLeft(), failure.getRight());
      }
    }
    for (Document doc : pendingUploads.values()) {
      if (!failedUploads.contains(doc)) {
        attempt.accepted.add(doc);
      }
    }
    failedDocs.addAll(uploadFailures);

    try {
      deleteById(idsToDelete);
    } catch (IndexerException e) {
      if (attempt.accepted.isEmpty()) {
        throw e;
      }
      failedDocs.addAll(failEach(idsToDelete.values(), e));
    }
    try {
      deleteByQuery(termsToDeleteByQuery);
    } catch (IndexerException e) {
      if (attempt.accepted.isEmpty()) {
        throw e;
      }
      failedDocs.addAll(failEach(docsToDeleteByQuery, e));
    }
  }

  // Returns the state of the attempts at this batch, or null when retries are disabled. Each sending thread runs its
  // batch's attempts in turn (sendWithRetry), so concurrent batches each have their own.
  private BatchAttempt startAttempt(List<Document> documents) {
    if (getMaxRetries() == 0) {
      return null;
    }
    BatchAttempt attempt = batchAttempt.get();
    if (attempt == null || attempt.batch != documents) {
      attempt = new BatchAttempt(documents);
      batchAttempt.set(attempt);
    }
    attempt.attempts++;
    return attempt;
  }

  // Forgets the batch once the base Indexer won't retry it, by the same rule it uses, so the documents aren't kept.
  private void finishAttempt(BatchAttempt attempt, Throwable thrown, Set<Pair<Document, Exception>> failedDocs) {
    boolean willRetry = attempt.attempts <= getMaxRetries()
        && (thrown != null ? isRetryable(thrown) : failedDocs.stream().anyMatch(p -> isRetryable(p.getRight())));
    if (!willRetry) {
      batchAttempt.remove();
    }
  }

  private static Set<Pair<Document, Exception>> failEach(Collection<Document> docs, Exception e) {
    Set<Pair<Document, Exception>> failedDocs = new HashSet<>();
    for (Document doc : docs) {
      failedDocs.add(Pair.of(doc, e));
    }
    return failedDocs;
  }

  private static final class BatchAttempt {
    private final List<Document> batch;
    // the number of times the batch has been sent, this attempt included
    private int attempts;
    // identity sets, so documents are never hashed
    private final Set<Document> accepted = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Map<Document, Exception> permanentFailures = new IdentityHashMap<>();

    private BatchAttempt(List<Document> batch) {
      this.batch = batch;
    }
  }

  private void deleteById(Map<Pair<String, String>, Document> idsToDelete) throws Exception {
    if (idsToDelete.isEmpty()) {
      return;
    }

    BulkRequest.Builder br = new BulkRequest.Builder();
    for (Map.Entry<Pair<String, String>, Document> entry : idsToDelete.entrySet()) {
      Pair<String, String> indexAndId = entry.getKey();
      Long versionNum = getDeleteVersionNum(entry.getValue());
      br.operations(op -> op
          .delete(d -> {
            d.index(indexAndId.getLeft()).id(indexAndId.getRight());
            // a versioned delete is rejected (409) when the index holds a newer version, and leaves a tombstone that
            // rejects an older upload arriving after it, for as long as the index keeps tombstones (index.gc_deletes)
            if (versionNum != null) {
              d.versionType(versionType).version(versionNum);
            }
            return d;
          })
      );
    }

    BulkResponse response;
    try {
      response = client.bulk(br.build());
    } catch (OpenSearchException e) {
      throw new IndexerRetryableException(e.status(), "OpenSearch returned HTTP " + e.status(), e);
    } catch (IOException e) {
      throw new IndexerRetryableException("Transport failure communicating with OpenSearch", e);
    }

    if (response.errors()) {
      boolean failed = false;
      for (BulkResponseItem item : response.items()) {
        if (item.error() != null) {
          if (isVersionConflictSuccess(item)) {
            log.debug("Delete of id: {} superseded by a newer version (409), treating as success: {}", item.id(),
                item.error().reason());
            continue;
          }
          failed = true;
          log.debug("Error while deleting id: {} because: {}", item.id(), item.error().reason());
        }
      }
      if (failed) {
        throw new IndexerException("encountered errors while deleting documents");
      }
    }
  }

  private void deleteByQuery(Map<Pair<String, String>, List<String>> termsToDeleteByQuery) throws Exception {
    if (termsToDeleteByQuery.isEmpty()) {
      return;
    }
    /*
      each entry from termsToDeleteByQuery would add a "terms" query to the should clause. In each entry, the key represents
      the field to look for while querying and values represent the values to look for in that field. We set the
      minimum should match to 1 such that any documents that passes a single terms query would end
      up in the result set. For a document to pass a terms query, it must contain that field and in that field
      contain any one of the field values.
      e.g. termsToDeleteByQuery -> {
                                      "field1" : ["value1", "value2", "value3"],
                                      "field2" : ["valueA", "valueB"]
                                   }
      would create a delete query:
      {
        "query": {
          "bool": {
            "should": [
              {
                "terms": {
                  "field1": ["value1", "value2", "value3"]
                }
              },
              {
                "terms": {
                  "field2": ["valueA", "valueB"]
                }
              }
            ],
            "minimum_should_match": "1"
          }
        }
      }
     */

    // Send a separate delete by query for each index.
    Map<String, BoolQuery.Builder> boolQueryBuilders = new HashMap<>();

    for (Map.Entry<Pair<String, String>, List<String>> entry : termsToDeleteByQuery.entrySet()) {
      Pair<String, String> indexAndField = entry.getKey();
      List<String> values = entry.getValue();
      BoolQuery.Builder boolQuery;
      if ( boolQueryBuilders.containsKey(indexAndField.getLeft()) ) {
        boolQuery = boolQueryBuilders.get(indexAndField.getLeft());
      } else {
        boolQuery = new BoolQuery.Builder();
        boolQueryBuilders.put(indexAndField.getLeft(), boolQuery);
      }
      boolQuery.should(s -> s
          .terms(t -> t
              .field(indexAndField.getRight())
              .terms(tt -> tt.value(values.stream()
                  .map(FieldValue::of)
                  .collect(Collectors.toList()))
              )
          )
      );
    }

    for (Map.Entry<String, BoolQuery.Builder> entry : boolQueryBuilders.entrySet()) {
      BoolQuery.Builder boolQuery = entry.getValue();

      boolQuery.minimumShouldMatch("1");

      DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest.Builder()
          .index(entry.getKey())
          .query(q -> q.bool(boolQuery.build()))
          .build();

      DeleteByQueryResponse response;
      try {
        response = client.deleteByQuery(deleteByQueryRequest);
      } catch (OpenSearchException e) {
        throw new IndexerRetryableException(e.status(), "OpenSearch returned HTTP " + e.status(), e);
      } catch (IOException e) {
        throw new IndexerRetryableException("Transport failure communicating with OpenSearch", e);
      }

      if (!response.failures().isEmpty()) {
        for (BulkByScrollFailure failure : response.failures()) {
          if (failure.cause() == null) {
            log.debug("Error while deleting by query, cause was null");
          } else {
            log.debug("Error while deleting by query: {}, because of: {}", failure.cause().reason(), failure.cause());
          }

        }
        throw new IndexerException("encountered errors while deleting by query");
      }
    }
  }

  /**
   * Uploads the documents in one bulk request. With opensearch.splitOnRequestTooLarge, a request OpenSearch rejects as
   * too large (HTTP 413, a body over http.max_content_length) is split in half and each half is uploaded on its own, so
   * one oversized document doesn't fail the rest of its batch. A single document that still gets a 413 fails
   * permanently. Once a request is split, each half's exception is returned as a failure of each of its documents, since
   * the other half may already be indexed.
   */
  private Set<Pair<Document, Exception>> uploadDocuments(Map<Pair<String, String>, Document> documentsToUpload) throws IndexerException {
    if (documentsToUpload.isEmpty()) {
      return Set.of();
    }

    try {
      return sendUploadBatch(documentsToUpload);
    } catch (IndexerRetryableException e) {
      if (!splitOnRequestTooLarge || e.getStatusCode() != HTTP_REQUEST_TOO_LARGE) {
        throw e;
      }
      if (documentsToUpload.size() == 1) {
        Map.Entry<Pair<String, String>, Document> only = documentsToUpload.entrySet().iterator().next();
        log.error("OpenSearch rejected document {} for index {} on its own with HTTP 413: it exceeds http.max_content_length.",
            only.getKey().getRight(), only.getKey().getLeft());
        return Set.of(Pair.of(only.getValue(), new IndexerException(
            "Document exceeds OpenSearch http.max_content_length (HTTP 413) when sent on its own", e)));
      }

      List<Map.Entry<Pair<String, String>, Document>> entries = new ArrayList<>(documentsToUpload.entrySet());
      int mid = entries.size() / 2;
      log.warn("OpenSearch rejected a bulk request of {} documents with HTTP 413; splitting it into {} and {}.",
          entries.size(), mid, entries.size() - mid);
      Set<Pair<Document, Exception>> failedDocs = new HashSet<>(uploadSplitPart(entries.subList(0, mid)));
      failedDocs.addAll(uploadSplitPart(entries.subList(mid, entries.size())));
      return failedDocs;
    }
  }

  private Set<Pair<Document, Exception>> uploadSplitPart(List<Map.Entry<Pair<String, String>, Document>> entries) {
    Map<Pair<String, String>, Document> part = new LinkedHashMap<>();
    for (Map.Entry<Pair<String, String>, Document> entry : entries) {
      part.put(entry.getKey(), entry.getValue());
    }
    try {
      return uploadDocuments(part);
    } catch (IndexerException e) {
      return failEach(part.values(), e);
    }
  }

  private Set<Pair<Document, Exception>> sendUploadBatch(Map<Pair<String, String>, Document> documentsToUpload) throws IndexerException {
    Map<String, Document> uploadedDocuments = new HashMap<>();
    BulkRequest.Builder br = new BulkRequest.Builder();

    for (Map.Entry<Pair<String, String>, Document> entry : documentsToUpload.entrySet()) {
      String indexToSend = entry.getKey().getLeft();
      String docId = entry.getKey().getRight();
      Document doc = entry.getValue();

      Object indexerDoc = getUploadBody(doc, docId);

      uploadedDocuments.put(docId, doc);

      Long versionNum = (versionType == VersionType.External || versionType == VersionType.ExternalGte)
          ? getVersionNum(doc)
          : null;

      if (update) {
        br.operations(op -> op
            .update((up) -> {
              up.index(indexToSend).id(docId);
              if (routingField != null) {
                up.routing(doc.getString(routingField));
              }
              if (versionNum != null) {
                up.versionType(versionType).version(versionNum);
              }
              return up.document(indexerDoc);
            }));
      } else {
        br.operations(op -> op
            .index((up) -> {
              up.index(indexToSend).id(docId);
              if (routingField != null) {
                up.routing(doc.getString(routingField));
              }
              if (versionNum != null) {
                up.versionType(versionType).version(versionNum);
              }
              return up.document(indexerDoc);
            }));
      }
    }

    Set<Pair<Document, Exception>> failedDocs = new HashSet<>();

    BulkResponse response;
    try {
      response = client.bulk(br.build());
    } catch (OpenSearchException e) {
      // HTTP-level error with a known status code — wrap so the base Indexer can apply retry policy
      throw new IndexerRetryableException(e.status(), "OpenSearch returned HTTP " + e.status(), e);
    } catch (IOException e) {
      // Transport-level failure (connection refused, timeout, etc.) — no status code available, except for a 413 when
      // opensearch.splitOnRequestTooLarge is set
      throw transportFailure(e);
    }

    if (response != null) {
      for (BulkResponseItem item : response.items()) {
        if (item.error() != null) {
          if (isVersionConflictSuccess(item)) {
            log.debug("Upload of id: {} superseded by a newer version (409), treating as success: {}", item.id(),
                item.error().reason());
            continue;
          }
          // For the error - if the id is a document's id, then it failed, and we add it to the set.
          // If not, we don't know what the error is, and opt to throw an actual IndexerException instead.
          if (uploadedDocuments.containsKey(item.id())) {
            Document failedDoc = uploadedDocuments.get(item.id());
            // item.status() is always an HTTP status code. The OpenSearch bulk API defines the per-item
            // status field as the HTTP status of that individual operation (e.g. 200, 201, 400, 409, 429,
            // 503). This is consistent across all OpenSearch client libraries and confirmed by the
            // OpenSearch REST API documentation and source — there are no documented non-HTTP values.
            failedDocs.add(Pair.of(failedDoc, new IndexerRetryableException(item.status(),
                "OpenSearch bulk item error (status " + item.status() + "): " + item.error().reason(), null)));
          } else {
            throw new IndexerException(item.error().reason());
          }
        }
      }
    }

    return failedDocs;
  }

  // With opensearch.splitOnRequestTooLarge, recovers the status of a 413 from the cause chain. OpenSearch's 413 for a
  // body over http.max_content_length has no error body the client can parse, so it arrives as an IOException carrying
  // an httpclient5 ResponseException, not as an OpenSearchException. Other statuses stay unknown, as before.
  private IndexerRetryableException transportFailure(IOException e) {
    if (splitOnRequestTooLarge) {
      for (Throwable t = e; t != null; t = (t.getCause() == t ? null : t.getCause())) {
        if (t instanceof ResponseException responseException && responseException.status() == HTTP_REQUEST_TOO_LARGE) {
          return new IndexerRetryableException(HTTP_REQUEST_TOO_LARGE,
              "OpenSearch returned HTTP 413: request body exceeds http.max_content_length", e);
        }
      }
    }
    return new IndexerRetryableException("Transport failure communicating with OpenSearch", e);
  }

  // With opensearch.versionConflictIsSuccess, a 409 means the index already holds a version at least as new as the
  // operation's (the option requires External or ExternalGte), so a newer write superseded it.
  private boolean isVersionConflictSuccess(BulkResponseItem item) {
    return versionConflictIsSuccess && item.status() == HTTP_CONFLICT;
  }

  /**
   * Returns the body of a document's index or update operation. For a JsonDocument, this is a {@link JsonDocumentBody}
   * that writes the document's own tree into the client's Jackson generator when the request is serialized, with the
   * same JSON as {@link #getMapBody}, but without building a Map of every document in the batch first. Any other
   * Document gets the Map.
   */
  Object getUploadBody(Document doc, String docId) {
    if (!JsonDocumentWriter.canWrite(doc)) {
      return getMapBody(doc, docId);
    }

    // the fields written in place of the document's own, or added, as getMapBody puts them
    Map<String, ValueWriter> replacements = new LinkedHashMap<>();
    if (fieldFilter.shouldInclude(Document.ID_FIELD)) {
      replacements.put(Document.ID_FIELD, generator -> generator.writeString(docId));
    }
    if (doc.hasChildren() && hasChildDocumentsField(doc)) {
      List<Document> children = doc.getChildren();
      replacements.put(childDocumentsField, generator -> {
        generator.writeStartArray();
        for (Document child : children) {
          writeDocument(child, generator, Map.of());
        }
        generator.writeEndArray();
      });
    }
    return new JsonDocumentBody(doc, replacements);
  }

  /** Returns the body of a document's index or update operation as a Map. */
  Map<String, Object> getMapBody(Document doc, String docId) {
    // removing fields in the blacklist or not in the whitelist in configurations
    Map<String, Object> indexerDoc = getRawIndexerDoc(doc);

    // only add id if our fieldFilter allows it (based on our blacklist and whitelist)
    // - Case 1: id and idOverride filtered out -> idOverride used by Indexer, both removed from Document (tested in testBlacklistWithOverride)
    // - Case 2: id is filtered, idOverride exists -> idOverride used by Indexer, only id field removed from Document (tested in testBlacklistWithOverride2)
    // - Case 3: id is filtered, idOverride null -> id used by Indexer, id also removed from Document (tested in testRouting)
    // - Case 4: both unfiltered, idOverride exists -> idOverride used by Indexer, id and idOverride field exist in Document (tested in testOverride)
    // - Case 5: both unfiltered, idOverride null -> document id remains and used by Indexer (Default case & tested)
    if (fieldFilter.shouldInclude(Document.ID_FIELD)) {
      indexerDoc.put(Document.ID_FIELD, docId);
    }

    if (doc.hasChildren()) {
      addChildren(doc, indexerDoc);
    }
    return indexerDoc;
  }

  private boolean hasChildDocumentsField(Document doc) {
    if (childDocumentsField == null) {
      log.warn("Document {} has children but opensearch.childDocumentsField is not configured. They will not be indexed.", doc.getId());
      return false;
    }
    return true;
  }

  // the field filter applies to children too, and grandchildren are dropped, as in addChildren
  private void writeDocument(Document doc, JsonGenerator generator, Map<String, ValueWriter> replacements)
      throws IOException {
    JsonDocumentWriter.writeObject(doc, generator, fieldFilter, replacements);
  }

  /** Only call on documents that have children. */
  private void addChildren(Document doc, Map<String, Object> indexerDoc) {
    if (!hasChildDocumentsField(doc)) {
      return;
    }

    List<Map<String, Object>> childDocMaps = new ArrayList<>();
    List<Document> children = doc.getChildren();

    for (Document child : children) {
      // calling getRawIndexerDoc applies black/whitelist and drops nested children, which we don't support
      childDocMaps.add(getRawIndexerDoc(child));
    }
    indexerDoc.put(childDocumentsField, childDocMaps);
  }

  /**
   * The body of a JsonDocument's index or update operation. The client hands a {@link JsonpSerializable} its own
   * generator, so this writes the document's tree into the same Jackson generator, and so with the same number
   * formatting, string escaping and Base64, that the client's mapper would write a Map with. The body holds the
   * document until the request is serialized, in client.bulk.
   */
  private final class JsonDocumentBody implements JsonpSerializable {

    private final Document doc;
    private final Map<String, ValueWriter> replacements;

    private JsonDocumentBody(Document doc, Map<String, ValueWriter> replacements) {
      this.doc = doc;
      this.replacements = replacements;
    }

    /**
     * @throws IllegalArgumentException if the client doesn't serialize with Jackson. OpenSearchUtils always builds the
     *     client with a JacksonJsonpMapper; the failure is not retried.
     */
    @Override
    public void serialize(jakarta.json.stream.JsonGenerator generator, JsonpMapper mapper) {
      if (!(generator instanceof JacksonJsonpGenerator jacksonGenerator)) {
        throw new IllegalArgumentException("OpenSearchIndexer can only write a document to a Jackson generator, not "
            + generator.getClass().getName() + "; build the client with a JacksonJsonpMapper");
      }
      try {
        writeDocument(doc, jacksonGenerator.jacksonGenerator(), replacements);
      } catch (IOException e) {
        throw new JsonException("Couldn't write document " + doc.getId(), e);
      }
    }

    @Override
    public String toString() {
      return doc.toString();
    }
  }

  // Accepts both the enum constant name (External, ExternalGte) and the JSON spelling (external, external_gte).
  private static VersionType parseVersionType(String name) {
    for (VersionType type : VersionType.values()) {
      if (type.name().equalsIgnoreCase(name) || type.jsonValue().equalsIgnoreCase(name)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unknown indexer.versionType \"" + name + "\"; expected one of "
        + java.util.Arrays.toString(VersionType.values()));
  }

  private Long getVersionNum(Document doc) {
    if (versionField != null && doc.has(versionField)) {
      return doc.getLong(versionField);
    }
    if (doc instanceof KafkaDocument kafkaDoc) {
      return kafkaDoc.getOffset();
    }
    return null;
  }

  // The version to send with a delete by id, or null for an unversioned delete. Unlike getVersionNum, this never falls
  // back to the Kafka offset: the offset of a delete is not comparable to a version taken from indexer.versionField, so
  // only a document that carries indexer.versionField gets a versioned delete.
  private Long getDeleteVersionNum(Document doc) {
    if ((versionType == VersionType.External || versionType == VersionType.ExternalGte)
        && versionField != null && doc.has(versionField)) {
      return doc.getLong(versionField);
    }
    return null;
  }
}
