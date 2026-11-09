package com.kmwllc.lucille.opensearch.indexer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.auto.service.AutoService;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerContext;
import com.kmwllc.lucille.core.IndexerFailure;
import com.kmwllc.lucille.core.PendingSend;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.IndexerRetryableException;
import com.kmwllc.lucille.core.JsonDocumentWriter;
import com.kmwllc.lucille.core.JsonDocumentWriter.ValueWriter;
import com.kmwllc.lucille.core.KafkaDocument;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.indexer.IndexerType;
import com.kmwllc.lucille.opensearch.util.OpenSearchUtils;
import com.typesafe.config.Config;
import jakarta.json.JsonException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
 *   <li>update (Boolean, Optional) : Use partial update API instead of index/replace, creating the document if it does not exist. Defaults to false.</li>
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
 *   <li>routingField (String, Optional) : Document field that supplies the routing key.</li>
 *   <li>versionType (String, Optional) : Versioning type when using external versions. With External or ExternalGte,
 *   an upload carries the document's versionField value, or else its Kafka offset. Offsets are only comparable within
 *   one partition of one topic; if the partition count can change, the topic can be recreated, or idOverride maps
 *   different documents to one destination id, set versionField, since a newer write with a lower offset is rejected
 *   with a 409 and reported as succeeded.</li>
 *   <li>versionField (String, Optional) : Document field holding the external version. Requires
 *   versionType. With External or ExternalGte, a delete by id carries this field's value too; a delete without it
 *   is unversioned, and never uses the Kafka offset.</li>
 *   <li>splitOnRequestTooLarge (Boolean, Optional) : When OpenSearch rejects a bulk upload with HTTP 413 (a body over
 *   http.max_content_length), split it in half and send each half on its own. A single document that still gets a 413
 *   fails permanently. Defaults to false.</li>
 * </ul>
 * <p>
 * With versionType External or ExternalGte, a 409 on an upload or delete by id sent with a version is a success: the
 * index already holds that version or a newer one.
 * <p>
 * A batch is sent in input order, with a delete-by-query never sent ahead of an earlier upload in the batch. Each
 * document is reported on its own: once part of a send has been applied, an exception from a later request fails only
 * the documents in that request, and a delete by id that OpenSearch rejects fails only that document. When
 * indexer.maxRetries is set, the documents after a retryable failure are held back until the retry, so the retry resumes
 * in input order.
 * <p>
 * Each document's body is written straight from its tree into the client's Jackson generator, with the same JSON as a
 * Map of the document, so the batch isn't copied before it is sent. See {@link JsonDocumentWriter}.
 */
@AutoService(Indexer.class)
@IndexerType("OpenSearch")
public class OpenSearchIndexer extends Indexer {

  public static final Spec SPEC = SpecBuilder.indexer()
      .requiredString("index")
      .requiredStringOrList("url")
      .optionalBoolean("update", "acceptInvalidCert", "useCompression", "splitOnRequestTooLarge")
      .optionalNumber("maxConnectionsPerRoute", "maxConnectionsTotal",
          "connectTimeoutMs", "socketTimeoutMs", "connectionTimeToLiveMs")
      .optionalString("childDocumentsField", "routingField", "versionType", "versionField").build();

  private static final Logger log = LoggerFactory.getLogger(OpenSearchIndexer.class);

  private static final int HTTP_CONFLICT = 409;
  private static final int HTTP_REQUEST_TOO_LARGE = 413;

  // not final: the standard constructor builds it only after validation, see there
  private OpenSearchClient client;
  private final String index;

  private final String routingField;

  private final VersionType versionType;
  private final String versionField;

  //flag for using partial update API when sending documents to opensearch
  private final boolean update;

  private final String childDocumentsField;

  // split a bulk upload OpenSearch rejects with HTTP 413 in half, and send each half on its own
  private final boolean splitOnRequestTooLarge;

  public OpenSearchIndexer(Config config, IndexerContext context, OpenSearchClient client) {
    super(config, context);

    this.client = client;
    this.index = OpenSearchUtils.getOpenSearchIndex(config);
    this.routingField = ConfigUtils.getStringOrDefault(config, "opensearch.routingField", null);
    this.update = ConfigUtils.getBooleanOrDefault(config, "opensearch.update", false);
    this.versionType =
        config.hasPath("opensearch.versionType") ? parseVersionType(config.getString("opensearch.versionType")) : null;
    this.childDocumentsField = ConfigUtils.getStringOrDefault(config, "opensearch.childDocumentsField", null);
    this.versionField = ConfigUtils.getStringOrDefault(config, "opensearch.versionField", null);
    this.splitOnRequestTooLarge = ConfigUtils.getBooleanOrDefault(config, "opensearch.splitOnRequestTooLarge", false);

    // validate config opensearch.versionType that must be set if config opensearch.versionField is set
    if (this.versionField != null && this.versionType == null) {
      throw new IllegalArgumentException("opensearch.versionType must be set if opensearch.versionField is set");
    }
  }

  public OpenSearchIndexer(Config config, IndexerContext context) throws IndexerException {
    this(config, context, null);
    // built after super() and this class's config checks, so a constructor that throws can't leak the client's
    // connection pool and threads; nothing after this line can throw
    this.client = getClient(config, isBypass());
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
  protected boolean checkConnection() {
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
    if (client != null && client._transport() != null) {
      try {
        client._transport().close();
      } catch (Exception e) {
        log.error("Error closing Opensearchclient", e);
      }
    }
  }

  /**
   * Sends the documents in the requests {@link #toSegments} splits them into, in input order.
   * <ul>
   *   <li>An exception thrown by a request is thrown on only while nothing in this call has been applied or has failed,
   *   so the base Indexer keeps every document pending. After that it is returned as a failure of each document that
   *   request was sending, so documents already indexed are not reported as failed and are not sent again.</li>
   *   <li>After a segment with a failure that {@link #isRetryable} accepts, the later segments are held back and
   *   reported as {@link IndexerFailure#notSent not sent}, so a retry resumes in input order and nothing, in particular
   *   a delete-by-query, is sent ahead of an earlier operation that is being retried. The final attempt sends every
   *   segment, since no retry follows it.</li>
   * </ul>
   */
  @Override
  protected List<IndexerFailure> sendToIndex(PendingSend send) throws IndexerException {
    List<Document> documents = send.documents();
    SendState state = new SendState();
    List<Segment> segments = toSegments(documents, state.failures);

    boolean holdBack = false;
    for (Segment segment : segments) {
      if (holdBack) {
        for (Document doc : segment.documents()) {
          state.failures.putIfAbsent(doc, IndexerFailure.notSent(doc));
        }
        continue;
      }

      if (segment instanceof WriteSegment write) {
        sendRequest(state, write.uploads.values(), () -> uploadDocuments(write.uploads));
        sendRequest(state, write.deletes.values(), () -> deleteById(write.deletes));
      } else {
        // One delete-by-query is sent per index, and each is reported on its own, so a retry after one index fails
        // doesn't send again a delete-by-query another index applied.
        Map<String, List<Document>> queryDocsByIndex = new LinkedHashMap<>();
        for (Document doc : segment.documents()) {
          queryDocsByIndex.computeIfAbsent(targetIndex(doc), k -> new ArrayList<>()).add(doc);
        }
        for (List<Document> queryDocs : queryDocsByIndex.values()) {
          sendRequest(state, queryDocs, () -> {
            deleteByQuery(deleteByQueryTerms(queryDocs));
            return List.of();
          });
        }
      }

      holdBack = !send.isFinal() && segment.documents().stream()
          .map(state.failures::get)
          .anyMatch(failure -> failure != null && isRetryable(failure.cause()));
    }

    // the failures in input order, each document once
    List<IndexerFailure> result = new ArrayList<>();
    Set<Document> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Document doc : documents) {
      IndexerFailure failure = state.failures.get(doc);
      if (failure != null && seen.add(doc)) {
        result.add(failure);
      }
    }
    return result;
  }

  // One request: its per-document failures, or an exception if the request as a whole failed.
  @FunctionalInterface
  private interface Request {
    List<IndexerFailure> send() throws IndexerException;
  }

  // What one call to sendToIndex has done so far. Identity map, so documents are never hashed.
  private static final class SendState {
    private final Map<Document, IndexerFailure> failures = new IdentityHashMap<>();
    // whether a request in this call has applied at least one document
    private boolean applied;
  }

  // Sends one request for the given documents and records its outcome. See sendToIndex for when an exception is thrown
  // on rather than returned as a failure of each document.
  private static void sendRequest(SendState state, Collection<Document> docs, Request request) throws IndexerException {
    if (docs.isEmpty()) {
      return;
    }
    List<IndexerFailure> requestFailures;
    try {
      requestFailures = request.send();
    } catch (IndexerException e) {
      if (!state.applied && state.failures.isEmpty()) {
        throw e;
      }
      for (Document doc : docs) {
        state.failures.putIfAbsent(doc, new IndexerFailure(doc, e));
      }
      return;
    }
    int failed = 0;
    for (IndexerFailure failure : requestFailures) {
      if (state.failures.putIfAbsent(failure.document(), failure) == null) {
        failed++;
      }
    }
    if (failed < docs.size()) {
      state.applied = true;
    }
  }

  /**
   * Splits the batch into the requests it is sent as, in input order. A delete-by-query must not delete documents that
   * come after it in the batch, so pending uploads and deletes by id form a segment that is sent before a run of
   * delete-by-query documents, and the queries are sent before the next upload or delete by id. Within a write segment
   * only the last operation on each (index, id) is sent. A delete-by-query document whose field name or value is null
   * can't be part of a terms query, so it is added to failures instead.
   */
  private List<Segment> toSegments(List<Document> documents, Map<Document, IndexerFailure> failures) {
    List<Segment> segments = new ArrayList<>();
    WriteSegment write = null;
    List<Document> queryDocs = null;

    for (Document doc : documents) {
      // use doc id override if specified, otherwise delete will try to delete wrong id
      String id = getDestinationId(doc);

      // if an index override field has been specified, use its value as the index to send to opensearch,
      final String indexToSend = targetIndex(doc);

      Pair<String, String> indexAndId = Pair.of(indexToSend, id);

      if (!isDeleteByQuery(doc)) {
        // a buffered run of delete-by-queries goes before this write rather than after it
        if (queryDocs != null) {
          segments.add(new QuerySegment(queryDocs));
          queryDocs = null;
        }
        if (write == null) {
          write = new WriteSegment();
        }
        if (!isMarkedForDeletion(doc)) {
          write.deletes.remove(indexAndId);
          write.uploads.put(indexAndId, doc);
        } else {
          write.uploads.remove(indexAndId);
          write.deletes.put(indexAndId, doc);
        }
      } else {
        // An invalid delete-by-query fails on its own and leaves the batch as if it weren't there: an earlier upload of
        // the same id is still sent.
        String fieldName = getDeleteByQueryFieldName(doc);
        String value = getDeleteByQueryValue(doc);
        if (fieldName == null || value == null) {
          failures.putIfAbsent(doc, new IndexerFailure(doc, new IndexerException("Document " + doc.getId()
              + " is marked for delete by query but its " + (fieldName == null ? "field name" : "value") + " is null")));
          continue;
        }
        // the writes buffered before this delete-by-query are sent first, without an upload of the same id
        if (write != null) {
          write.uploads.remove(indexAndId);
          segments.add(write);
          write = null;
        }
        if (queryDocs == null) {
          queryDocs = new ArrayList<>();
        }
        queryDocs.add(doc);
      }
    }

    // at most one of these is non-null
    if (write != null) {
      segments.add(write);
    }
    if (queryDocs != null) {
      segments.add(new QuerySegment(queryDocs));
    }
    return segments;
  }

  private sealed interface Segment permits WriteSegment, QuerySegment {
    Collection<Document> documents();
  }

  // Uploads and deletes by id sent together. They never share an (index, id), so their order between each other
  // doesn't matter.
  private static final class WriteSegment implements Segment {
    private final Map<Pair<String, String>, Document> uploads = new LinkedHashMap<>();
    // the document that marked each (index, id) for deletion, so deleteById can send its version
    private final Map<Pair<String, String>, Document> deletes = new LinkedHashMap<>();

    @Override
    public Collection<Document> documents() {
      List<Document> docs = new ArrayList<>(uploads.values());
      docs.addAll(deletes.values());
      return docs;
    }
  }

  // A run of consecutive delete-by-query documents, combined into one query per index.
  private record QuerySegment(List<Document> documents) implements Segment {
  }

  // The terms to delete for the delete-by-query documents: for each (index, field name), the values to match.
  private Map<Pair<String, String>, List<String>> deleteByQueryTerms(List<Document> queryDocs) {
    Map<Pair<String, String>, List<String>> terms = new LinkedHashMap<>();
    for (Document doc : queryDocs) {
      String indexToSend = targetIndex(doc);
      terms.computeIfAbsent(Pair.of(indexToSend, getDeleteByQueryFieldName(doc)), k -> new ArrayList<>())
          .add(getDeleteByQueryValue(doc));
    }
    return terms;
  }

  // The index a document is written to: its index override when it has one, otherwise opensearch.index.
  private String targetIndex(Document doc) {
    return Optional.ofNullable(getIndexOverride(doc)).orElse(index);
  }

  /**
   * Deletes the documents by id in one bulk request. An error on an item that lines up with the delete sent in its
   * position fails that document, with the item's status code; an error that can't be matched to a delete fails the
   * whole request.
   */
  private List<IndexerFailure> deleteById(Map<Pair<String, String>, Document> idsToDelete) throws IndexerException {
    if (idsToDelete.isEmpty()) {
      return List.of();
    }

    BulkRequest.Builder br = new BulkRequest.Builder();
    // the id of each delete, in request order, and whether it was sent with a version; the response items come back in
    // the same order
    List<String> ids = new ArrayList<>();
    List<Document> docs = new ArrayList<>();
    List<Boolean> versioned = new ArrayList<>();
    for (Map.Entry<Pair<String, String>, Document> entry : idsToDelete.entrySet()) {
      Pair<String, String> indexAndId = entry.getKey();
      Long versionNum = getDeleteVersionNum(entry.getValue());
      ids.add(indexAndId.getRight());
      docs.add(entry.getValue());
      versioned.add(versionNum != null);
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

    List<IndexerFailure> failedDocs = new ArrayList<>();
    if (response.errors()) {
      List<BulkResponseItem> items = response.items();
      for (int i = 0; i < items.size(); i++) {
        BulkResponseItem item = items.get(i);
        if (item.error() == null) {
          continue;
        }
        // Only an item that lines up with the delete sent in its position can be matched to that delete's document
        // and version.
        if (i >= ids.size() || !ids.get(i).equals(item.id())) {
          throw new IndexerException("encountered errors while deleting documents: " + item.error().reason());
        }
        // a 409 on a versioned delete means the index already holds this version or a newer one: either this delete
        // was applied by an earlier request whose response was lost (a retry resends it), or a newer document
        // superseded it
        if (item.status() == HTTP_CONFLICT && versioned.get(i)) {
          log.info("Versioned delete of id: {} already applied or superseded: {}", item.id(), item.error().reason());
          continue;
        }
        log.debug("Error while deleting id: {} because: {}", item.id(), item.error().reason());
        failedDocs.add(new IndexerFailure(docs.get(i), new IndexerRetryableException(item.status(),
            "OpenSearch bulk item error deleting id " + item.id() + " (status " + item.status() + "): "
                + item.error().reason(), null)));
      }
    }
    return failedDocs;
  }

  private void deleteByQuery(Map<Pair<String, String>, List<String>> termsToDeleteByQuery) throws IndexerException {
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
        // a delete-by-query only matches what the index has refreshed, so refresh first: otherwise it misses documents
        // uploaded just before it, in this batch or an earlier one, and can fail on a document deleted but not yet refreshed
        client.indices().refresh(r -> r.index(entry.getKey()));
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
  private List<IndexerFailure> uploadDocuments(Map<Pair<String, String>, Document> documentsToUpload) throws IndexerException {
    if (documentsToUpload.isEmpty()) {
      return List.of();
    }

    try {
      return sendUploadRequest(documentsToUpload);
    } catch (IndexerRetryableException e) {
      if (!splitOnRequestTooLarge || e.getStatusCode() != HTTP_REQUEST_TOO_LARGE) {
        throw e;
      }
      if (documentsToUpload.size() == 1) {
        Map.Entry<Pair<String, String>, Document> only = documentsToUpload.entrySet().iterator().next();
        log.error("OpenSearch rejected document {} for index {} on its own with HTTP 413: it exceeds http.max_content_length.",
            only.getKey().getRight(), only.getKey().getLeft());
        return List.of(new IndexerFailure(only.getValue(), new IndexerException(
            "Document exceeds OpenSearch http.max_content_length (HTTP 413) when sent on its own", e)));
      }

      List<Map.Entry<Pair<String, String>, Document>> entries = new ArrayList<>(documentsToUpload.entrySet());
      int mid = entries.size() / 2;
      log.warn("OpenSearch rejected a bulk request of {} documents with HTTP 413; splitting it into {} and {}.",
          entries.size(), mid, entries.size() - mid);
      List<IndexerFailure> failedDocs = new ArrayList<>(uploadSplitPart(entries.subList(0, mid)));
      failedDocs.addAll(uploadSplitPart(entries.subList(mid, entries.size())));
      return failedDocs;
    }
  }

  private List<IndexerFailure> uploadSplitPart(List<Map.Entry<Pair<String, String>, Document>> entries) {
    Map<Pair<String, String>, Document> part = new LinkedHashMap<>();
    for (Map.Entry<Pair<String, String>, Document> entry : entries) {
      part.put(entry.getKey(), entry.getValue());
    }
    try {
      return uploadDocuments(part);
    } catch (IndexerException e) {
      List<IndexerFailure> failedDocs = new ArrayList<>();
      for (Document doc : part.values()) {
        failedDocs.add(new IndexerFailure(doc, e));
      }
      return failedDocs;
    }
  }

  private List<IndexerFailure> sendUploadRequest(Map<Pair<String, String>, Document> documentsToUpload) throws IndexerException {
    List<Map.Entry<Pair<String, String>, Document>> uploads = new ArrayList<>(documentsToUpload.entrySet());
    // whether each upload, in request order, was sent with an external version; the response items come back in the
    // same order
    List<Boolean> versioned = new ArrayList<>();
    BulkRequest.Builder br = new BulkRequest.Builder();

    for (Map.Entry<Pair<String, String>, Document> entry : documentsToUpload.entrySet()) {
      String indexToSend = entry.getKey().getLeft();
      String docId = entry.getKey().getRight();
      Document doc = entry.getValue();

      JsonpSerializable indexerDoc = getUploadBody(doc, docId);

      Long versionNum = (versionType == VersionType.External || versionType == VersionType.ExternalGte)
          ? getVersionNum(doc)
          : null;
      versioned.add(versionNum != null);

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
              // doc_as_upsert: an update to an id not yet in the index creates it instead of failing with
              // document_missing_exception
              return up.document(indexerDoc).docAsUpsert(true);
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

    List<IndexerFailure> failedDocs = new ArrayList<>();

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
      // The bulk API returns one item per operation, in request order. Documents in different indexes can share an id,
      // so the position identifies the document rather than the id.
      List<BulkResponseItem> items = response.items();
      for (int i = 0; i < items.size(); i++) {
        BulkResponseItem item = items.get(i);
        if (item.error() != null) {
          // For the error - if the item lines up with a document we sent, then it failed, and we add it to the list.
          // If not, we don't know what the error is, and opt to throw an actual IndexerException instead.
          if (i < uploads.size() && uploads.get(i).getKey().getRight().equals(item.id())) {
            // a 409 on an upload sent with an external version means the index already holds that version or a newer
            // one: a newer write superseded this one, or this upload was applied by an earlier request whose response
            // was lost
            if (item.status() == HTTP_CONFLICT && versioned.get(i)) {
              log.info("Versioned upload of id: {} already applied or superseded: {}", item.id(),
                  item.error().reason());
              continue;
            }
            Document failedDoc = uploads.get(i).getValue();
            // item.status() is always an HTTP status code. The OpenSearch bulk API defines the per-item
            // status field as the HTTP status of that individual operation (e.g. 200, 201, 400, 409, 429,
            // 503). This is consistent across all OpenSearch client libraries and confirmed by the
            // OpenSearch REST API documentation and source — there are no documented non-HTTP values.
            failedDocs.add(new IndexerFailure(failedDoc, new IndexerRetryableException(item.status(),
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

  /**
   * Returns the body of a document's index or update operation: a {@link JsonDocumentBody} that writes the document's
   * own tree into the client's Jackson generator when the request is serialized, with the same JSON as
   * {@link #getMapBody}, but without building a Map of every document in the batch first.
   */
  JsonpSerializable getUploadBody(Document doc, String docId) {
    // the fields written in place of the document's own, or added, as getMapBody puts them
    Map<String, ValueWriter> replacements = new LinkedHashMap<>();
    if (getFieldFilter().shouldInclude(Document.ID_FIELD)) {
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

  /**
   * Returns the body of a document's index or update operation as a Map, as it was built before the body was streamed.
   * It is the reference for the JSON {@link #getUploadBody} writes, which a test compares byte for byte.
   */
  Map<String, Object> getMapBody(Document doc, String docId) {
    // removing fields in indexer.excludeFields or not in indexer.includeFields
    Map<String, Object> indexerDoc = getRawIndexerDoc(doc);

    // only add id if our fieldFilter allows it (based on indexer.includeFields and indexer.excludeFields)
    // - Case 1: id and idOverride filtered out -> idOverride used by Indexer, both removed from Document (tested in testBlacklistWithOverride)
    // - Case 2: id is filtered, idOverride exists -> idOverride used by Indexer, only id field removed from Document (tested in testBlacklistWithOverride2)
    // - Case 3: id is filtered, idOverride null -> id used by Indexer, id also removed from Document (tested in testRouting)
    // - Case 4: both unfiltered, idOverride exists -> idOverride used by Indexer, id and idOverride field exist in Document (tested in testOverride)
    // - Case 5: both unfiltered, idOverride null -> document id remains and used by Indexer (Default case & tested)
    if (getFieldFilter().shouldInclude(Document.ID_FIELD)) {
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
    JsonDocumentWriter.writeObject(doc, generator, getFieldFilter(), replacements);
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

  /** Only call on documents that have children. */
  private void addChildren(Document doc, Map<String, Object> indexerDoc) {
    if (!hasChildDocumentsField(doc)) {
      return;
    }

    List<Map<String, Object>> childDocMaps = new ArrayList<>();
    List<Document> children = doc.getChildren();

    for (Document child : children) {
      // calling getRawIndexerDoc applies includeFields/excludeFields and drops nested children, which we don't support
      childDocMaps.add(getRawIndexerDoc(child));
    }
    indexerDoc.put(childDocumentsField, childDocMaps);
  }

  // Accepts both the enum constant name (External, ExternalGte) and the JSON spelling (external, external_gte).
  private static VersionType parseVersionType(String name) {
    for (VersionType type : VersionType.values()) {
      if (type.name().equalsIgnoreCase(name) || type.jsonValue().equalsIgnoreCase(name)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unknown opensearch.versionType \"" + name + "\"; expected one of "
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
  // back to the Kafka offset: the offset of a delete is not comparable to a version taken from opensearch.versionField, so
  // only a document that carries opensearch.versionField gets a versioned delete.
  private Long getDeleteVersionNum(Document doc) {
    if ((versionType == VersionType.External || versionType == VersionType.ExternalGte)
        && versionField != null && doc.has(versionField)) {
      return doc.getLong(versionField);
    }
    return null;
  }
}
