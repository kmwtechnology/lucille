package com.kmwllc.lucille.relevan.indexer;

import com.fasterxml.jackson.databind.JsonNode;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.IndexerRetryableException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.relevan.indexer.RelevanClient.Response;
import com.typesafe.config.Config;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Indexes documents into a <a href="https://relevan.dev">Relevan</a> search index through the Relevan REST API.
 *
 * <p> The target index must already exist and have a mapping deployed: Relevan rejects documents for an index without a
 * mapping, and rejects any field that the mapping does not declare. Lucille's {@code id} and {@code run_id} fields are
 * never sent as content; the document ID (or the value of {@code indexer.idOverrideField}) becomes the Relevan document ID.
 *
 * <p> Upserts are sent through the bulk endpoint, up to 1000 documents per request. Relevan rejects a whole bulk request
 * when any document in it fails validation, so this indexer fails only the rejected documents and resends the rest.
 * Documents marked for deletion with {@code indexer.deletionMarkerField} are deleted by ID. Delete-by-query
 * ({@code indexer.deleteByFieldField}) is not supported.
 *
 * <p> HTTP 429 and 5xx responses and network errors are reported as {@link IndexerRetryableException}s, so they are
 * retried when {@code indexer.maxRetries} is set.
 *
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>apiKey (String) : An org-scoped Relevan API key.</li>
 *   <li>index (String) : The name of the Relevan index to write to.</li>
 *   <li>orgId (String, Optional) : The ID or slug of the Relevan organization. When omitted, it is resolved from the
 *   API key, which always belongs to exactly one organization.</li>
 *   <li>url (String, Optional) : The base URL of the Relevan API. Defaults to {@value #DEFAULT_URL}.</li>
 *   <li>dropUnmappedFields (Boolean, Optional) : Whether to leave out fields that the index's mapping does not declare,
 *   instead of letting Relevan reject the document. Defaults to false.</li>
 *   <li>coerceTypes (Boolean, Optional) : Whether to convert string values to numbers or booleans when the index's
 *   mapping declares the field with kind {@code number} or {@code boolean}. Useful for sources such as CSV files, which
 *   produce only strings. Strings that do not parse are sent unchanged. Defaults to false.</li>
 *   <li>requestTimeoutMs (Integer, Optional) : The timeout for connecting and for each request, in milliseconds.
 *   Defaults to {@value #DEFAULT_REQUEST_TIMEOUT_MS}.</li>
 * </ul>
 */
public class RelevanIndexer extends Indexer {

  public static final Spec SPEC = SpecBuilder.indexer()
      .requiredString("apiKey", "index")
      .optionalString("orgId", "url")
      .optionalBoolean("dropUnmappedFields", "coerceTypes")
      .optionalNumber("requestTimeoutMs")
      .build();

  public static final String DEFAULT_URL = "https://api.relevan.dev";
  public static final int DEFAULT_REQUEST_TIMEOUT_MS = 30000;

  static final int MAX_BULK_DOCUMENTS = 1000;
  static final int DELETE_CONCURRENCY = 8;

  private static final int MAX_ID_LENGTH = 512;
  private static final Pattern ID_PATTERN = Pattern.compile("^[A-Za-z0-9_.:-]+$");
  private static final Set<String> TEXT_USES = Set.of("searchable", "autocompletable", "highlightable");

  private static final Logger log = LoggerFactory.getLogger(RelevanIndexer.class);

  private final RelevanClient client;
  private final String index;
  private final String configuredOrgId;
  private final boolean dropUnmappedFields;
  private final boolean coerceTypes;

  private volatile String orgId;
  // field name -> kind, for each index whose mapping has been fetched; only used for dropUnmappedFields and coerceTypes
  private final Map<String, Map<String, String>> fieldKindsByIndex = new ConcurrentHashMap<>();

  public RelevanIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix, String localRunId) {
    super(config, messenger, bypass, metricsPrefix, localRunId);

    if (deleteByFieldField != null) {
      throw new IllegalArgumentException(
          "RelevanIndexer does not support delete-by-query. Remove indexer.deleteByFieldField and indexer.deleteByFieldValue.");
    }

    Config relevanConfig = config.getConfig("relevan");
    this.index = relevanConfig.getString("index");
    this.configuredOrgId = ConfigUtils.getOrDefault(relevanConfig, "orgId", null);
    this.dropUnmappedFields = ConfigUtils.getOrDefault(relevanConfig, "dropUnmappedFields", false);
    this.coerceTypes = ConfigUtils.getOrDefault(relevanConfig, "coerceTypes", false);
    int timeoutMs = ConfigUtils.getOrDefault(relevanConfig, "requestTimeoutMs", DEFAULT_REQUEST_TIMEOUT_MS);
    String url = ConfigUtils.getOrDefault(relevanConfig, "url", DEFAULT_URL);

    this.client = bypass ? null : new RelevanClient(url, relevanConfig.getString("apiKey"), Duration.ofMillis(timeoutMs));
  }

  public RelevanIndexer(Config config, IndexerMessenger messenger, String metricsPrefix, String localRunId) {
    this(config, messenger, false, metricsPrefix, localRunId);
  }

  @Override
  protected String getIndexerConfigKey() {
    return "relevan";
  }

  @Override
  public boolean validateConnection() {
    if (bypass) {
      return true;
    }
    try {
      String indexPath = indexPath(index);
      Response indexResponse = client.get(indexPath);
      if (indexResponse.status() == 404) {
        log.error("Relevan index '{}' does not exist in organization '{}'. Create it before indexing.", index, orgId());
        return false;
      }
      if (!indexResponse.isSuccess()) {
        log.error("Could not read Relevan index '{}': {}", index, describe(indexResponse));
        return false;
      }
      Response mappingsResponse = client.get(indexPath + "/mappings");
      if (!mappingsResponse.isSuccess()) {
        log.error("Could not read the mappings of Relevan index '{}': {}", index, describe(mappingsResponse));
        return false;
      }
      if (latestMapping(mappingsResponse.body()) == null) {
        log.error("Relevan index '{}' has no mapping. Deploy a mapping before indexing; Relevan rejects documents "
            + "for an index without one.", index);
        return false;
      }
      return true;
    } catch (Exception e) {
      log.error("Could not connect to Relevan.", e);
      return false;
    }
  }

  @Override
  protected Set<Pair<Document, Exception>> sendToIndex(List<Document> documents) throws Exception {
    Map<String, List<Document>> documentsByIndex = new LinkedHashMap<>();
    for (Document doc : documents) {
      String override = getIndexOverride(doc);
      documentsByIndex.computeIfAbsent(override != null ? override : index, k -> new ArrayList<>()).add(doc);
    }

    Set<Pair<Document, Exception>> failures = new LinkedHashSet<>();
    for (Map.Entry<String, List<Document>> entry : documentsByIndex.entrySet()) {
      sendToRelevanIndex(entry.getKey(), entry.getValue(), failures);
    }
    return failures;
  }

  /**
   * Sends the documents for one Relevan index. Operations on different IDs are independent, so upserts are collected
   * into bulk requests and deletes are sent together. A pending operation is flushed before another operation on the
   * same ID is queued, so operations on any one ID are applied in the order the documents arrived. This also keeps
   * duplicate IDs, which Relevan rejects, out of a single bulk request.
   */
  private void sendToRelevanIndex(String indexName, List<Document> documents, Set<Pair<Document, Exception>> failures)
      throws IndexerException {
    Map<String, String> fieldKinds = null;
    if (dropUnmappedFields || coerceTypes) {
      try {
        fieldKinds = fieldKinds(indexName);
      } catch (IndexerException e) {
        documents.forEach(doc -> failures.add(Pair.of(doc, e)));
        return;
      }
    }

    String indexPath = indexPath(indexName);
    Map<String, PendingUpsert> pendingUpserts = new LinkedHashMap<>();
    Map<String, Document> pendingDeletes = new LinkedHashMap<>();

    for (Document doc : documents) {
      String idOverride = getDocIdOverride(doc);
      String id = idOverride != null ? idOverride : doc.getId();
      String idProblem = idProblem(id);
      if (idProblem != null) {
        failures.add(Pair.of(doc, new IndexerException("Document ID '" + id + "' is not a valid Relevan document ID: "
            + idProblem + ". Use indexer.idOverrideField to send a different ID.")));
        continue;
      }

      if (isDeletion(doc)) {
        if (pendingUpserts.containsKey(id)) {
          flushUpserts(indexPath, pendingUpserts, failures);
        }
        if (pendingDeletes.containsKey(id)) {
          flushDeletes(indexPath, pendingDeletes, failures);
        }
        pendingDeletes.put(id, doc);
      } else {
        if (pendingDeletes.containsKey(id)) {
          flushDeletes(indexPath, pendingDeletes, failures);
        }
        if (pendingUpserts.containsKey(id)) {
          flushUpserts(indexPath, pendingUpserts, failures);
        }
        pendingUpserts.put(id, new PendingUpsert(doc, id, content(doc, fieldKinds)));
        if (pendingUpserts.size() >= MAX_BULK_DOCUMENTS) {
          flushUpserts(indexPath, pendingUpserts, failures);
        }
      }
    }

    flushUpserts(indexPath, pendingUpserts, failures);
    flushDeletes(indexPath, pendingDeletes, failures);
  }

  private record PendingUpsert(Document doc, String id, Map<String, Object> content) {}

  /**
   * Sends the pending upserts in one bulk request. When Relevan rejects documents that fail validation, nothing
   * is written, so those documents are failed and the rest are sent again. Each round removes at least one document.
   */
  private void flushUpserts(String indexPath, Map<String, PendingUpsert> pending, Set<Pair<Document, Exception>> failures) {
    List<PendingUpsert> remaining = new ArrayList<>(pending.values());
    pending.clear();

    while (!remaining.isEmpty()) {
      List<Map<String, Object>> items = new ArrayList<>(remaining.size());
      for (PendingUpsert upsert : remaining) {
        items.add(Map.of("id", upsert.id(), "content", upsert.content()));
      }

      Response response;
      try {
        response = client.post(indexPath + "/documents/_bulk", Map.of("documents", items));
      } catch (IOException e) {
        Exception failure = new IndexerRetryableException("Error sending documents to Relevan: " + reason(e), e);
        remaining.forEach(upsert -> failures.add(Pair.of(upsert.doc(), failure)));
        return;
      }

      if (response.isSuccess()) {
        return;
      }

      Map<Integer, List<String>> rejections = validationErrors(response, remaining.size());
      if (rejections.isEmpty()) {
        Exception failure = failureFor(response, "bulk ingest");
        remaining.forEach(upsert -> failures.add(Pair.of(upsert.doc(), failure)));
        return;
      }

      List<PendingUpsert> accepted = new ArrayList<>();
      for (int i = 0; i < remaining.size(); i++) {
        List<String> messages = rejections.get(i);
        if (messages == null) {
          accepted.add(remaining.get(i));
        } else {
          failures.add(Pair.of(remaining.get(i).doc(),
              new IndexerException("Relevan rejected document '" + remaining.get(i).id() + "': " + String.join("; ", messages))));
        }
      }
      log.warn("Relevan rejected {} of {} documents as not valid; resending the other {}.",
          rejections.size(), remaining.size(), accepted.size());
      remaining = accepted;
    }
  }

  /**
   * Returns the documents that a 400 bulk response rejected, as a map of their position in the request to the error
   * messages for them. Returns an empty map if the response does not attribute its errors to documents.
   */
  private static Map<Integer, List<String>> validationErrors(Response response, int documentCount) {
    Map<Integer, List<String>> rejections = new HashMap<>();
    if (response.status() != 400 || response.body() == null || !response.body().path("errors").isArray()) {
      return rejections;
    }
    for (JsonNode error : response.body().get("errors")) {
      JsonNode position = error.get("documentIndex");
      if (position == null || !position.canConvertToInt() || position.asInt() < 0 || position.asInt() >= documentCount) {
        // an error we can't attribute to one document; fail the whole request instead of guessing
        return Map.of();
      }
      rejections.computeIfAbsent(position.asInt(), k -> new ArrayList<>()).add(error.path("message").asText());
    }
    return rejections;
  }

  /** Deletes the pending documents by ID. A document that is already gone counts as deleted. */
  private void flushDeletes(String indexPath, Map<String, Document> pending, Set<Pair<Document, Exception>> failures) {
    List<Map.Entry<String, Document>> deletes = new ArrayList<>(pending.entrySet());
    pending.clear();

    for (int start = 0; start < deletes.size(); start += DELETE_CONCURRENCY) {
      List<Map.Entry<String, Document>> wave = deletes.subList(start, Math.min(start + DELETE_CONCURRENCY, deletes.size()));
      List<CompletableFuture<Response>> responses = new ArrayList<>(wave.size());
      for (Map.Entry<String, Document> delete : wave) {
        responses.add(client.deleteAsync(indexPath + "/documents/" + RelevanClient.segment(delete.getKey())));
      }
      for (int i = 0; i < wave.size(); i++) {
        Document doc = wave.get(i).getValue();
        try {
          Response response = responses.get(i).join();
          if (!response.isSuccess() && response.status() != 404) {
            failures.add(Pair.of(doc, failureFor(response, "delete of document '" + wave.get(i).getKey() + "'")));
          }
        } catch (CompletionException e) {
          Throwable cause = e.getCause() != null ? e.getCause() : e;
          failures.add(Pair.of(doc, new IndexerRetryableException("Error deleting document from Relevan: " + reason(cause), cause)));
        }
      }
    }
  }

  /** Builds the content Relevan stores for a document. {@code fieldKinds} is null unless the mapping is needed. */
  private Map<String, Object> content(Document doc, Map<String, String> fieldKinds) {
    Map<String, Object> content = getConvertedIndexerDoc(doc);
    content.remove(Document.ID_FIELD);
    content.remove(Document.RUNID_FIELD);
    if (dropUnmappedFields) {
      content.keySet().retainAll(fieldKinds.keySet());
    }
    if (coerceTypes) {
      for (Map.Entry<String, Object> field : content.entrySet()) {
        String kind = fieldKinds.get(field.getKey());
        if (kind != null) {
          field.setValue(coerce(field.getValue(), kind));
        }
      }
    }
    return content;
  }

  /** Converts a string, or each string in a list, to the given mapping kind when it parses; otherwise returns it unchanged. */
  static Object coerce(Object value, String kind) {
    if (value instanceof List<?> list) {
      List<Object> coerced = new ArrayList<>(list.size());
      for (Object element : list) {
        coerced.add(coerce(element, kind));
      }
      return coerced;
    }
    if (!(value instanceof String string)) {
      return value;
    }
    String trimmed = string.trim();
    switch (kind) {
      case "number":
        try {
          return Long.parseLong(trimmed);
        } catch (NumberFormatException e) {
          // not an integer; try a decimal below
        }
        try {
          double parsed = Double.parseDouble(trimmed);
          return Double.isFinite(parsed) ? parsed : value;
        } catch (NumberFormatException e) {
          return value;
        }
      case "boolean":
        if (trimmed.equalsIgnoreCase("true")) {
          return Boolean.TRUE;
        }
        if (trimmed.equalsIgnoreCase("false")) {
          return Boolean.FALSE;
        }
        return value;
      default:
        return value;
    }
  }

  /** Returns why the ID can't be used as a Relevan document ID, or null if it can. */
  private static String idProblem(String id) {
    if (id == null || id.isEmpty()) {
      return "it is empty";
    }
    if (id.length() > MAX_ID_LENGTH) {
      return "it is longer than " + MAX_ID_LENGTH + " characters";
    }
    if (!ID_PATTERN.matcher(id).matches()) {
      return "it may only contain letters, digits, '_', '.', ':' and '-'";
    }
    return null;
  }

  private boolean isDeletion(Document doc) {
    return deletionMarkerField != null
        && doc.hasNonNull(deletionMarkerField)
        && doc.getString(deletionMarkerField).equals(deletionMarkerFieldValue);
  }

  /** Returns the kind of each field in the index's latest mapping, fetching it the first time the index is used. */
  private Map<String, String> fieldKinds(String indexName) throws IndexerException {
    Map<String, String> cached = fieldKindsByIndex.get(indexName);
    if (cached != null) {
      return cached;
    }

    Response response;
    try {
      response = client.get(indexPath(indexName) + "/mappings");
    } catch (IOException e) {
      throw new IndexerRetryableException("Error reading the mapping of Relevan index '" + indexName + "': " + reason(e), e);
    }
    if (!response.isSuccess()) {
      throw failureFor(response, "reading the mapping of index '" + indexName + "'");
    }
    JsonNode mapping = latestMapping(response.body());
    if (mapping == null) {
      throw new IndexerException("Relevan index '" + indexName + "' has no mapping. Deploy a mapping before indexing.");
    }

    Map<String, String> kinds = new HashMap<>();
    for (Map.Entry<String, JsonNode> field : mapping.path("fields").properties()) {
      kinds.put(field.getKey(), kindOf(field.getValue()));
    }
    fieldKindsByIndex.put(indexName, kinds);
    return kinds;
  }

  /** Returns a field's kind, inferring it as Relevan does when the mapping leaves it out. */
  private static String kindOf(JsonNode fieldMapping) {
    if (fieldMapping.hasNonNull("kind")) {
      return fieldMapping.get("kind").asText();
    }
    for (JsonNode use : fieldMapping.path("use")) {
      if (TEXT_USES.contains(use.asText())) {
        return "text";
      }
    }
    return "id";
  }

  /** Returns the {@code relevanMapping} of the highest revision in a list of mapping revisions, or null if there is none. */
  private static JsonNode latestMapping(JsonNode revisions) {
    JsonNode latest = null;
    if (revisions != null && revisions.isArray()) {
      for (JsonNode revision : revisions) {
        if (latest == null || revision.path("revision").asLong() > latest.path("revision").asLong()) {
          latest = revision;
        }
      }
    }
    return latest == null ? null : latest.get("relevanMapping");
  }

  private String indexPath(String indexName) throws IndexerException {
    return "/v1/" + RelevanClient.segment(orgId()) + "/indexes/" + RelevanClient.segment(indexName);
  }

  /** Returns the configured organization, or resolves it from the API key on first use. */
  private String orgId() throws IndexerException {
    if (orgId != null) {
      return orgId;
    }
    if (configuredOrgId != null) {
      orgId = configuredOrgId;
      return orgId;
    }

    Response response;
    try {
      response = client.get("/v1/organizations");
    } catch (IOException e) {
      throw new IndexerRetryableException("Error resolving the Relevan organization: " + reason(e), e);
    }
    if (!response.isSuccess()) {
      throw failureFor(response, "resolving the organization for the API key");
    }
    JsonNode organizations = response.body();
    if (organizations == null || !organizations.isArray() || organizations.size() != 1) {
      throw new IndexerException("Expected the Relevan API key to belong to exactly one organization, but got: "
          + response.rawBody() + ". Set relevan.orgId.");
    }
    orgId = organizations.get(0).path("id").asText();
    log.info("Resolved Relevan organization '{}' from the API key.", organizations.get(0).path("slug").asText(orgId));
    return orgId;
  }

  /** Returns a retryable exception for a 429 or 5xx response, and a plain IndexerException for any other failure. */
  private static IndexerException failureFor(Response response, String action) {
    String message = "Relevan " + action + " failed: " + describe(response);
    int status = response.status();
    if (status == 429 || status >= 500) {
      return new IndexerRetryableException(status, message, null);
    }
    return new IndexerException(message);
  }

  /** Describes an error response by its status and the most specific message in its body. */
  private static String describe(Response response) {
    StringBuilder description = new StringBuilder("HTTP ").append(response.status());
    JsonNode body = response.body();
    if (body == null) {
      if (response.rawBody() != null && !response.rawBody().isBlank()) {
        description.append(": ").append(truncate(response.rawBody()));
      }
      return description.toString();
    }
    if (body.hasNonNull("code")) {
      description.append(" (").append(body.get("code").asText()).append(")");
    }
    if (body.hasNonNull("message")) {
      description.append(": ").append(body.get("message").asText());
    }
    JsonNode error = body.get("error");
    if (error != null && !error.isNull()) {
      description.append(body.hasNonNull("message") ? " - " : ": ")
          .append(truncate(error.isTextual() ? error.asText() : error.toString()));
    }
    return description.toString();
  }

  /** Returns an exception's message, or its class name when it has none, as for a refused connection. */
  private static String reason(Throwable e) {
    return e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
  }

  private static String truncate(String text) {
    return text.length() <= 500 ? text : text.substring(0, 500) + "...";
  }

  @Override
  public void closeConnection() {
    if (client != null) {
      client.close();
    }
  }
}
