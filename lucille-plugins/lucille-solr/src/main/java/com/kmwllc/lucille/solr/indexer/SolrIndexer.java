package com.kmwllc.lucille.solr.indexer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.auto.service.AutoService;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.IndexerRetryableException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.indexer.IndexerType;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.solr.util.SolrUtils;
import com.kmwllc.lucille.util.SSLUtils;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.HttpJdkSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Indexes documents to Solr using SolrJ.
 * Additional parameters are made available by the {@link com.kmwllc.lucille.core.Indexer} abstract class.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>url (String or List&lt;String&gt;, Optional) : One or more Solr base URLs (e.g., https://localhost:8983).</li>
 *   <li>useCloudClient (Boolean, Optional) : Use the SolrCloud client. Defaults to false.</li>
 *   <li>zkHosts (List&lt;String&gt;, Optional) : ZooKeeper connection strings when using SolrCloud.</li>
 *   <li>zkChroot (String, Optional) : ZooKeeper chroot used with SolrCloud.</li>
 *   <li>defaultCollection (String, Optional) : Default Solr collection to index into when no index override is present.</li>
 *   <li>userName (String, Optional) : Username for HTTP authentication.</li>
 *   <li>password (String, Optional) : Password for HTTP authentication.</li>
 *   <li>acceptInvalidCert (Boolean, Optional) : Allow invalid TLS certificates. Defaults to false.</li>
 * </ul>
 */
@AutoService(Indexer.class)
@IndexerType("Solr")
public class SolrIndexer extends Indexer {

  public static final Spec SPEC = SpecBuilder.indexer()
      .optionalStringOrList("url")
      .optionalBoolean("useCloudClient", "acceptInvalidCert")
      .optionalString("defaultCollection", "userName", "password", "zkChroot")
      .optionalString(SSLUtils.SSL_CONFIG_OPTIONAL_PROPERTIES)
      .optionalList("zkHosts", new TypeReference<List<String>>(){}).build();

  private static final Logger log = LoggerFactory.getLogger(SolrIndexer.class);

  // separators tried in turn for a terms query; "," is the terms parser's default
  private static final List<String> TERMS_SEPARATORS = List.of(",", "|", ";", "^", "~", "\t");

  private final SolrClient solrClient;

  public SolrIndexer(Config config, IndexerMessenger messenger, boolean bypass,
      String metricsPrefix, String localRunId, SolrClient solrClient) {
    super(config, messenger, bypass, metricsPrefix, localRunId);

    this.solrClient = solrClient;
  }

  public SolrIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix, String localRunId) {
    super(config, messenger, bypass, metricsPrefix, localRunId);

    // If the SolrIndexer is creating its own client it needs to happen after the Indexer has validated its config
    // to avoid problems where a client is created with no way to close it.
    this.solrClient = getSolrClient(config, bypass);
  }

  // Convenience Constructors
  public SolrIndexer(Config config, IndexerMessenger messenger, String metricsPrefix, SolrClient solrClient) {
    this(config, messenger, false, metricsPrefix, null, solrClient);
  }

  @Override
  protected String getIndexerConfigKey() { return "solr"; }

  // SolrJ's Http2SolrClient and CloudHttp2SolrClient are thread-safe and intended to be shared, so concurrent sends
  // through the single shared client are safe.
  @Override
  protected boolean supportsConcurrentSends() {
    return true;
  }

  private static SolrClient getSolrClient(Config config, boolean bypass) {
    return bypass ? null : SolrUtils.getSolrClient(config);
  }

  @Override
  public boolean validateConnection() {
    if (bypass) {
      return true;
    }
    if (solrClient instanceof HttpJdkSolrClient) {
      try {
        SolrPingResponse resp = solrClient.ping();
        int status = resp.getStatus();
        if (status != 0) {
          log.error("Non zero response when checking solr cluster status: {}", status);
          return false;
        }
        log.debug("SolrIndexer connection successfully validated: {}", resp);
        return true;
      } catch (Exception e) {
        log.error("Couldn't ping solr cluster.", e);
        return false;
      }
    } else if (solrClient instanceof CloudHttp2SolrClient) {
      // If we are indexing to multiple collections with the CloudSolrClient and the default
      // collection is not set then
      // we can't use ping. Instead, verify that we can connect to the cluster.
      NamedList response;
      try {
        log.debug("Validating SolrIndexer connection by checking cluster status.");
        response = solrClient.request(new CollectionAdminRequest.ClusterStatus());
      } catch (Exception e) {
        log.error("Couldn't check solr cluster status.", e);
        return false;
      }
      if (response == null) {
        log.error("Null response when checking solr cluster status.");
        return false;
      }
      Integer status = (Integer) ((SimpleOrderedMap) response.get("responseHeader")).get("status");
      if (status != 0) {
        log.error("Non zero response when checking solr cluster status: {}", status);
        return false;
      }
      log.debug("SolrIndexer connection successfully validated: {}", response);
      return true;
    } else {
      throw new UnsupportedOperationException("Client type is not supported");
    }
  }

  @Override
  public void closeConnection() {
    if (!bypass && solrClient != null) {
      try {
        solrClient.close();
      } catch (Exception e) {
        log.error("Error closing SolrClient", e);
      }
    }
  }

  @Override
  protected Set<Pair<Document, Exception>> sendToIndex(List<Document> documents) throws Exception {
    if (bypass) {
      log.debug("sendToSolr bypassed for documents: {}", documents);
      return Set.of();
    }

    Map<String, SolrDocRequests> solrDocRequestsByCollection = new HashMap<>();

    for (Document doc : documents) {

      // if an id override field has been specified, use its value as the id to send to solr,
      // instead
      // of the document's own id
      String idOverride = getDocIdOverride(doc);

      // if an index override field has been specified, use its value as the solr collection to send
      // the document to,
      // instead of the default collection. Remove the field from the solr document.
      String collection = getIndexOverride(doc);

      if (!solrDocRequestsByCollection.containsKey(collection)) {
        solrDocRequestsByCollection.put(collection, new SolrDocRequests());
      }
      SolrDocRequests solrDocRequests = solrDocRequestsByCollection.get(collection);
      String solrId = idOverride != null ? idOverride : doc.getId();

      if (isMarkedForDeletion(doc)) {

        // if the add/update requests contain the ID of this delete, send the add/updates
        // immediately so the add/update
        // of the document is processed before this delete.

        if (solrDocRequests.containsIdForAddUpdate(solrId) || hasDeleteByFieldValues(doc)) {
          sendAddUpdateBatch(collection, solrDocRequests.getAddUpdateDocs());
          solrDocRequests.resetAddUpdates();
        }

        if (hasDeleteByFieldValues(doc)) {
          solrDocRequests.addDeleteByFieldValue(
              doc.getString(deleteByFieldField), doc.getString(deleteByFieldValue));
        } else {
          solrDocRequests.addIdForDeletion(solrId);
        }

      } else {
        SolrInputDocument solrDoc = toSolrDoc(doc, solrId, collection);

        // if the delete requests contain the ID of this document, send the deletes immediately so
        // the delete is
        // processed before this document.

        if (solrDocRequests.containsIdForDeletion(solrId)
            || solrDocRequests.containsAnyDeleteByField()) {
          sendDeletionBatch(collection, solrDocRequests);
          solrDocRequests.resetDeletes();
        }
        solrDocRequests.addDocForAddUpdate(solrDoc, doc, solrId);
      }
    }

    Set<Pair<Document, Exception>> failedDocs = new HashSet<>();

    for (String collection : solrDocRequestsByCollection.keySet()) {
      SolrDocRequests requests = solrDocRequestsByCollection.get(collection);

      try {
        sendAddUpdateBatch(collection, requests.getAddUpdateDocs());
      } catch (Exception e) {
        // Add all the docs to failed docs
        for (Document d : requests.getAddUpdateSourceDocs()) {
          failedDocs.add(Pair.of(d, e));
        }
      }

      sendDeletionBatch(collection, requests);
    }

    return failedDocs;
  }

  private void sendAddUpdateBatch(String collection, List<SolrInputDocument> solrDocs)
      throws IndexerException {
    if (solrDocs.isEmpty()) {
      return;
    }
    try {
      if (collection == null) {
        solrClient.add(solrDocs);
      } else {
        solrClient.add(collection, solrDocs);
      }
    } catch (SolrException e) {
      // Decide error message based on http status code from SolrException
      throw toRetryableException(e);
    } catch (SolrServerException | IOException e) {
      // Communication / transport failure with no http status code available
      throw new IndexerRetryableException("Error communicating with Solr", e);
    }
  }

  private void sendDeletionBatch(String collection, SolrDocRequests requests)
      throws IndexerException {
    if (requests.getDeleteIds().isEmpty() && requests.getValuesToDeleteByField().isEmpty()) {
      return;
    }

    try {
      if (requests.getValuesToDeleteByField().isEmpty()) {
        // All of the deletes are by ID. Simply delete by ID.
        List<String> deletionIds = requests.getDeleteIds();
        if (collection == null) {
          solrClient.deleteById(deletionIds);
        } else {
          solrClient.deleteById(collection, deletionIds);
        }
      } else {
        // At least some of the deletes are by field. Perform the deletes with a single request using
        // terms queries.
        List<String> termsQueries = new ArrayList<>();
        if (!requests.getDeleteIds().isEmpty()) {
          termsQueries.add(toTermsQuery(Document.ID_FIELD, requests.getDeleteIds()));
        }

        requests
            .getValuesToDeleteByField()
            .forEach((field, values) -> termsQueries.add(toTermsQuery(field, values)));

        String queryToDelete = String.join(" OR ", termsQueries);

        if (collection == null) {
          solrClient.deleteByQuery(queryToDelete);
        } else {
          solrClient.deleteByQuery(collection, queryToDelete);
        }
      }
    } catch (SolrException e) {
      throw toRetryableException(e);
    } catch (SolrServerException | IOException e) {
      throw new IndexerRetryableException("Error communicating with Solr", e);
    }
  }

  /**
   * Builds a query clause matching documents whose field has any of the given values exactly. The values are joined
   * with a separator that none of them contains, so a value such as "Smith, John" stays whole, and are quoted so a
   * quote or brace in a value cannot end the local params early.
   */
  static String toTermsQuery(String field, List<String> values) {
    String separator = TERMS_SEPARATORS.stream()
        .filter(sep -> values.stream().noneMatch(v -> v.contains(sep)))
        .findFirst()
        .orElse(null);

    if (separator == null) {
      // every candidate separator appears in some value: match each value with its own term query instead
      return values.stream()
          .map(v -> String.format("(+{!term f=%s v=%s})", quoteLocalParam(field), quoteLocalParam(v)))
          .collect(Collectors.joining(" OR "));
    }

    return String.format("(+{!terms f=%s separator=%s v=%s})",
        quoteLocalParam(field), quoteLocalParam(separator), quoteLocalParam(String.join(separator, values)));
  }

  // escapes the characters that would otherwise end a double-quoted local param value, or the local params themselves
  private static String quoteLocalParam(String value) {
    StringBuilder sb = new StringBuilder("\"");
    for (char c : value.toCharArray()) {
      if (c == '\\' || c == '"' || c == '{' || c == '}') {
        sb.append('\\');
      }
      sb.append(c);
    }
    return sb.append('"').toString();
  }

  private static IndexerRetryableException toRetryableException(SolrException e) {
    int code = e.code();
    if (code > 0) {
      return new IndexerRetryableException(code, "Solr returned HTTP " + code, e);
    }
    // SolrException can carry an ErrorCode.UNKNOWN, which is a 0 and not a real http response
    return new IndexerRetryableException("Solr error with no HTTP status code", e);
  }

  private SolrInputDocument toSolrDoc(Document doc, String solrId, String indexOverride)
      throws IndexerException {
    // removes fields specified by fieldFilter config, including id
    Map<String, Object> map = getConvertedIndexerDoc(doc);
    SolrInputDocument solrDoc = new SolrInputDocument();
    // Solr needs the id (or its override) on every document, so it is set even when the field filter removes it
    solrDoc.setField(Document.ID_FIELD, solrId);

    for (String key : map.keySet()) {
      if (Document.ID_FIELD.equals(key)) {
        continue;
      }

      if (indexOverrideField != null && indexOverride != null && indexOverrideField.equals(key)) {
        // do not add the indexOverrideField to the solr document
        continue;
      }

      Object value = map.get(key);
      if (value instanceof Map) {
        throw new IndexerException(
            String.format(
                "Object field '%s' on document id=%s is not supported by the " + "SolrIndexer.",
                key, doc.getId()));
      }
      solrDoc.setField(key, value);
    }

    addChildren(doc, solrDoc);
    return solrDoc;
  }

  private void addChildren(Document doc, SolrInputDocument solrDoc) throws IndexerException {
    List<Document> children = doc.getChildren();
    if (children == null || children.isEmpty()) {
      return;
    }
    for (Document child : children) {
      // remove key:value pair mappings if they appear in blacklist
      Map<String, Object> map = getConvertedIndexerDoc(child);

      SolrInputDocument solrChild = new SolrInputDocument();
      // as for the parent, the id is set even when the field filter removes it
      solrChild.setField(Document.ID_FIELD, child.getId());
      // getConvertedIndexerDoc omits CHILDREN_FIELD, so children nested inside a child are not sent
      for (String key : map.keySet()) {
        if (Document.ID_FIELD.equals(key)) {
          continue;
        }
        Object value = map.get(key);
        if (value instanceof Map) {
          throw new IndexerException(
              String.format(
                  "Object field '%s' on child document id=%s of document id=%s is "
                      + "not supported by the SolrIndexer.",
                  key, child.getId(), doc.getId()));
        }
        solrChild.setField(key, value);
      }
      solrDoc.addChildDocument(solrChild);
    }
  }
}
