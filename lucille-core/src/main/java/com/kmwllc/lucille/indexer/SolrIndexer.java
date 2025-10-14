package com.kmwllc.lucille.indexer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.util.SSLUtils;
import com.kmwllc.lucille.util.SolrUtils;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Indexes documents to Solr using SolrJ.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>url (List&lt;String&gt;, Optional) : One or more Solr base URLs (e.g., https://localhost:8983).</li>
 *   <li>useCloudClient (Boolean, Optional) : Use the SolrCloud client. Defaults to false.</li>
 *   <li>zkHosts (List&lt;String&gt;, Optional) : ZooKeeper connection strings when using SolrCloud.</li>
 *   <li>zkChroot (String, Optional) : ZooKeeper chroot used with SolrCloud.</li>
 *   <li>defaultCollection (String, Optional) : Default Solr collection to index into when no index override is present.</li>
 *   <li>userName (String, Optional) : Username for HTTP authentication.</li>
 *   <li>password (String, Optional) : Password for HTTP authentication.</li>
 *   <li>acceptInvalidCert (Boolean, Optional) : Allow invalid TLS certificates. Defaults to false.</li>
 * </ul>
 */
public class SolrIndexer extends Indexer {

  public static final Spec SPEC = SpecBuilder.indexer()
      .optionalList("url", new TypeReference<List<String>>() {})
      .optionalBoolean("useCloudClient", "acceptInvalidCert")
      .optionalString("defaultCollection", "userName", "password", "zkChroot")
      .optionalString(SSLUtils.SSL_CONFIG_OPTIONAL_PROPERTIES)
      .optionalList("zkHosts", new TypeReference<List<String>>(){}).build();

  private static final Logger log = LoggerFactory.getLogger(SolrIndexer.class);

  private final SolrClient solrClient;

  public SolrIndexer(Config config, IndexerMessenger messenger, SolrClient solrClient, String metricsPrefix, String localRunId) {
    super(config, messenger, metricsPrefix, localRunId);

    this.solrClient = solrClient;
  }

  public SolrIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix, String localRunId) {
    super(config, messenger, metricsPrefix, localRunId);

    // If the SolrIndexer is creating its own client it needs to happen after the Indexer has validated its config
    // to avoid problems where a client is created with no way to close it.
    this.solrClient = getSolrClient(config, bypass);
  }

  // Convenience Constructors
  public SolrIndexer(Config config, IndexerMessenger messenger, SolrClient solrClient, String metricsPrefix) {
    this(config, messenger, solrClient, metricsPrefix, null);
  }

  public SolrIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix) {
    this(config, messenger, bypass, metricsPrefix, null);
  }

  @Override
  protected String getIndexerConfigKey() { return "solr"; }

  private static SolrClient getSolrClient(Config config, boolean bypass) {
    return bypass ? null : SolrUtils.getSolrClient(config);
  }

  @Override
  public boolean validateConnection() {
    if (solrClient == null) {
      return true;
    }
    if (solrClient instanceof Http2SolrClient) {
      try {
        SolrPingResponse resp = solrClient.ping();
        int status = resp.getStatus();
        if (status != 0) {
          log.error("Non zero response when checking solr cluster status: " + status);
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
        log.error("Non zero response when checking solr cluster status: " + status);
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
    if (solrClient != null) {
      try {
        solrClient.close();
      } catch (Exception e) {
        log.error("Error closing SolrClient", e);
      }
    }
  }

  @Override
  protected Set<Pair<Document, String>> sendToIndex(List<Document> documents) throws Exception {
    if (solrClient == null) {
      log.debug("sendToSolr bypassed for documents: " + documents);
      return Set.of();
    }

    Map<String, SolrDocRequests> solrDocRequestsByCollection = new HashMap<>();
    Map<String, Document> docsUploaded = new LinkedHashMap<>();

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

      if (isDeletion(doc)) {

        // if the add/update requests contain the ID of this delete, send the add/updates
        // immediately so the add/update
        // of the document is processed before this delete.

        if (solrDocRequests.containsIdForAddUpdate(solrId)
            || (deleteByFieldField != null
            && doc.has(deleteByFieldField)
            && deleteByFieldValue != null
            && doc.has(deleteByFieldValue))) {
          sendAddUpdateBatch(collection, solrDocRequests.getAddUpdateDocs());
          solrDocRequests.resetAddUpdates();
        }

        if (deleteByFieldField != null
            && doc.has(deleteByFieldField)
            && deleteByFieldValue != null
            && doc.has(deleteByFieldValue)) {
          solrDocRequests.addDeleteByFieldValue(
              doc.getString(deleteByFieldField), doc.getString(deleteByFieldValue));
        } else {
          solrDocRequests.addIdForDeletion(solrId);
        }

      } else {
        docsUploaded.put(solrId, doc);
        // note that solrDoc after this code block does not guarantee that "id" field is in document
        SolrInputDocument solrDoc = toSolrDoc(doc, idOverride, collection);

        // if the delete requests contain the ID of this document, send the deletes immediately so
        // the delete is
        // processed before this document.

        if (solrDocRequests.containsIdForDeletion(solrId)
            || solrDocRequests.containsAnyDeleteByField()) {
          sendDeletionBatch(collection, solrDocRequests);
          solrDocRequests.resetDeletes();
        }
        solrDocRequests.addDocForAddUpdate(solrDoc, solrId);
      }
    }

    Set<Pair<Document, String>> failedDocs = new HashSet<>();

    for (String collection : solrDocRequestsByCollection.keySet()) {
      List<SolrInputDocument> collectionDocs = solrDocRequestsByCollection.get(collection).getAddUpdateDocs();

      try {
        sendAddUpdateBatch(collection, collectionDocs);
      } catch (Exception e) {
        // Add all the docs to failed docs
        for (SolrInputDocument d : collectionDocs) {
          String docId = d.getFieldValue(Document.ID_FIELD).toString();

          if (docsUploaded.containsKey(docId)) {
            failedDocs.add(Pair.of(docsUploaded.get(docId), e.getMessage()));
          } else {
            throw e;
          }
        }
      }

      sendDeletionBatch(collection, solrDocRequestsByCollection.get(collection));
    }

    return failedDocs;
  }

  private void sendAddUpdateBatch(String collection, List<SolrInputDocument> solrDocs)
      throws SolrServerException, IOException {
    if (solrDocs.isEmpty()) {
      return;
    }
    if (collection == null) {
      solrClient.add(solrDocs);
    } else {
      solrClient.add(collection, solrDocs);
    }
  }

  private void sendDeletionBatch(String collection, SolrDocRequests requests)
      throws SolrServerException, IOException {
    if (requests.getDeleteIds().isEmpty() && requests.getValuesToDeleteByField().isEmpty()) {
      return;
    }

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
        termsQueries.add(
            String.format("(+{!terms f='id' v='%s'})", String.join(",", requests.getDeleteIds())));
      }

      requests
          .getValuesToDeleteByField()
          .entrySet()
          .forEach(
              entry ->
                  termsQueries.add(
                      String.format(
                          "(+{!terms f='%s' v='%s'})",
                          entry.getKey(), String.join(",", entry.getValue()))));

      String queryToDelete = String.join(" OR ", termsQueries);

      if (collection == null) {
        solrClient.deleteByQuery(queryToDelete);
      } else {
        solrClient.deleteByQuery(collection, queryToDelete);
      }
    }
  }

  private boolean isDeletion(Document doc) {
    return this.deletionMarkerField != null
        && this.deletionMarkerFieldValue != null
        && doc.hasNonNull(this.deletionMarkerField)
        && doc.getString(this.deletionMarkerField).equals(this.deletionMarkerFieldValue);
  }

  private SolrInputDocument toSolrDoc(Document doc, String idOverride, String indexOverride)
      throws IndexerException {
    // removes fields from ignoredFields config, including id
    Map<String, Object> map = getIndexerDoc(doc);
    SolrInputDocument solrDoc = new SolrInputDocument();

    for (String key : map.keySet()) {

      if (Document.CHILDREN_FIELD.equals(key)) {
        continue;
      }
      
      if (idOverride != null && Document.ID_FIELD.equals(key)) {
        solrDoc.setField(Document.ID_FIELD, idOverride);
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
      // remove key:value pair mappings if they appear in ignoreFields
      Map<String, Object> map = getIndexerDoc(child);

      SolrInputDocument solrChild = new SolrInputDocument();
      for (String key : map.keySet()) {
        // we don't support children that contain nested children
        if (Document.CHILDREN_FIELD.equals(key)) {
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
