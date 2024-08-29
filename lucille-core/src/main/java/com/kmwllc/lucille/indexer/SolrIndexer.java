package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.message.KafkaIndexerMessenger;
import com.kmwllc.lucille.util.SolrUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import sun.misc.Signal;

public class SolrIndexer extends Indexer {

  private static final Logger log = LoggerFactory.getLogger(SolrIndexer.class);

  private final SolrClient solrClient;

  public SolrIndexer(
      Config config, IndexerMessenger messenger, SolrClient solrClient, String metricsPrefix) {
    super(config, messenger, metricsPrefix);
    this.solrClient = solrClient;
  }

  public SolrIndexer(
      Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix) {
    super(config, messenger, metricsPrefix);
    // If the SolrIndexer is creating its own client it needs to happen after the Indexer has validated its config
    // to avoid problems where a client is created with no way to close it.
    this.solrClient = getSolrClient(config, bypass);
  }

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
  protected void sendToIndex(List<Document> documents) throws Exception {

    if (solrClient == null) {
      log.debug("sendToSolr bypassed for documents: " + documents);
      return;
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
        SolrInputDocument solrDoc = toSolrDoc(doc, idOverride, collection);

        // if the delete requests contain the ID of this document, send the deletes immediately so
        // the delete is
        // processed before this document.

        if (solrDocRequests.containsIdForDeletion(solrId)
            || solrDocRequests.containsAnyDeleteByField()) {
          sendDeletionBatch(collection, solrDocRequests);
          solrDocRequests.resetDeletes();
        }
        solrDocRequests.addDocForAddUpdate(solrDoc);
      }
    }
    for (String collection : solrDocRequestsByCollection.keySet()) {
      sendAddUpdateBatch(
          collection, solrDocRequestsByCollection.get(collection).getAddUpdateDocs());
      sendDeletionBatch(collection, solrDocRequestsByCollection.get(collection));
    }
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
      Map<String, Object> map = child.asMap();
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
