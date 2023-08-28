package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Document;
import org.apache.solr.common.SolrInputDocument;

import java.util.*;

/**
 * Maintains the list of Solr Add and Update requests that are built up as a SolrIndexer processes a
 * batch. Allows the calling code (typically a SolrIndexer instance) to keep track of the document
 * ids within a batch and check for collisions. Maintains separate list of documents to delete
 * based on the configured delete marker on incoming documents, these are held in a list of ids for documents
 * that can be deleted directly by the ID or a map of field names to field values for documents that have to
 * be deleted with a terms query.
 */
public class SolrDocRequests {

  private final List<SolrInputDocument> docsToAddOrUpdate;
  private final Set<String> idsToAddOrUpdate;
  private final Set<String> idsToDelete;
  private final Map<String,List<String>> valuesToDeleteByField;

  public SolrDocRequests() {
    this(new ArrayList<>(), new HashSet<>(), new HashSet<>(), new HashMap<>());
  }

  public SolrDocRequests(
      List<SolrInputDocument> docsToAddOrUpdate,
      Set<String> idsToAddOrUpdate,
      Set<String> idsToDelete,
      Map<String,List<String>> valuesToDeleteByField) {
    this.docsToAddOrUpdate = docsToAddOrUpdate;
    this.idsToAddOrUpdate = idsToAddOrUpdate;
    this.idsToDelete = idsToDelete;
    this.valuesToDeleteByField = valuesToDeleteByField;
  }

  public void addDocForAddUpdate(SolrInputDocument doc) {
    docsToAddOrUpdate.add(doc);
    idsToAddOrUpdate.add((String) doc.getFieldValue(Document.ID_FIELD));
  }

  public void addIdForDeletion(String id) {
    idsToDelete.add(id);
  }

  public void addDeleteByFieldValue(String deleteByField, String deleteByValue) {
    if(!valuesToDeleteByField.containsKey(deleteByField)) {
      valuesToDeleteByField.put(deleteByField, new ArrayList<>());
    }
    valuesToDeleteByField.get(deleteByField).add(deleteByValue);
  }

  public boolean containsIdForAddUpdate(String id) {
    return idsToAddOrUpdate.contains(id);
  }

  public boolean containsIdForDeletion(String id) {
    return idsToDelete.contains(id);
  }

  public boolean containsAnyDeleteByField() {
    return valuesToDeleteByField.size() > 0;
  }

  public void resetAddUpdates() {
    docsToAddOrUpdate.clear();
    idsToAddOrUpdate.clear();
  }

  public void resetDeletes() {
    idsToDelete.clear();
    valuesToDeleteByField.clear();
  }

  public List<SolrInputDocument> getAddUpdateDocs() {
    return docsToAddOrUpdate;
  }

  public List<String> getDeleteIds() {
    return new ArrayList<>(this.idsToDelete);
  }

  public Map<String, List<String>> getValuesToDeleteByField() {
    return valuesToDeleteByField;
  }
}
