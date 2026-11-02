package com.kmwllc.lucille.solr.indexer;

import com.kmwllc.lucille.core.Document;
import org.apache.solr.common.SolrInputDocument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Maintains the list of Solr Add and Update requests that are built up as a SolrIndexer processes a
 * batch. Allows the calling code (typically a SolrIndexer instance) to keep track of the document
 * ids within a batch and check for collisions. Maintains separate list of documents to delete
 * based on the configured delete marker on incoming documents, these are held in a list of ids for documents
 * that can be deleted directly by the ID or a map of field names to field values for documents that have to
 * be deleted with a terms query.
 */
public class SolrDocRequests {

  // a Solr document to add or update, with the Lucille Document it was built from
  private record AddUpdate(SolrInputDocument solrDoc, Document sourceDoc) {}

  private final List<AddUpdate> addUpdates = new ArrayList<>();
  private final Set<String> idsToAddOrUpdate = new HashSet<>();
  private final Set<String> idsToDelete = new HashSet<>();
  private final Map<String,List<String>> valuesToDeleteByField = new HashMap<>();

  // solrDoc must have its id set, which SolrIndexer always does
  public void addDocForAddUpdate(SolrInputDocument solrDoc, Document sourceDoc) {
    addUpdates.add(new AddUpdate(solrDoc, sourceDoc));
    idsToAddOrUpdate.add((String) solrDoc.getFieldValue(Document.ID_FIELD));
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
    return !valuesToDeleteByField.isEmpty();
  }

  public void resetAddUpdates() {
    addUpdates.clear();
    idsToAddOrUpdate.clear();
  }

  public void resetDeletes() {
    idsToDelete.clear();
    valuesToDeleteByField.clear();
  }

  public List<SolrInputDocument> getAddUpdateDocs() {
    return addUpdates.stream().map(AddUpdate::solrDoc).toList();
  }

  public List<Document> getAddUpdateSourceDocs() {
    return addUpdates.stream().map(AddUpdate::sourceDoc).toList();
  }

  public List<String> getDeleteIds() {
    return new ArrayList<>(this.idsToDelete);
  }

  public Map<String, List<String>> getValuesToDeleteByField() {
    return valuesToDeleteByField;
  }
}
