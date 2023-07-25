package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Document;
import org.apache.solr.common.SolrInputDocument;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Maintains the list of Solr Add and Update requests that are built up as a SolrIndexer processes a batch. Allows the calling code
 * (typically a SolrIndexer instance) to keep track of the document ids within a batch and check for collisions.
 */
public class SolrDocRequests {

  private final List<SolrInputDocument> docsToAddOrUpdate;
  private final Set<String> idsToAddOrUpdate;
  private final Set<String> idsToDelete;

  public SolrDocRequests() {
    this(new ArrayList<>(), new HashSet<>(), new HashSet<>());
  }

  public SolrDocRequests(
      List<SolrInputDocument> docsToAddOrUpdate,
      Set<String> idsToAddOrUpdate,
      Set<String> idsToDelete) {
    this.docsToAddOrUpdate = docsToAddOrUpdate;
    this.idsToAddOrUpdate = idsToAddOrUpdate;
    this.idsToDelete = idsToDelete;
  }

  public void addDocForAddUpdate(SolrInputDocument doc) {
    docsToAddOrUpdate.add(doc);
    idsToAddOrUpdate.add((String) doc.getFieldValue(Document.ID_FIELD));
  }

  public void addIdForDeletion(String id) {
    idsToDelete.add(id);
  }

  public boolean containsIdForAddUpdate(String id) {
    return idsToAddOrUpdate.contains(id);
  }

  public boolean containsIdForDeletion(String id) {
    return idsToDelete.contains(id);
  }

  public void resetAddUpdates() {
    docsToAddOrUpdate.clear();
    idsToAddOrUpdate.clear();
  }

  public void resetDeletes() {
    idsToDelete.clear();
  }

  public List<SolrInputDocument> getAddUpdateDocs() {
    return docsToAddOrUpdate;
  }

  public List<String> getDeleteIds() {
    return new ArrayList<>(this.idsToDelete);
  }
}
