package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Document;
import org.apache.solr.common.SolrInputDocument;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SolrDocRequests {
  private final List<SolrInputDocument> solrAddUpdateDocs;
  private final Set<String> solrAddUpdateIds;
  private final Set<String> solrDeleteIds;

  public SolrDocRequests() {
    this(new ArrayList<>(), new HashSet<>(), new HashSet<>());
  }

  public SolrDocRequests(
      List<SolrInputDocument> solrAddUpdateDocs,
      Set<String> solrAddUpdateIds,
      Set<String> solrDeleteIds) {
    this.solrAddUpdateDocs = solrAddUpdateDocs;
    this.solrAddUpdateIds = solrAddUpdateIds;
    this.solrDeleteIds = solrDeleteIds;
  }

  public void addDocForAddUpdate(SolrInputDocument doc) {
    this.solrAddUpdateDocs.add(doc);
    this.solrAddUpdateIds.add((String) doc.getFieldValue(Document.ID_FIELD));
  }

  public void addIdForDeletion(String id) {
    this.solrDeleteIds.add(id);
  }

  public boolean containsIdForAddUpdate(String id) {
    return this.solrAddUpdateIds.contains(id);
  }

  public boolean containsIdForDeletion(String id) {
    return this.solrDeleteIds.contains(id);
  }

  public void resetAddUpdates() {
    this.solrAddUpdateDocs.clear();
    this.solrAddUpdateIds.clear();
  }

  public void resetDeletes() {
    this.solrDeleteIds.clear();
  }

  public List<SolrInputDocument> getAddUpdateDocs() {
    return this.solrAddUpdateDocs;
  }

  public List<String> getDeleteIds() {
    return new ArrayList<>(this.solrDeleteIds);
  }
}
