package com.kmwllc.lucille.connector;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

public class MockQueryResponse extends QueryResponse {

  private SolrDocumentList results;

  public MockQueryResponse() {
    super();
    this.results = new SolrDocumentList();
  }

  public void addToResults(SolrDocument d) {
    results.add(d);
  }

  @Override
  public SolrDocumentList getResults() {
    return results;
  }

  @Override
  public String getNextCursorMark() {
    return "**";
  }

  @Override
  public String toString() {
    return "MockQueryResponse";
  }
}
