package com.kmwllc.lucille.indexer;

import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class OpenSearchProxy implements SearchProxy {

  private RestHighLevelClient client;
  private static final Logger log = LoggerFactory.getLogger(OpenSearchProxy.class);

  private String index;
  private BulkRequest bulkRequest = new BulkRequest(index);

  public OpenSearchProxy(RestHighLevelClient client, String index) {
    this.client = client;
    this.index = index;
  }

  @Override
  public boolean ping() throws Exception {
    return client.ping(RequestOptions.DEFAULT);
  }

  @Override
  public void close() throws Exception {
    client.close();
  }

  @Override
  public void addToBulkRequest(String id, Map<String, Object> map) {
    // create new IndexRequest
    IndexRequest indexRequest = new IndexRequest(index);
    indexRequest.id(id);
    indexRequest.source(map);

    // add indexRequest to bulkRequest
    bulkRequest.add(indexRequest);
  }

  @Override
  public void sendAndResetBulkRequest() throws Exception {
    client.bulk(bulkRequest, RequestOptions.DEFAULT);

    // reset bulk request
    bulkRequest = new BulkRequest(index);
  }
}