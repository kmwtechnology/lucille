package com.kmwllc.lucille.indexer;

import java.util.Map;

public interface SearchProxy {

  boolean ping() throws Exception;

  void close() throws Exception;

  void addToBulkRequest(String id, Map<String,Object> map);

  void sendAndResetBulkRequest() throws Exception;
}