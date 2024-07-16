package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.typesafe.config.Config;
import java.util.List;

public class NoOpIndexer extends Indexer {

  public NoOpIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix) {
    super(config, messenger, metricsPrefix);
  }

  @Override
  public boolean validateConnection() {
    return true;
  }

  @Override
  protected void sendToIndex(List<Document> documents) throws Exception {
  }

  @Override
  public void closeConnection() {
  }
}
