package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.typesafe.config.Config;
import java.util.List;

/**
 * The NopIndexer performs no operations and does not send documents to any index. It is intended to be used for testing or
 * validating that a pipeline is properly configured without ingesting content.
 */
public class NopIndexer extends Indexer {

  public NopIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix, String localRunId) {
    super(config, messenger, metricsPrefix, localRunId, Spec.indexer());
  }

  public NopIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix) {
    this(config, messenger, bypass, metricsPrefix, null);
  }

  @Override
  public String getIndexerConfigKey() { return null; }

  @Override
  public boolean validateConnection() {
    return true;
  }

  @Override
  protected void sendToIndex(List<Document> documents) throws Exception {
    // no-op
  }

  @Override
  public void closeConnection() {
  }
}
