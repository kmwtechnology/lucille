package com.kmwllc.lucille.opensearch.indexer;

import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.indexer.IndexerFactory;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

/**
 * Verifies that {@code indexer.type: "OpenSearch"} resolves to {@link OpenSearchIndexer} through {@link IndexerFactory}
 * now that the OpenSearch backend lives in this plugin module rather than in
 * lucille-core. This is the plugin-side counterpart to core's IndexerFactoryTest, which no longer references
 * OpenSearchIndexer.
 */
public class OpenSearchIndexerFactoryTest {

  @Test
  public void testTypeResolvesToOpenSearchIndexer() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerFactoryTest/config_valid_type_open.conf");
    // bypass=true plus sendEnabled:false means no real client is created; we only assert the resolved type.
    Indexer indexer = IndexerFactory.fromConfig(config, messenger, true, "testing");
    assertTrue(indexer instanceof OpenSearchIndexer);
  }
}
