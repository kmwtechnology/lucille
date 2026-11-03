package com.kmwllc.lucille.solr.indexer;

import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.indexer.IndexerFactory;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

/**
 * Verifies that {@code indexer.type: "Solr"} resolves to {@link SolrIndexer} through {@link IndexerFactory}
 * now that the Solr backend lives in this plugin module rather than in lucille-core. This is the
 * plugin-side counterpart to core's IndexerFactoryTest, which no longer references SolrIndexer.
 */
public class SolrIndexerFactoryTest {

  @Test
  public void testTypeResolvesToSolrIndexer() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("SolrIndexerFactoryTest/config_valid_type_solr.conf");
    // bypass=true plus sendEnabled:false means no real client is created; we only assert the resolved type.
    Indexer indexer = IndexerFactory.fromConfig(config, messenger, true, "testing");
    assertTrue(indexer instanceof SolrIndexer);
  }
}
