package com.kmwllc.lucille.elasticsearch.indexer;

import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.indexer.IndexerFactory;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

/**
 * Verifies that {@code indexer.type: "Elasticsearch"} resolves to {@link ElasticsearchIndexer} through the
 * {@link IndexerFactory} ServiceLoader mechanism now that the Elasticsearch backend lives in this plugin module rather
 * than in lucille-core. This is the plugin-side counterpart to core's IndexerFactoryTest, which no longer references
 * ElasticsearchIndexer.
 */
public class ElasticsearchIndexerFactoryTest {

  @Test
  public void testTypeResolvesToElasticsearchIndexer() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerFactoryTest/config_valid_type_elastic.conf");
    // bypass=true plus sendEnabled:false means no real client is created; we only assert the resolved type.
    Indexer indexer = IndexerFactory.fromConfig(config, messenger, true, "testing");
    assertTrue(indexer instanceof ElasticsearchIndexer);
  }
}
