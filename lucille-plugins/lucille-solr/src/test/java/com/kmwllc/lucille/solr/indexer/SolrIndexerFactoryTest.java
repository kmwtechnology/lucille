package com.kmwllc.lucille.solr.indexer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
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

  private static final String TRUST_STORE = "javax.net.ssl.trustStore";

  @Test
  public void testTypeResolvesToSolrIndexer() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("SolrIndexerFactoryTest/config_valid_type_solr.conf");
    // bypass=true plus sendEnabled:false means no real client is created; we only assert the resolved type.
    Indexer indexer = IndexerFactory.fromConfig(config, messenger, true, "testing");
    assertTrue(indexer instanceof SolrIndexer);
  }

  @Test
  public void testSslSettingsInSolrBlockAreApplied() throws Exception {
    String original = System.getProperty(TRUST_STORE);
    System.clearProperty(TRUST_STORE);
    try {
      Config config = ConfigFactory.parseString(
          "indexer { solr { url: \"http://localhost:8983/solr\", " + TRUST_STORE + ": \"/path/to/trustStore\" } }");
      // bypass=false, so the client is created and the setting is copied to the system property
      Indexer indexer = IndexerFactory.fromConfig(config, new TestMessenger(), false, "testing");
      indexer.closeConnection();
      assertEquals("/path/to/trustStore", System.getProperty(TRUST_STORE));
    } finally {
      restoreTrustStore(original);
    }
  }

  @Test
  public void testRootSslSettingsAreNotRead() throws Exception {
    String original = System.getProperty(TRUST_STORE);
    System.clearProperty(TRUST_STORE);
    try {
      Config config = ConfigFactory.parseString(
          TRUST_STORE + ": \"/path/to/trustStore\"\nindexer { solr { url: \"http://localhost:8983/solr\" } }");
      Indexer indexer = IndexerFactory.fromConfig(config, new TestMessenger(), false, "testing");
      indexer.closeConnection();
      assertNull(System.getProperty(TRUST_STORE));
    } finally {
      restoreTrustStore(original);
    }
  }

  private static void restoreTrustStore(String value) {
    if (value == null) {
      System.clearProperty(TRUST_STORE);
    } else {
      System.setProperty(TRUST_STORE, value);
    }
  }
}
