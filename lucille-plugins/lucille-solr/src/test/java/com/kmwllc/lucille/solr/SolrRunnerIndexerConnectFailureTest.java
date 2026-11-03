package com.kmwllc.lucille.solr;

import static org.junit.Assert.assertFalse;

import com.kmwllc.lucille.core.Runner;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

/**
 * Verifies that a LOCAL run fails when the indexer cannot connect to its destination. Moved here from core's RunnerTest
 * once Solr was extracted: it needs a real indexer whose connection check can fail, which the in-core NopIndexer (always
 * reporting a successful connection) cannot provide. The config points the Solr indexer at an unreachable URL.
 */
public class SolrRunnerIndexerConnectFailureTest {

  @Test
  public void testIndexerConnectFailure() throws Exception {
    // RunType.LOCAL (not TEST) so an Indexer is actually started and the connect failure is exercised.
    assertFalse(Runner.run(ConfigFactory.load("SolrRunnerIndexerConnectFailureTest/indexerConnectFailure.conf"),
        Runner.RunType.LOCAL).getStatus());
  }
}
