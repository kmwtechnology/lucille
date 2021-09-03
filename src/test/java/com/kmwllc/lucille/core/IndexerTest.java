package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;
import static org.junit.Assert.*;

public class IndexerTest {

  /**
   * Tests that the indexer correctly polls completed documents from the destination topic and sends them to Solr.
   *
   * @throws Exception
   */
  @Test
  public void testIndexer() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("IndexerTest/config.conf");

    Document doc = new Document("doc", "test_run");
    Document doc2 = new Document("doc2", "test_run");

    Indexer indexer = new Indexer(config, manager);
    manager.sendCompleted(doc);
    manager.sendCompleted(doc2);
    indexer.run(2);
    assertEquals(2, manager.getSavedDocsSentToSolr().size());
    assertEquals(doc, manager.getSavedDocsSentToSolr().get(0));
    assertEquals(doc2, manager.getSavedDocsSentToSolr().get(1));
    assertTrue(manager.hasEvents());
  }
}
