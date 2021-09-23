package com.kmwllc.lucille.core;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.kmwllc.lucille.message.IndexerMessageManager;
import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import java.util.List;

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

    Document doc = new Document("doc1", "test_run");
    Document doc2 = new Document("doc2", "test_run");

    Indexer indexer = new Indexer(config, manager, true);
    manager.sendCompleted(doc);
    manager.sendCompleted(doc2);
    indexer.run(2);
    //assertEquals(2, manager.getSavedDocsSentToSolr().size());
    //assertEquals(doc, manager.getSavedDocsSentToSolr().get(0));
    //assertEquals(doc2, manager.getSavedDocsSentToSolr().get(1));
    assertTrue(manager.hasEvents());
    assertEquals(2, manager.getSavedEvents().size());

    List<Event> events = manager.getSavedEvents();
    for (int i = 1; i <= events.size(); i++) {
      assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      assertEquals(Event.Type.FINISH, events.get(i - 1).getType());
    }
  }

  @Test
  public void testSolrException() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("IndexerTest/exception.conf");

    Document doc = new Document("doc1", "test_run");
    Document doc2 = new Document("doc2", "test_run");
    Document doc3 = new Document("doc3", "test_run");
    Document doc4 = new Document("doc4", "test_run");
    Document doc5 = new Document("doc5", "test_run");

    Indexer indexer = new ErroringIndexer(config, manager, true);
    manager.sendCompleted(doc);
    manager.sendCompleted(doc2);
    manager.sendCompleted(doc3);
    manager.sendCompleted(doc4);
    manager.sendCompleted(doc5);
    indexer.run(5);

    List<Event> events = manager.getSavedEvents();
    assertEquals(5, events.size());
    for (int i = 1; i <= events.size(); i++) {
      assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      assertEquals(Event.Type.FAIL, events.get(i - 1).getType());
    }
  }

  private static class ErroringIndexer extends Indexer {

    public ErroringIndexer(Config config, IndexerMessageManager manager, boolean bypass) {
      super(config, manager, bypass);
    }

    @Override
    public void sendToSolr(List<Document> docs) throws Exception {
      throw new Exception("Test that errors when sending to Solr are correctly handled");
    }
  }



}
