package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.message.IndexerMessageManager;
import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OpenSearchIndexerTest {

  /**
   * Tests that the indexer correctly polls completed documents from the destination topic and sends them to OpenSearch.
   *
   * @throws Exception
   */
  @Test
  public void testIndexer() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/config.conf");

    Document doc = new Document("doc1", "test_run");
    Document doc2 = new Document("doc2", "test_run");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, manager, true, "testing");
    manager.sendCompleted(doc);
    manager.sendCompleted(doc2);
    indexer.run(2);

    assertTrue(manager.hasEvents());
    assertEquals(2, manager.getSavedEvents().size());

    List<Event> events = manager.getSavedEvents();
    for (int i = 1; i <= events.size(); i++) {
      assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      assertEquals(Event.Type.FINISH, events.get(i - 1).getType());
    }
  }

//  /**
//   * Tests that Indexer.fromConfig properly instantiates OpenSearchIndexer instance
//   *
//   * @throws Exception
//   */
//  @Test
//  public void testIndexerFromConfig() throws Exception {
//    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
//    Config config = ConfigFactory.load("OpenSearchIndexerTest/config.conf");
//
//    Document doc = new Document("doc1", "test_run");
//    Document doc2 = new Document("doc2", "test_run");
//
//    Indexer indexer = IndexerFactory.fromConfig(config, manager, true, "testing");
//    assertTrue(indexer instanceof OpenSearchIndexer);
//
//    manager.sendCompleted(doc);
//    manager.sendCompleted(doc2);
//    indexer.run(2);
//
//    assertTrue(manager.hasEvents());
//    assertEquals(2, manager.getSavedEvents().size());
//
//    List<Event> events = manager.getSavedEvents();
//    for (int i = 1; i <= events.size(); i++) {
//      assertEquals("doc" + i, events.get(i - 1).getDocumentId());
//      assertEquals(Event.Type.FINISH, events.get(i - 1).getType());
//    }
//  }

  @Test
  public void testIndexerException() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/exception.conf");

    Document doc = new Document("doc1", "test_run");
    Document doc2 = new Document("doc2", "test_run");
    Document doc3 = new Document("doc3", "test_run");
    Document doc4 = new Document("doc4", "test_run");
    Document doc5 = new Document("doc5", "test_run");

    OpenSearchIndexer indexer = new ErroringOpenSearchIndexer(config, manager, true);
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

  private static class ErroringOpenSearchIndexer extends OpenSearchIndexer {

    public ErroringOpenSearchIndexer(Config config, IndexerMessageManager manager, boolean bypass) {
      super(config, manager, bypass, "testing");
    }

    @Override
    public void sendToIndex(List<Document> docs) throws Exception {
      throw new Exception("Test that errors when sending to indexer are correctly handled");
    }
  }



}
