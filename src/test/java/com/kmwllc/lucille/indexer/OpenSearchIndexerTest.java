package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.message.IndexerMessageManager;
import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.opensearch.client.base.BooleanResponse;
import org.opensearch.client.opensearch.OpenSearchClient;

import java.io.IOException;
import java.util.List;

public class OpenSearchIndexerTest {
  private OpenSearchClient mockClient;

  @Before
  public void setup() throws IOException {
    setupOpenSearchClient();
  }

  private void setupOpenSearchClient() throws IOException {
    mockClient = Mockito.mock(OpenSearchClient.class);

    // make first call to validateConnection succeed but subsequent calls to fail
    Mockito.when(mockClient.ping()).thenReturn(new BooleanResponse(true), new BooleanResponse(false));
  }

  /**
   * Tests that the indexer correctly polls completed documents from the destination topic and sends them to OpenSearch.
   *
   * @throws Exception
   */
  @Test
  public void testOpenSearchIndexer() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/config.conf");

    Document doc = new Document("doc1", "test_run");
    Document doc2 = new Document("doc2", "test_run");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, manager, mockClient, "testing");
    manager.sendCompleted(doc);
    manager.sendCompleted(doc2);
    indexer.run(2);

    Assert.assertTrue(manager.hasEvents());
    Assert.assertEquals(2, manager.getSavedEvents().size());

    List<Event> events = manager.getSavedEvents();
    for (int i = 1; i <= events.size(); i++) {
      Assert.assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      Assert.assertEquals(Event.Type.FINISH, events.get(i - 1).getType());
    }
  }

  @Test
  public void testOpenSearchIndexerException() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/exception.conf");

    Document doc = new Document("doc1", "test_run");
    Document doc2 = new Document("doc2", "test_run");
    Document doc3 = new Document("doc3", "test_run");
    Document doc4 = new Document("doc4", "test_run");
    Document doc5 = new Document("doc5", "test_run");

    OpenSearchIndexer indexer = new ErroringOpenSearchIndexer(config, manager, mockClient, "testing");
    manager.sendCompleted(doc);
    manager.sendCompleted(doc2);
    manager.sendCompleted(doc3);
    manager.sendCompleted(doc4);
    manager.sendCompleted(doc5);
    indexer.run(5);

    List<Event> events = manager.getSavedEvents();
    Assert.assertEquals(5, events.size());
    for (int i = 1; i <= events.size(); i++) {
      Assert.assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      Assert.assertEquals(Event.Type.FAIL, events.get(i - 1).getType());
    }
  }

  @Test
  public void testValidateConnection() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/config.conf");
    OpenSearchIndexer indexer = new OpenSearchIndexer(config, manager, mockClient, "testing");
    Assert.assertTrue(indexer.validateConnection()); // should only work the first time with the mockClient
    Assert.assertFalse(indexer.validateConnection());
    Assert.assertFalse(indexer.validateConnection());

  }

  private static class ErroringOpenSearchIndexer extends OpenSearchIndexer {

    public ErroringOpenSearchIndexer(Config config, IndexerMessageManager manager,
                                     OpenSearchClient client, String metricsPrefix) {
      super(config, manager, client, "testing");
    }

    @Override
    public void sendToIndex(List<Document> docs) throws Exception {
      throw new Exception("Test that errors when sending to indexer are correctly handled");
    }
  }



}
