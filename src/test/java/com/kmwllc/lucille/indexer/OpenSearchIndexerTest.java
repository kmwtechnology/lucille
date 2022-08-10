package com.kmwllc.lucille.indexer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.message.IndexerMessageManager;
import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class OpenSearchIndexerTest {
  private RestHighLevelClient mockClient;

  @Before
  public void setup() throws IOException {
    setupOpenSearchClient();
  }

  private void setupOpenSearchClient() throws IOException {
    mockClient = Mockito.mock(RestHighLevelClient.class);

    // make first call to validateConnection succeed but subsequent calls to fail
    Mockito.when(mockClient.ping(RequestOptions.DEFAULT)).thenReturn(true, false);

    BulkResponse mockResponse = Mockito.mock(BulkResponse.class);
    Mockito.when(mockClient.bulk(any(), any())).thenReturn(mockResponse);
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

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, manager, mockClient, "testing");
    manager.sendCompleted(doc);
    manager.sendCompleted(doc2);
    indexer.run(2);

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

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");
    Document doc3 = Document.create("doc3", "test_run");
    Document doc4 = Document.create("doc4", "test_run");
    Document doc5 = Document.create("doc5", "test_run");

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

  @Test
  public void testMultipleBatches() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/batching.conf");
    OpenSearchIndexer indexer = new OpenSearchIndexer(config, manager, mockClient, "testing");

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");
    Document doc3 = Document.create("doc3", "test_run");
    Document doc4 = Document.create("doc4", "test_run");
    Document doc5 = Document.create("doc5", "test_run");

    manager.sendCompleted(doc);
    manager.sendCompleted(doc2);
    manager.sendCompleted(doc3);
    manager.sendCompleted(doc4);
    manager.sendCompleted(doc5);
    indexer.run(5);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);
    ArgumentCaptor<RequestOptions> requestOptionsArgumentCaptor = ArgumentCaptor.forClass(RequestOptions.class);

    verify(mockClient, times(3)).bulk(bulkRequestArgumentCaptor.capture(), requestOptionsArgumentCaptor.capture());

    List<BulkRequest> bulkRequestValue = bulkRequestArgumentCaptor.getAllValues();
    assertEquals(3, bulkRequestValue.size());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<DocWriteRequest<?>> requests = br.requests();
    IndexRequest indexRequest = (IndexRequest) requests.get(0);
    Map<String, Object> map = indexRequest.sourceAsMap();

    assertEquals(doc5.getId(), map.get("id"));

    Assert.assertEquals(5, manager.getSavedEvents().size());

    List<Event> events = manager.getSavedEvents();
    Assert.assertEquals("doc1", events.get(0).getDocumentId());
    Assert.assertEquals(Event.Type.FINISH, events.get(0).getType());
  }

  @Test
  public void testOpenSearchIndexerNestedJson() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree("{\"a\": [{\"aa\":1}, {\"aa\": 2}] }");
    doc.setField("myJsonField", jsonNode);


    OpenSearchIndexer indexer = new OpenSearchIndexer(config, manager, mockClient, "testing");
    manager.sendCompleted(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);
    ArgumentCaptor<RequestOptions> requestOptionsArgumentCaptor = ArgumentCaptor.forClass(RequestOptions.class);

    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture(), requestOptionsArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<DocWriteRequest<?>> requests = br.requests();
    IndexRequest indexRequest = (IndexRequest) requests.get(0);
    Map<String, Object> map = indexRequest.sourceAsMap();

    assertEquals(doc.getId(), map.get("id"));
    assertEquals(doc.asMap().get("myJsonField"), map.get("myJsonField"));

    Assert.assertEquals(1, manager.getSavedEvents().size());

    List<Event> events = manager.getSavedEvents();
    Assert.assertEquals("doc1", events.get(0).getDocumentId());
    Assert.assertEquals(Event.Type.FINISH, events.get(0).getType());
  }

  @Test
  public void testOpenSearchIndexerNestedJsonMultivalued() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree("{\"a\": [{\"aa\":1}, {\"aa\": 2}] }");
    doc.setField("myJsonField", jsonNode);

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, manager, mockClient, "testing");
    manager.sendCompleted(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);
    ArgumentCaptor<RequestOptions> requestOptionsArgumentCaptor = ArgumentCaptor.forClass(RequestOptions.class);

    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture(), requestOptionsArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<DocWriteRequest<?>> requests = br.requests();
    IndexRequest indexRequest = (IndexRequest) requests.get(0);
    Map<String, Object> map = indexRequest.sourceAsMap();

    assertEquals(doc.getId(), map.get("id"));
    assertEquals(doc.asMap().get("myJsonField"), map.get("myJsonField"));

    Assert.assertEquals(1, manager.getSavedEvents().size());

    List<Event> events = manager.getSavedEvents();
    Assert.assertEquals("doc1", events.get(0).getDocumentId());
    Assert.assertEquals(Event.Type.FINISH, events.get(0).getType());
  }

  @Test
  public void testOpenSearchIndexerNestedJsonWithObjects() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree("{\"a\": {\"aa\":1}, \"b\":{\"ab\": 2} }");
    doc.setField("myJsonField", jsonNode);

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, manager, mockClient, "testing");
    manager.sendCompleted(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);
    ArgumentCaptor<RequestOptions> requestOptionsArgumentCaptor = ArgumentCaptor.forClass(RequestOptions.class);

    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture(), requestOptionsArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<DocWriteRequest<?>> requests = br.requests();
    IndexRequest indexRequest = (IndexRequest) requests.get(0);
    Map<String, Object> map = indexRequest.sourceAsMap();

    assertEquals(doc.getId(), map.get("id"));
    assertEquals(doc.asMap().get("myJsonField"), map.get("myJsonField"));

    Assert.assertEquals(1, manager.getSavedEvents().size());

    List<Event> events = manager.getSavedEvents();
    Assert.assertEquals("doc1", events.get(0).getDocumentId());
    Assert.assertEquals(Event.Type.FINISH, events.get(0).getType());
  }

  private static class ErroringOpenSearchIndexer extends OpenSearchIndexer {

    public ErroringOpenSearchIndexer(Config config, IndexerMessageManager manager,
                                     RestHighLevelClient client, String metricsPrefix) {
      super(config, manager, client, "testing");
    }

    @Override
    public void sendToIndex(List<Document> docs) throws Exception {
      throw new Exception("Test that errors when sending to indexer are correctly handled");
    }
  }
}
