package com.kmwllc.lucille.indexer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch._types.VersionType;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.Event.Type;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.KafkaDocument;
import com.kmwllc.lucille.message.IndexerMessageManager;
import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ElasticsearchIndexerTest {

  private ElasticsearchClient mockClient;

  @Before
  public void setup() throws IOException {
    setupElasticsearchClient();
  }

  private void setupElasticsearchClient() throws IOException {
    mockClient = Mockito.mock(ElasticsearchClient.class);

    // make first call to validateConnection succeed but subsequent calls to fail
    Mockito.when(mockClient.ping()).thenReturn(new BooleanResponse(true), new BooleanResponse(false));

    BulkResponse mockResponse = Mockito.mock(BulkResponse.class);
    Mockito.when(mockClient.bulk(any(BulkRequest.class))).thenReturn(mockResponse);
  }

  /**
   * Tests that the indexer correctly polls completed documents from the destination topic and sends them to
   * Elasticsearch.
   *
   * @throws Exception
   */
  @Test
  public void testElasticsearchIndexer() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, manager, mockClient, "testing");
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
  public void testElasticsearchIndexerException() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/exception.conf");

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");
    Document doc3 = Document.create("doc3", "test_run");
    Document doc4 = Document.create("doc4", "test_run");
    Document doc5 = Document.create("doc5", "test_run");

    ElasticsearchIndexer indexer = new ErroringElasticsearchIndexer(config, manager, mockClient, "testing");
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
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/config.conf");
    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, manager, mockClient, "testing");
    Assert.assertTrue(indexer.validateConnection()); // should only work the first time with the mockClient
    Assert.assertFalse(indexer.validateConnection());
    Assert.assertFalse(indexer.validateConnection());

  }

  @Test
  public void testMultipleBatches() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/batching.conf");
    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, manager, mockClient, "testing");

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

    verify(mockClient, times(3)).bulk(bulkRequestArgumentCaptor.capture());

    List<BulkRequest> bulkRequestValue = bulkRequestArgumentCaptor.getAllValues();
    assertEquals(3, bulkRequestValue.size());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation indexRequest = requests.get(0).index();

    assertEquals(doc5.getId(), indexRequest.id());

    Assert.assertEquals(5, manager.getSavedEvents().size());

    List<Event> events = manager.getSavedEvents();
    Assert.assertEquals("doc1", events.get(0).getDocumentId());
    Assert.assertEquals(Event.Type.FINISH, events.get(0).getType());
  }

  @Test
  public void testElasticsearchIndexerNestedJson() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree("{\"a\": [{\"aa\":1}, {\"aa\": 2}] }");
    doc.setField("myJsonField", jsonNode);

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, manager, mockClient, "testing");
    manager.sendCompleted(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);

    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation indexRequest = requests.get(0).index();

    Map<String, Object> map = (Map<String, Object>) indexRequest.document();

    assertEquals(doc.getId(), indexRequest.id());
    assertEquals(doc.asMap().get("myJsonField"), map.get("myJsonField"));

    Assert.assertEquals(1, manager.getSavedEvents().size());

    List<Event> events = manager.getSavedEvents();
    Assert.assertEquals("doc1", events.get(0).getDocumentId());
    Assert.assertEquals(Event.Type.FINISH, events.get(0).getType());
  }

  @Test
  public void testElasticsearchIndexerNestedJsonMultivalued() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree("{\"a\": [{\"aa\":1}, {\"aa\": 2}] }");
    doc.setField("myJsonField", jsonNode);

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, manager, mockClient, "testing");
    manager.sendCompleted(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);

    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation indexRequest = requests.get(0).index();
    Map<String, Object> map = (Map<String, Object>) indexRequest.document();

    assertEquals(doc.getId(), map.get("id"));
    assertEquals(doc.asMap().get("myJsonField"), map.get("myJsonField"));

    Assert.assertEquals(1, manager.getSavedEvents().size());

    List<Event> events = manager.getSavedEvents();
    Assert.assertEquals("doc1", events.get(0).getDocumentId());
    Assert.assertEquals(Event.Type.FINISH, events.get(0).getType());
  }

  @Test
  public void testElasticsearchIndexerNestedJsonWithObjects() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree("{\"a\": {\"aa\":1}, \"b\":{\"ab\": 2} }");
    doc.setField("myJsonField", jsonNode);

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, manager, mockClient, "testing");
    manager.sendCompleted(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);

    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation indexRequest = requests.get(0).index();
    Map<String, Object> map = (Map<String, Object>) indexRequest.document();

    assertEquals(doc.getId(), map.get("id"));
    assertEquals(doc.asMap().get("myJsonField"), map.get("myJsonField"));

    Assert.assertEquals(1, manager.getSavedEvents().size());

    List<Event> events = manager.getSavedEvents();
    Assert.assertEquals("doc1", events.get(0).getDocumentId());
    Assert.assertEquals(Event.Type.FINISH, events.get(0).getType());
  }


  @Test
  public void testRouting() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/routing.conf");

    Document doc = Document.create("doc1");
    doc.setField("routing", "routing1");
    doc.setField("field1", "value1");

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, manager, mockClient, "testing");
    manager.sendCompleted(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);
    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();

    List<BulkOperation> requests = br.operations();
    IndexOperation<Map<String, Object>> indexRequest = requests.get(0).index();

    assertEquals("doc1", indexRequest.id());
    assertEquals("routing1", indexRequest.routing());
    assertEquals(doc.asMap(), indexRequest.document());
  }

  @Test
  public void testDocumentVersioning() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/versioning.conf");

    KafkaDocument doc = new KafkaDocument(
        new ObjectMapper().createObjectNode()
            .put("id", "doc1")
            .put("field1", "value1"));
    doc.setKafkaMetadata(new ConsumerRecord<>("testing", 0, 100, null, null));

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, manager, mockClient, "testing");
    manager.sendCompleted(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);
    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();

    List<BulkOperation> requests = br.operations();
    IndexOperation<Map<String, Object>> indexRequest = requests.get(0).index();

    assertEquals("doc1", indexRequest.id());
    assertEquals(Long.valueOf(100), indexRequest.version());
    assertEquals(VersionType.ExternalGte, indexRequest.versionType());
    assertEquals(Map.of("id", "doc1", "field1", "value1"), indexRequest.document());
  }

  private void testJoin(String configPath, Document doc, Map<String, Object> expected) throws Exception {

    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load(configPath);

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, manager, mockClient, "testing");
    manager.sendCompleted(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);

    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation<Map<String, Object>> indexRequest = requests.get(0).index();

    // add doc id to expected
    expected.put("id", doc.getId());
    assertEquals(expected, indexRequest.document());
  }

  @Test
  public void testJoinParent() throws Exception {

    Document doc = Document.create("doc1");

    Map<String, Object> expected = new HashMap<>();
    expected.put("my_join_field", "parentName1");

    testJoin("ElasticsearchIndexerTest/joinParent.conf", doc, expected);
  }

  @Test
  public void testJoinChild() throws Exception {

    Document doc = Document.create("doc1");
    // define parent id
    doc.setField("parentDocumentIdSource1", "parentId1");

    Map<String, Object> joinData = new HashMap<>();
    joinData.put("name", "childName1");
    joinData.put("parent", "parentId1");

    Map<String, Object> expected = new HashMap<>();
    expected.put("parentDocumentIdSource1", "parentId1");
    expected.put("my_join_field", joinData);

    testJoin("ElasticsearchIndexerTest/joinChild.conf", doc, expected);
  }

  @Test
  public void testBulkResponseErroring() throws Exception {
    ElasticsearchClient mockClient2 = Mockito.mock(ElasticsearchClient.class);

    // make first call to validateConnection succeed but subsequent calls to fail
    Mockito.when(mockClient2.ping()).thenReturn(new BooleanResponse(true), new BooleanResponse(false));

    BulkResponse mockResponse = Mockito.mock(BulkResponse.class);
    Mockito.when(mockClient2.bulk(any(BulkRequest.class))).thenReturn(mockResponse);

    // mocking for the bulk response items and error causes
    BulkResponseItem.Builder mockItemBuilder = Mockito.mock(BulkResponseItem.Builder.class);
    BulkResponseItem mockItemError = Mockito.mock(BulkResponseItem.class);
    BulkResponseItem mockItemNoError = Mockito.mock(BulkResponseItem.class);
    ErrorCause mockError = new ErrorCause.Builder().reason("mock reason").type("mock-type").build();
    Mockito.when(mockItemBuilder.build()).thenReturn(mockItemError);
    Mockito.when(mockItemError.error()).thenReturn(mockError);

    List<BulkResponseItem> bulkResponseItems = Arrays.asList(mockItemNoError, mockItemError, mockItemNoError);
    Mockito.when(mockResponse.items()).thenReturn(bulkResponseItems);

    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");
    Document doc3 = Document.create("doc3", "test_run");

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, manager, mockClient2, "testing");
    manager.sendCompleted(doc);
    manager.sendCompleted(doc2);
    manager.sendCompleted(doc3);

    indexer.run(3);

    IndexerException exc = assertThrows(IndexerException.class, () -> indexer.sendToIndex(Arrays.asList(doc, doc2, doc3)));
    assertEquals("mock reason", exc.getMessage());

    List<Event> events = manager.getSavedEvents();
    assertEquals(3, events.size());
    for (int i = 1; i <= events.size(); i++) {
      Assert.assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      Assert.assertEquals(Type.FAIL, events.get(i - 1).getType());
    }
  }

  private static class ErroringElasticsearchIndexer extends ElasticsearchIndexer {

    public ErroringElasticsearchIndexer(Config config, IndexerMessageManager manager,
        ElasticsearchClient client, String metricsPrefix) {
      super(config, manager, client, "testing");
    }

    @Override
    public void sendToIndex(List<Document> docs) throws Exception {
      throw new Exception("Test that errors when sending to indexer are correctly handled");
    }
  }
}
