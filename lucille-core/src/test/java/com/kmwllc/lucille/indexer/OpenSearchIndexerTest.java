package com.kmwllc.lucille.indexer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.Event.Type;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.KafkaDocument;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorCause;
import org.opensearch.client.opensearch._types.VersionType;
import org.opensearch.client.opensearch._types.query_dsl.BoolQuery;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch._types.query_dsl.TermsQuery;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.DeleteByQueryRequest;
import org.opensearch.client.opensearch.core.DeleteByQueryResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.client.transport.endpoints.BooleanResponse;

public class OpenSearchIndexerTest {

  private OpenSearchClient mockClient;

  @Before
  public void setup() throws IOException {
    setupOpenSearchClient();
  }

  private void setupOpenSearchClient() throws IOException {
    mockClient = Mockito.mock(OpenSearchClient.class);

    BooleanResponse mockBooleanResponse = Mockito.mock(BooleanResponse.class);
    Mockito.when(mockClient.ping()).thenReturn(mockBooleanResponse);

    // make first call to validateConnection succeed but subsequent calls to fail
    Mockito.when(mockBooleanResponse.value()).thenReturn(true, false);

    BulkRequest.Builder mockRequestBuilder = Mockito.mock(BulkRequest.Builder.class);
    BulkResponse mockResponse = Mockito.mock(BulkResponse.class);
    BulkRequest mockBulkRequest = Mockito.mock(BulkRequest.class);
    Mockito.when(mockRequestBuilder.build()).thenReturn(mockBulkRequest);
    Mockito.when(mockClient.bulk(any(BulkRequest.class))).thenReturn(mockResponse);

    DeleteByQueryResponse mockDeleteByQueryResponse = Mockito.mock(DeleteByQueryResponse.class);
    when(mockClient.deleteByQuery(any(DeleteByQueryRequest.class))).thenReturn(mockDeleteByQueryResponse);
    when(mockDeleteByQueryResponse.failures()).thenReturn(new ArrayList<>());
  }

  /**
   * Tests that the indexer correctly polls completed documents from the destination topic and sends them to OpenSearch.
   *
   * @throws Exception
   */
  @Test
  public void testOpenSearchIndexer() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, mockClient, "testing");
    messenger.sendForIndexing(doc);
    messenger.sendForIndexing(doc2);
    indexer.run(2);

    Assert.assertEquals(2, messenger.getSentEvents().size());

    List<Event> events = messenger.getSentEvents();
    for (int i = 1; i <= events.size(); i++) {
      Assert.assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      Assert.assertEquals(Event.Type.FINISH, events.get(i - 1).getType());
    }
  }

  @Test
  public void testOpenSearchIndexerException() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/exception.conf");

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");
    Document doc3 = Document.create("doc3", "test_run");
    Document doc4 = Document.create("doc4", "test_run");
    Document doc5 = Document.create("doc5", "test_run");

    OpenSearchIndexer indexer = new ErroringOpenSearchIndexer(config, messenger, mockClient, "testing");
    messenger.sendForIndexing(doc);
    messenger.sendForIndexing(doc2);
    messenger.sendForIndexing(doc3);
    messenger.sendForIndexing(doc4);
    messenger.sendForIndexing(doc5);
    indexer.run(5);

    List<Event> events = messenger.getSentEvents();
    Assert.assertEquals(5, events.size());
    for (int i = 1; i <= events.size(); i++) {
      Assert.assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      Assert.assertEquals(Event.Type.FAIL, events.get(i - 1).getType());
    }
  }

  @Test
  public void testValidateConnection() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/config.conf");
    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, mockClient, "testing");
    Assert.assertTrue(indexer.validateConnection()); // should only work the first time with the mockClient
    Assert.assertFalse(indexer.validateConnection());
    Assert.assertFalse(indexer.validateConnection());

  }

  @Test
  public void testMultipleBatches() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/batching.conf");
    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, mockClient, "testing");

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");
    Document doc3 = Document.create("doc3", "test_run");
    Document doc4 = Document.create("doc4", "test_run");
    Document doc5 = Document.create("doc5", "test_run");

    messenger.sendForIndexing(doc);
    messenger.sendForIndexing(doc2);
    messenger.sendForIndexing(doc3);
    messenger.sendForIndexing(doc4);
    messenger.sendForIndexing(doc5);
    indexer.run(5);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(
        BulkRequest.class);

    verify(mockClient, times(3)).bulk(bulkRequestArgumentCaptor.capture());

    List<BulkRequest> bulkRequestValue = bulkRequestArgumentCaptor.getAllValues();
    assertEquals(3, bulkRequestValue.size());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation indexRequest = requests.get(0).index();

    assertEquals(doc5.getId(), indexRequest.id());

    Assert.assertEquals(5, messenger.getSentEvents().size());

    List<Event> events = messenger.getSentEvents();
    Assert.assertEquals("doc1", events.get(0).getDocumentId());
    Assert.assertEquals(Event.Type.FINISH, events.get(0).getType());
  }

  @Test
  public void testDeletion() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/batching.conf");
    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, mockClient, "testing");

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");

    Document doc3MarkedDeleted = Document.create("doc3", "test_run");
    doc3MarkedDeleted.setField("deleted", "true");
    Document doc4 = Document.create("doc4", "test_run");

    Document doc5 = Document.create("doc5", "test_run");
    Document doc6MarkedDeleted = Document.create("doc6", "test_run");
    doc6MarkedDeleted.setField("deleted", "true");

    Document doc7 = Document.create("doc7", "test_run");
    Document doc8DelByQuery = Document.create("doc8", "test_run");
    doc8DelByQuery.setField("deleted", "true");
    doc8DelByQuery.setField("field", "field");
    doc8DelByQuery.setField("value", "value");

    Document doc9MarkedDeleted = Document.create("doc9", "test_run");
    doc9MarkedDeleted.setField("deleted", "true");
    Document doc10DelByQuery = Document.create("doc10", "test_run");
    doc10DelByQuery.setField("deleted", "true");
    doc10DelByQuery.setField("field", "field");
    doc10DelByQuery.setField("value", "value");

    // batch 1
    messenger.sendForIndexing(doc);
    messenger.sendForIndexing(doc2);
    // batch 2
    messenger.sendForIndexing(doc3MarkedDeleted);
    messenger.sendForIndexing(doc4);
    // batch 3
    messenger.sendForIndexing(doc5);
    messenger.sendForIndexing(doc6MarkedDeleted);
    // batch 4
    messenger.sendForIndexing(doc7);
    messenger.sendForIndexing(doc8DelByQuery);
    // batch 5
    messenger.sendForIndexing(doc9MarkedDeleted);
    messenger.sendForIndexing(doc10DelByQuery);

    indexer.run(10);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(
        BulkRequest.class);
    ArgumentCaptor<DeleteByQueryRequest> deleteByQueryRequestArgumentCaptor = ArgumentCaptor.forClass(DeleteByQueryRequest.class);

    // batch 1 -> one bulk req to upload both
    // batch 2 -> one bulk req to delete, one bulk req to upload
    // batch 3 -> one bulk req to upload, one bulk req to delete
    // batch 4 -> one bulk req to upload, one delByQuery req
    // batch 5 -> one bulk req to delete, one delByQuery req
    verify(mockClient, times(7)).bulk(bulkRequestArgumentCaptor.capture());
    verify(mockClient, times(2)).deleteByQuery(deleteByQueryRequestArgumentCaptor.capture());

    // checking bulk request types and id
    List<BulkRequest> bulkRequestValue = bulkRequestArgumentCaptor.getAllValues();
    assertEquals("doc1", bulkRequestValue.get(0).operations().get(0).index().id());
    assertEquals("doc2", bulkRequestValue.get(0).operations().get(1).index().id());
    assertEquals("doc4", bulkRequestValue.get(1).operations().get(0).index().id());
    assertEquals("doc3", bulkRequestValue.get(2).operations().get(0).delete().id());
    assertEquals("doc5", bulkRequestValue.get(3).operations().get(0).index().id());
    assertEquals("doc6", bulkRequestValue.get(4).operations().get(0).delete().id());
    assertEquals("doc7", bulkRequestValue.get(5).operations().get(0).index().id());
    assertEquals("doc9", bulkRequestValue.get(6).operations().get(0).delete().id());

    // checking delByQuery has the remainder of the documents 8 and 10
    List<DeleteByQueryRequest> deleteByQueryRequestValue = deleteByQueryRequestArgumentCaptor.getAllValues();
    assertEquals(2, deleteByQueryRequestValue.size());
  }

  @Test
  public void testOpenSearchIndexerNestedJson() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree("{\"a\": [{\"aa\":1}, {\"aa\": 2}] }");
    doc.setField("myJsonField", jsonNode);

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, mockClient, "testing");
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);

    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation indexRequest = requests.get(0).index();

    Map<String, Object> map = (Map<String, Object>) indexRequest.document();

    assertEquals(doc.getId(), indexRequest.id());
    assertEquals(doc.asMap().get("myJsonField"), map.get("myJsonField"));

    Assert.assertEquals(1, messenger.getSentEvents().size());

    List<Event> events = messenger.getSentEvents();
    Assert.assertEquals("doc1", events.get(0).getDocumentId());
    Assert.assertEquals(Event.Type.FINISH, events.get(0).getType());
  }

  @Test
  public void testOpenSearchIndexerNestedJsonMultivalued() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree("{\"a\": [{\"aa\":1}, {\"aa\": 2}] }");
    doc.setField("myJsonField", jsonNode);

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, mockClient, "testing");
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);

    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation indexRequest = requests.get(0).index();
    Map<String, Object> map = (Map<String, Object>) indexRequest.document();

    assertEquals(doc.getId(), map.get("id"));
    assertEquals(doc.asMap().get("myJsonField"), map.get("myJsonField"));

    Assert.assertEquals(1, messenger.getSentEvents().size());

    List<Event> events = messenger.getSentEvents();
    Assert.assertEquals("doc1", events.get(0).getDocumentId());
    Assert.assertEquals(Event.Type.FINISH, events.get(0).getType());
  }

  @Test
  public void testOpenSearchIndexerNestedJsonWithObjects() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree("{\"a\": {\"aa\":1}, \"b\":{\"ab\": 2} }");
    doc.setField("myJsonField", jsonNode);

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, mockClient, "testing");
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);

    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation indexRequest = requests.get(0).index();
    Map<String, Object> map = (Map<String, Object>) indexRequest.document();

    assertEquals(doc.getId(), map.get("id"));
    assertEquals(doc.asMap().get("myJsonField"), map.get("myJsonField"));

    Assert.assertEquals(1, messenger.getSentEvents().size());

    List<Event> events = messenger.getSentEvents();
    Assert.assertEquals("doc1", events.get(0).getDocumentId());
    Assert.assertEquals(Event.Type.FINISH, events.get(0).getType());
  }

  @Test
  public void testRouting() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/routing.conf");

    Document doc = Document.create("doc1");
    doc.setField("routing", "routing1");
    doc.setField("field1", "value1");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, mockClient, "testing");
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);

    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation indexRequest = requests.get(0).index();
    Map<String, Object> map = (Map<String, Object>) indexRequest.document();

    assertEquals("doc1", indexRequest.id());

    // routing has been set appropriately even though routing field has been deleted by ignoreFields
    // note that id has also been deleted from the document, but the id is still passed to the OpenSearch Index
    // to id documents. Can be found as the field "_id"
    assertEquals("routing1", indexRequest.routing());
    assertEquals(Map.of("field1", "value1"), map);
  }

  @Test
  public void testDocumentVersioning() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/versioning.conf");

    KafkaDocument doc = new KafkaDocument(
        new ObjectMapper().createObjectNode()
            .put("id", "doc1")
            .put("field1", "value1"));
    doc.setKafkaMetadata(new ConsumerRecord<>("testing", 0, 100, null, null));

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, mockClient, "testing");
    messenger.sendForIndexing(doc);
    indexer.run(1);
    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);

    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation indexRequest = requests.get(0).index();
    Map<String, Object> map = (Map<String, Object>) indexRequest.document();

    Long expectedVersion = Long.valueOf(100);
    assertEquals("doc1", indexRequest.id());
    assertEquals(expectedVersion, indexRequest.version());
    assertEquals(VersionType.ExternalGte, indexRequest.versionType());
    assertEquals(Map.of("id", "doc1", "field1", "value1"), map);
  }

  @Test
  public void testBulkResponseErroring() throws Exception {
    OpenSearchClient mockClient2 = Mockito.mock(OpenSearchClient.class);

    BooleanResponse mockBooleanResponse = Mockito.mock(BooleanResponse.class);
    Mockito.when(mockClient2.ping()).thenReturn(mockBooleanResponse);

    // make first call to validateConnection succeed but subsequent calls to fail
    Mockito.when(mockBooleanResponse.value()).thenReturn(true, false);

    BulkRequest.Builder mockRequestBuilder = Mockito.mock(BulkRequest.Builder.class);
    BulkResponse mockResponse = Mockito.mock(BulkResponse.class);
    BulkRequest mockBulkRequest = Mockito.mock(BulkRequest.class);
    Mockito.when(mockRequestBuilder.build()).thenReturn(mockBulkRequest);
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

    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");
    Document doc3 = Document.create("doc3", "test_run");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, mockClient2, "testing");
    messenger.sendForIndexing(doc);
    messenger.sendForIndexing(doc2);
    messenger.sendForIndexing(doc3);

    indexer.run(3);

    IndexerException exc = assertThrows(IndexerException.class, () -> indexer.sendToIndex(Arrays.asList(doc, doc2, doc3)));
    assertEquals("mock reason", exc.getMessage());

    List<Event> events = messenger.getSentEvents();
    assertEquals(3, events.size());
    for (int i = 1; i <= events.size(); i++) {
      Assert.assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      Assert.assertEquals(Type.FAIL, events.get(i - 1).getType());
    }
  }

  /**
   * Tests that the indexer correctly ignores fields stated in the ignoreFields portion of the config file
   * Note that indexer would even ignore the "id" field if configured, removing the id field in the Lucille document.
   * However, the id would still be passed to the OpenSearch index to id the documents.
   * @throws Exception
   */
  @Test
  public void testIgnoreFields() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/ignoreFields.conf");

    Document doc = Document.create("doc1");

    doc.setField("ignoreField1", "value1");
    doc.setField("ignoreField2", "value2");
    doc.setField("normalField", "normalValue");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, mockClient, "testing");
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);

    // verify that the bulk method has been called once by mockClient
    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation indexRequest = requests.get(0).index();
    Map<String, Object> map = (Map<String, Object>) indexRequest.document();

    // check that ignoreField1, ignoreField2 and id has been removed
    assertEquals(Map.of("normalField", "normalValue"), map);
  }

  /**
   * Tests that the indexer correctly ignores fields stated in the ignoreFields portion of the config file
   * In this case both id & idOverride is removed from the Lucille Document, but the idOverride is still
   * used as the Document id for the Indexer
   *
   * @throws Exception
   */
  @Test
  public void testIgnoreFieldsWithOverride() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/ignoreFieldsWithOverride.conf");

    Document doc = Document.create("doc1");

    doc.setField("normalField", "normalValue");
    doc.setField("other_id", "otherId");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, mockClient, "testing");
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);

    // verify that the bulk method has been called once by mockClient
    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation indexRequest = requests.get(0).index();
    Map<String, Object> map = (Map<String, Object>) indexRequest.document();

    // check that id and other_id has been removed
    assertEquals(Map.of("normalField", "normalValue"), map);
  }

  /**
   * Tests that the indexer correctly ignores fields stated in the ignoreFields portion of the config file
   * Even if idOverride exists, id is still removed and Indexer will use the idOverride as document id
   * @throws Exception
   */
  @Test
  public void testIgnoreFieldsWithOverride2() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/ignoreFieldsWithOverride2.conf");

    Document doc = Document.create("doc1");

    doc.setField("normalField", "normalValue");
    doc.setField("other_id", "otherId");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, mockClient, "testing");
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);

    // verify that the bulk method has been called once by mockClient
    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation indexRequest = requests.get(0).index();
    Map<String, Object> map = (Map<String, Object>) indexRequest.document();

    // check that id has been removed and that other_id field remains
    assertEquals(Map.of("other_id", "otherId", "normalField", "normalValue"), map);
  }


  /**
   * Tests that the indexer correctly overrides the id if it exists in conf
   * @throws Exception
   */
  @Test
  public void testOverride() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/testOverride.conf");

    Document doc = Document.create("doc1");

    doc.setField("other_id", "otherId");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, mockClient, "testing");
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);

    // verify that the bulk method has been called once by mockClient
    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation indexRequest = requests.get(0).index();
    Map<String, Object> map = (Map<String, Object>) indexRequest.document();

    // check that id has been overwritten and other_id remains
    assertEquals(Map.of("id", "otherId", "other_id", "otherId"), map);
  }


  private static class ErroringOpenSearchIndexer extends OpenSearchIndexer {

    public ErroringOpenSearchIndexer(Config config, IndexerMessenger messenger,
        OpenSearchClient client, String metricsPrefix) {
      super(config, messenger, client, "testing");
    }

    @Override
    public void sendToIndex(List<Document> docs) throws Exception {
      throw new Exception("Test that errors when sending to indexer are correctly handled");
    }
  }
}
