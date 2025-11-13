package com.kmwllc.lucille.indexer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
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
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorCause;
import org.opensearch.client.opensearch._types.FieldValue;
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
import org.opensearch.client.opensearch.core.bulk.UpdateOperation;
import org.opensearch.client.transport.OpenSearchTransport;
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

    assertEquals(2, messenger.getSentEvents().size());

    List<Event> events = messenger.getSentEvents();
    for (int i = 1; i <= events.size(); i++) {
      assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      assertEquals(Event.Type.FINISH, events.get(i - 1).getType());
    }
  }

  // Unsupported, but shouldn't cause any errors.
  @Test
  public void testIndexerWithChildDocs() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    doc.addChild(Document.create("childDoc1", "test_run"));

    Document doc2 = Document.create("doc2", "test_run");
    Document childDoc2 = Document.create("childDoc2", "test_run");
    doc2.addChild(childDoc2);
    childDoc2.addChild(Document.create("child_childDoc2", "test_run"));

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, mockClient, "testing");
    messenger.sendForIndexing(doc);
    messenger.sendForIndexing(doc2);
    indexer.run(2);

    Assert.assertEquals(2, messenger.getSentEvents().size());
  }

  @Test
  public void testOpensearchIndexerUpdate() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/updateConfig.conf");

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, mockClient, "testing");
    messenger.sendForIndexing(doc);
    messenger.sendForIndexing(doc2);
    indexer.run(2);

    assertEquals(2, messenger.getSentEvents().size());

    List<Event> events = messenger.getSentEvents();
    for (int i = 1; i <= events.size(); i++) {
      assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      assertEquals(Event.Type.FINISH, events.get(i - 1).getType());
    }
  }

  // When the method throws an exception, all documents fail
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
    assertEquals(5, events.size());
    for (int i = 1; i <= events.size(); i++) {
      assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      assertEquals(Event.Type.FAIL, events.get(i - 1).getType());
    }
  }

  // When specific documents have an error, they are returned by sendToIndex & the method does not throw an Exception
  @Test
  public void testOpensearchDocumentErrors() throws Exception {
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
    BulkResponseItem mockItemDoc1 = Mockito.mock(BulkResponseItem.class);
    BulkResponseItem mockItemDoc2 = Mockito.mock(BulkResponseItem.class);
    BulkResponseItem mockItemDoc3 = Mockito.mock(BulkResponseItem.class);
    ErrorCause mockError = new ErrorCause.Builder().reason("mock reason").type("mock-type").build();
    Mockito.when(mockItemDoc1.error()).thenReturn(mockError);
    Mockito.when(mockItemDoc3.error()).thenReturn(mockError);

    when(mockItemDoc1.id()).thenReturn("doc1");
    when(mockItemDoc2.id()).thenReturn("doc2");
    // testing that the idOverride field doesn't prevent a document from being returned in failed docs
    // the "id" will be set to be the override field. The exception would be thrown (instead of adding to failed docs)
    // if the id returned here doesn't match the id the doc gets indexed with.
    when(mockItemDoc3.id()).thenReturn("something_else");

    List<BulkResponseItem> bulkResponseItems = Arrays.asList(mockItemDoc1, mockItemDoc2, mockItemDoc3);
    Mockito.when(mockResponse.items()).thenReturn(bulkResponseItems);

    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/testOverride.conf");

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");
    Document doc3 = Document.create("doc3", "test_run");
    doc3.setField("other_id", "something_else");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, mockClient2, "testing");
    Set<Pair<Document, String>> failedDocs = indexer.sendToIndex(List.of(doc, doc2, doc3));
    assertEquals(2, failedDocs.size());
    assertTrue(failedDocs.stream().anyMatch(p -> p.getLeft().equals(doc)));
    assertFalse(failedDocs.stream().anyMatch(p -> p.getLeft().equals(doc2)));
    assertTrue(failedDocs.stream().anyMatch(p -> p.getLeft().equals(doc3)));
  }

  @Test
  public void testValidateConnection() throws Exception {
    OpenSearchClient mockFailPingClient = Mockito.mock(OpenSearchClient.class);
    when(mockFailPingClient.ping()).thenThrow(RuntimeException.class);

    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/config.conf");
    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, mockClient, "testing");
    assertTrue(indexer.validateConnection()); // should only work the first time with the mockClient
    assertFalse(indexer.validateConnection());
    assertFalse(indexer.validateConnection());

    OpenSearchIndexer nullClientIndexer = new OpenSearchIndexer(config, messenger, null, "testing");
    Assert.assertTrue(nullClientIndexer.validateConnection());

    OpenSearchIndexer failPingClientIndexer = new OpenSearchIndexer(config, messenger, mockFailPingClient, "testing");
    Assert.assertFalse(failPingClientIndexer.validateConnection());
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
    assertEquals("lucille-default", indexRequest.index());
    assertEquals(5, messenger.getSentEvents().size());

    List<Event> events = messenger.getSentEvents();
    assertEquals("doc1", events.get(0).getDocumentId());
    assertEquals(Event.Type.FINISH, events.get(0).getType());
  }

  @Test
  public void testMultipleBatchesIndexField() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/batchingIndexOverride.conf");
    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, mockClient, "testing");

    Document doc = Document.create("doc1", "test_run");
    doc.setField("index", "i1");
    Document doc2 = Document.create("doc2", "test_run");
    doc2.setField("index", "i1");
    Document doc3 = Document.create("doc3", "test_run");
    doc3.setField("index", "i1");
    Document doc4 = Document.create("doc4", "test_run");
    doc4.setField("index", "i1");
    Document doc5 = Document.create("doc5", "test_run");
    doc5.setField("index", "i1");

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
    assertEquals(doc5.getString("index"), indexRequest.index());
    assertEquals(5, messenger.getSentEvents().size());

    List<Event> events = messenger.getSentEvents();
    assertEquals("doc1", events.get(0).getDocumentId());
    assertEquals(Event.Type.FINISH, events.get(0).getType());
  }

  @Test
  public void testMultipleBatchesSeparateIndexFields() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/batchingIndexOverride.conf");
    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, mockClient, "testing");

    Document doc = Document.create("doc1", "test_run");
    doc.setField("index", "i1");
    Document doc2 = Document.create("doc2", "test_run");
    doc2.setField("index", "i1");
    Document doc3 = Document.create("doc3", "test_run");
    doc3.setField("index", "i2");
    Document doc4 = Document.create("doc4", "test_run");
    doc4.setField("index", "i2");
    Document doc5 = Document.create("doc5", "test_run");
    doc5.setField("index", "i3");

    messenger.sendForIndexing(doc);
    messenger.sendForIndexing(doc2);
    messenger.sendForIndexing(doc3);
    messenger.sendForIndexing(doc4);
    messenger.sendForIndexing(doc5);
    indexer.run(5);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(
        BulkRequest.class);

    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    List<BulkRequest> bulkRequestValue = bulkRequestArgumentCaptor.getAllValues();
    assertEquals(1, bulkRequestValue.size());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation indexRequest = requests.get(0).index();

    assertEquals(doc.getId(), indexRequest.id());
    assertEquals(doc.getString("index"), indexRequest.index());
    assertEquals(5, messenger.getSentEvents().size());

    assertEquals(doc.getString("index"), requests.get(0).index().index());
    assertEquals(doc2.getString("index"), requests.get(1).index().index());
    assertEquals(doc3.getString("index"), requests.get(2).index().index());
    assertEquals(doc4.getString("index"), requests.get(3).index().index());
    assertEquals(doc5.getString("index"), requests.get(4).index().index());
    assertEquals(doc.getId(), requests.get(0).index().id());
    assertEquals(doc2.getId(), requests.get(1).index().id());
    assertEquals(doc3.getId(), requests.get(2).index().id());
    assertEquals(doc4.getId(), requests.get(3).index().id());
    assertEquals(doc5.getId(), requests.get(4).index().id());

    List<Event> events = messenger.getSentEvents();
    assertEquals("doc1", events.get(0).getDocumentId());
    assertEquals(Event.Type.FINISH, events.get(0).getType());
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
    // checking each request
    for (DeleteByQueryRequest deleteByQueryRequest : deleteByQueryRequestValue) {
      assert deleteByQueryRequest.query() != null;
      BoolQuery query = deleteByQueryRequest.query().bool();
      // check that we did not set filter, must or mustnot
      assertEquals(0,query.filter().size());
      assertEquals(0, query.must().size());
      assertEquals(0, query.mustNot().size());

      // we expect 1 termsQuery for each deleteByQueryRequest
      List<Query> queries = query.should();
      assertEquals(1, queries.size());
      TermsQuery termsQuery = queries.get(0).terms();

      // check that the terms and fieldValue is what we expect from doc8 and doc10
      assertEquals("field", termsQuery.field());
      List<FieldValue> fieldValues = termsQuery.terms().value();
      assertEquals(1, fieldValues.size());
      assertEquals("value", fieldValues.get(0).stringValue());
    }
  }

  @Test
  public void testDeleteByQuery() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/testDeleteByQuery.conf");
    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, mockClient, "testing");

    Document doc1DelByQuery = Document.create("doc1", "test_run");
    doc1DelByQuery.setField("deleted", "true");
    doc1DelByQuery.setField("field", "doc1field");
    doc1DelByQuery.setField("value", "doc1value");

    Document doc1v2DelByQuery = Document.create("doc1v2", "test_run");
    doc1v2DelByQuery.setField("deleted", "true");
    doc1v2DelByQuery.setField("field", "doc1field");
    doc1v2DelByQuery.setField("value", "doc1v2value");

    Document doc2DelByQuery = Document.create("doc2", "test_run");
    doc2DelByQuery.setField("deleted", "true");
    doc2DelByQuery.setField("field", "doc2field");
    doc2DelByQuery.setField("value", "doc2value");

    messenger.sendForIndexing(doc1DelByQuery);
    messenger.sendForIndexing(doc1v2DelByQuery);
    messenger.sendForIndexing(doc2DelByQuery);

    indexer.run(3);

    ArgumentCaptor<DeleteByQueryRequest> deleteByQueryRequestArgumentCaptor = ArgumentCaptor.forClass(DeleteByQueryRequest.class);
    verify(mockClient, times(1)).deleteByQuery(deleteByQueryRequestArgumentCaptor.capture());

    assertEquals(1, deleteByQueryRequestArgumentCaptor.getAllValues().size());
    DeleteByQueryRequest deleteByQueryRequestValue = deleteByQueryRequestArgumentCaptor.getAllValues().get(0);
    assert deleteByQueryRequestValue.query() != null;
    BoolQuery query = deleteByQueryRequestValue.query().bool();
    assertEquals("1", query.minimumShouldMatch());

    // check that bool query does not contain any filter/must/mustnots
    assertEquals(0,query.filter().size());
    assertEquals(0, query.must().size());
    assertEquals(0, query.mustNot().size());

    // we expect 2 termQueries doc1field and doc2field
    List<Query> queries = query.should();
    assertEquals(2, queries.size());
    TermsQuery query1 = queries.get(0).terms();
    TermsQuery query2 = queries.get(1).terms();

    assertEquals("doc1field", query1.field());
    assertEquals("doc2field", query2.field());

    List<FieldValue> fieldValues1 = query1.terms().value();
    List<FieldValue> fieldValues2 = query2.terms().value();
    assertEquals(2, fieldValues1.size());
    assertEquals(1, fieldValues2.size());

    assertEquals("doc1value", fieldValues1.get(0).stringValue());
    assertEquals("doc1v2value", fieldValues1.get(1).stringValue());
    assertEquals("doc2value", fieldValues2.get(0).stringValue());
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

    assertEquals(1, messenger.getSentEvents().size());

    List<Event> events = messenger.getSentEvents();
    assertEquals("doc1", events.get(0).getDocumentId());
    assertEquals(Event.Type.FINISH, events.get(0).getType());
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

    assertEquals(1, messenger.getSentEvents().size());

    List<Event> events = messenger.getSentEvents();
    assertEquals("doc1", events.get(0).getDocumentId());
    assertEquals(Event.Type.FINISH, events.get(0).getType());
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

    assertEquals(1, messenger.getSentEvents().size());

    List<Event> events = messenger.getSentEvents();
    assertEquals("doc1", events.get(0).getDocumentId());
    assertEquals(Event.Type.FINISH, events.get(0).getType());
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
  public void testRoutingAndUpdate() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/routingAndUpdate.conf");

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
    UpdateOperation updateRequest = requests.get(0).update();

    assertEquals("doc1", updateRequest.id());
    assertEquals("routing1", updateRequest.routing());
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
  public void testDocumentVersioningWithUpdate() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/versioningUpdate.conf");

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
    UpdateOperation indexRequest = requests.get(0).update();

    Long expectedVersion = Long.valueOf(100);
    assertEquals("doc1", indexRequest.id());
    assertEquals(expectedVersion, indexRequest.version());
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
      assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      assertEquals(Type.FAIL, events.get(i - 1).getType());
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

  @Test
  public void testCloseConnection() throws IOException {
    OpenSearchTransport mockTransport = Mockito.mock(OpenSearchTransport.class);
    OpenSearchClient mockTransportClient = Mockito.mock(OpenSearchClient.class);
    when(mockTransportClient._transport()).thenReturn(mockTransport);

    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/config.conf");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, null, "testing");
    // will print out to the logs but no errors despite null client.
    indexer.closeConnection();

    // only the call to check if it is null
    indexer = new OpenSearchIndexer(config, messenger, mockClient, "testing");
    indexer.closeConnection();
    verify(mockClient, times(1))._transport();

    indexer = new OpenSearchIndexer(config, messenger, mockTransportClient, "testing");
    indexer.closeConnection();
    // call to check if null (it isn't) and then a call to close.
    verify(mockTransportClient, times(2))._transport();
    verify(mockTransport, times(1)).close();

    // No error if the close goes wrong
    Mockito.doThrow(new RuntimeException("Mock Exception")).when(mockTransport).close();
    indexer.closeConnection();
  }

  public static class ErroringOpenSearchIndexer extends OpenSearchIndexer {

    public static final Spec SPEC = OpenSearchIndexer.SPEC;

    public ErroringOpenSearchIndexer(Config config, IndexerMessenger messenger,
        OpenSearchClient client, String metricsPrefix) {
      super(config, messenger, client, "testing");
    }

    @Override
    public Set<Pair<Document, String>> sendToIndex(List<Document> docs) throws Exception {
      throw new Exception("Test that errors when sending to indexer are correctly handled");
    }
  }
}
