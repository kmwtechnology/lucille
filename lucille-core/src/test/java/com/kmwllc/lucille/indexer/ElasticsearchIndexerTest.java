package com.kmwllc.lucille.indexer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.TermsQuery;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteByQueryRequest;
import co.elastic.clients.elasticsearch.core.DeleteByQueryResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch._types.VersionType;
import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.endpoints.BooleanResponse;
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
import com.typesafe.config.ConfigValueFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    DeleteByQueryResponse mockDeleteByQueryResponse = Mockito.mock(DeleteByQueryResponse.class);
    when(mockClient.deleteByQuery(any(DeleteByQueryRequest.class))).thenReturn(mockDeleteByQueryResponse);
    when(mockDeleteByQueryResponse.failures()).thenReturn(new ArrayList<>());
  }

  /**
   * Tests that the indexer correctly polls completed documents from the destination topic and sends them to
   * Elasticsearch.
   *
   * @throws Exception
   */
  @Test
  public void testElasticsearchIndexer() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);
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

  // Unsupported, but shouldn't cause any errors.
  @Test
  public void testIndexerWithChildDocs() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    doc.addChild(Document.create("childDoc1", "test_run"));

    Document doc2 = Document.create("doc2", "test_run");
    Document childDoc2 = Document.create("childDoc2", "test_run");
    doc2.addChild(childDoc2);
    childDoc2.addChild(Document.create("child_childDoc2", "test_run"));

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);
    messenger.sendForIndexing(doc);
    messenger.sendForIndexing(doc2);
    indexer.run(2);

    Assert.assertEquals(2, messenger.getSentEvents().size());
  }

  @Test
  public void testElasticsearchIndexerUpdate() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/updateConfig.conf");

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);
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

  // When throwing an exception, all documents fail
  @Test
  public void testElasticsearchIndexerException() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/exception.conf");

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");
    Document doc3 = Document.create("doc3", "test_run");
    Document doc4 = Document.create("doc4", "test_run");
    Document doc5 = Document.create("doc5", "test_run");

    ElasticsearchIndexer indexer = new ErroringElasticsearchIndexer(config, messenger, "testing", mockClient);
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

  // When specific documents have an error, they are returned by sendToIndex & the method does not throw an Exception
  @Test
  public void testElasticsearchDocumentErrors() throws Exception {
    ElasticsearchClient mockClient2 = Mockito.mock(ElasticsearchClient.class);

    // make first call to validateConnection succeed but subsequent calls to fail
    Mockito.when(mockClient2.ping()).thenReturn(new BooleanResponse(true), new BooleanResponse(false));

    BulkResponse mockResponse = Mockito.mock(BulkResponse.class);
    Mockito.when(mockClient2.bulk(any(BulkRequest.class))).thenReturn(mockResponse);

    // mocking for the bulk response items and error causes
    BulkResponseItem mockItemDoc1 = Mockito.mock(BulkResponseItem.class);
    BulkResponseItem mockItemDoc2 = Mockito.mock(BulkResponseItem.class);
    BulkResponseItem mockItemDoc3 = Mockito.mock(BulkResponseItem.class);
    ErrorCause mockError = new ErrorCause.Builder().reason("mock reason").type("mock-type").build();
    Mockito.when(mockItemDoc1.error()).thenReturn(mockError);
    Mockito.when(mockItemDoc3.error()).thenReturn(mockError);

    Mockito.when(mockItemDoc1.id()).thenReturn("doc1");
    Mockito.when(mockItemDoc2.id()).thenReturn("doc2");
    // testing that the idOverride field doesn't prevent a document from being returned in failed docs
    // the "id" will be set to be the override field. The exception would be thrown (instead of adding to failed docs)
    // if the id returned here doesn't match the id the doc gets indexed with.
    Mockito.when(mockItemDoc3.id()).thenReturn("something_else");

    List<BulkResponseItem> bulkResponseItems = Arrays.asList(mockItemDoc1, mockItemDoc2, mockItemDoc3);
    Mockito.when(mockResponse.items()).thenReturn(bulkResponseItems);

    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/testOverride.conf");

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");
    Document doc3 = Document.create("doc3", "test_run");
    doc3.setField("other_id", "something_else");

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient2);
    Set<Pair<Document, String>> failedDocs = indexer.sendToIndex(List.of(doc, doc2, doc3));
    assertEquals(2, failedDocs.size());
    assertTrue(failedDocs.stream().anyMatch(p -> p.getLeft().equals(doc)));
    assertFalse(failedDocs.stream().anyMatch(p -> p.getLeft().equals(doc2)));
    assertTrue(failedDocs.stream().anyMatch(p -> p.getLeft().equals(doc3)));
  }

  @Test
  public void testValidateConnection() throws Exception {
    ElasticsearchClient mockFailPingClient = Mockito.mock(ElasticsearchClient.class);
    Mockito.when(mockFailPingClient.ping()).thenThrow(RuntimeException.class);

    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/config.conf");
    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);
    Assert.assertTrue(indexer.validateConnection()); // should only work the first time with the mockClient
    Assert.assertFalse(indexer.validateConnection());
    Assert.assertFalse(indexer.validateConnection());

    ElasticsearchIndexer bypassClientIndexer = new ElasticsearchIndexer(config, messenger, true, "testing", null);
    Assert.assertTrue(bypassClientIndexer.validateConnection());

    ElasticsearchIndexer failPingClientIndexer = new ElasticsearchIndexer(config, messenger, "testing", mockFailPingClient);
    Assert.assertFalse(failPingClientIndexer.validateConnection());
  }

  @Test
  public void testMultipleBatches() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/batching.conf");
    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);

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

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);

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
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/batching.conf");
    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);

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

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);
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
      assertEquals(0, query.filter().size());
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
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/testDeleteByQuery.conf");
    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);

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
    assertEquals(0, query.filter().size());
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
  public void testDeleteUsesIdOverride() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/batching.conf")
        .withValue("indexer.idOverrideField", ConfigValueFactory.fromAnyRef("other_id"));
    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);

    Document deletedDoc = Document.create("doc-original", "test_run");
    deletedDoc.setField("other_id", "doc-override");
    deletedDoc.setField("deleted", "true");

    messenger.sendForIndexing(deletedDoc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);
    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());
    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    assertEquals(1, br.operations().size());
    // delete path should use id override just like index/update paths.
    assertNotNull(br.operations().get(0).delete());
    assertEquals("doc-override", br.operations().get(0).delete().id());
  }

  @Test
  public void testDeletionMarkerFieldAndValue() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/batching.conf")
        .withValue("indexer.deletionMarkerField", ConfigValueFactory.fromAnyRef("file_expired"))
        .withValue("indexer.deletionMarkerFieldValue", ConfigValueFactory.fromAnyRef("true"));
    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);

    Document expiredDoc = Document.create("doc-expired", "test_run");
    // deletion marker check uses getString(), so boolean true matches "true".
    expiredDoc.setField("file_expired", true);

    messenger.sendForIndexing(expiredDoc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);
    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());
    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    assertEquals(1, br.operations().size());
    assertNotNull(br.operations().get(0).delete());
    assertEquals("doc-expired", br.operations().get(0).delete().id());
  }

  @Test
  public void testElasticsearchIndexerNestedJson() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree("{\"a\": [{\"aa\":1}, {\"aa\": 2}] }");
    doc.setField("myJsonField", jsonNode);

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);
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
  public void testElasticsearchIndexerNestedJsonMultivalued() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree("{\"a\": [{\"aa\":1}, {\"aa\": 2}] }");
    doc.setField("myJsonField", jsonNode);

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);
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
  public void testElasticsearchIndexerNestedJsonWithObjects() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree("{\"a\": {\"aa\":1}, \"b\":{\"ab\": 2} }");
    doc.setField("myJsonField", jsonNode);

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);
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
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/routing.conf");

    Document doc = Document.create("doc1");
    doc.setField("routing", "routing1");
    doc.setField("field1", "value1");

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);
    messenger.sendForIndexing(doc);
    indexer.run(1);
    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);
    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();

    List<BulkOperation> requests = br.operations();
    IndexOperation<Map<String, Object>> indexRequest = requests.get(0).index();

    assertEquals("doc1", indexRequest.id());

    // routing has been set appropriately even though routing field has been deleted by blacklist
    // note that id has also been deleted from the document, but the id is still passed to the ElasticSearch Index
    // to id documents.
    assertEquals("routing1", indexRequest.routing());
    assertEquals(Map.of("field1", "value1"), indexRequest.document());
  }

  @Test
  public void testRoutingAndUpdate() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/routingAndUpdate.conf");

    Document doc = Document.create("doc1");
    doc.setField("routing", "routing1");
    doc.setField("field1", "value1");

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);
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
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/versioning.conf");

    KafkaDocument doc = new KafkaDocument(
        new ObjectMapper().createObjectNode()
            .put("id", "doc1")
            .put("field1", "value1"));
    doc.setKafkaMetadata(new ConsumerRecord<>("testing", 0, 100, null, null));

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);
    messenger.sendForIndexing(doc);
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

  @Test
  public void testDocumentVersioningWithUpdate() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/versioningUpdate.conf");

    KafkaDocument doc = new KafkaDocument(
        new ObjectMapper().createObjectNode()
            .put("id", "doc1")
            .put("field1", "value1"));
    doc.setKafkaMetadata(new ConsumerRecord<>("testing", 0, 100, null, null));

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);
    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();

    UpdateOperation indexRequest = requests.get(0).update();
    assertEquals("doc1", indexRequest.id());
    assertEquals(Long.valueOf(100), indexRequest.version());
  }

  private void testJoin(String configPath, Document doc, Map<String, Object> expected) throws Exception {

    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load(configPath);

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);
    messenger.sendForIndexing(doc);
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

    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");
    Document doc3 = Document.create("doc3", "test_run");

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient2);
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
   * Tests that the indexer correctly ignores fields stated in the blacklist portion of the config file
   * Note that indexer would even ignore the "id" field if configured, removing the id field in the Lucille document.
   * However, the id would still be passed to the ElasticSearch index to id the documents.
   * @throws Exception
   */
  @Test
  public void testBlacklist() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/blacklist.conf");

    Document doc = Document.create("doc1");
    doc.setField("ignoreField1", "value1");
    doc.setField("ignoreField2", "value2");
    doc.setField("normalField", "normalValue");

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);
    // verify that bulk has been called by the mockClient once
    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation<Map<String, Object>> indexRequest = requests.get(0).index();

    // check that ignoreField1, ignoreField2, and id has been removed
    assertEquals(Map.of("normalField", "normalValue"), indexRequest.document());
  }

  @Test
  public void testWhitelist() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/whitelist.conf");

    Document doc = Document.create("doc1");
    doc.setField("includeField1", "value1");
    doc.setField("includeField2", "value2");
    doc.setField("normalField", "normalValue");

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);
    // verify that bulk has been called by the mockClient once
    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation<Map<String, Object>> indexRequest = requests.get(0).index();

    // check that normalField and id have been removed
    assertEquals(Map.of("includeField1", "value1", "includeField2", "value2"), indexRequest.document());
  }

  @Test
  public void testBlacklistAndWhitelist() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/whitelist.conf");

    Document doc = Document.create("doc1");
    doc.setField("ignoreField1", "value1");
    doc.setField("includeField2", "value2");
    doc.setField("normalField", "normalValue");

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);
    // verify that bulk has been called by the mockClient once
    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation<Map<String, Object>> indexRequest = requests.get(0).index();

    // check that ignoreField1, normalField, and id have been removed
    assertEquals(Map.of("includeField2", "value2"), indexRequest.document());
  }

  /**
   * Tests that the indexer correctly ignores fields stated in the blacklist portion of the config file
   * In this case both id & idOverride is removed from the Lucille Document, but the idOverride is still
   * used as the Document id for the Indexer
   *
   * @throws Exception
   */
  @Test
  public void testBlacklistWithOverride() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/blacklistWithOverride.conf");

    Document doc = Document.create("doc1");
    doc.setField("normalField", "normalValue");
    doc.setField("other_id", "otherId");

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);
    // verify that bulk has been called by the mockClient once
    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation<Map<String, Object>> indexRequest = requests.get(0).index();

    // check that id and other_id has been removed
    assertEquals(Map.of("normalField", "normalValue"), indexRequest.document());
  }

  @Test
  public void testWhitelistWithOverride() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/whitelistWithOverride.conf");

    Document doc = Document.create("doc1");
    doc.setField("normalField", "normalValue");
    doc.setField("other_id", "otherId");

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);
    // verify that bulk has been called by the mockClient once
    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation<Map<String, Object>> indexRequest = requests.get(0).index();

    // check that id and other_id have been removed
    assertEquals(Map.of("normalField", "normalValue"), indexRequest.document());
  }

  /**
   * Tests that the indexer correctly ignores fields stated in the blacklist portion of the config file
   * Even if idOverride exists, id is still removed and Indexer will use the idOverride as document id
   * @throws Exception
   */
  @Test
  public void testBlacklistWithOverride2() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/blacklistWithOverride2.conf");

    Document doc = Document.create("doc1");
    doc.setField("normalField", "normalValue");
    doc.setField("other_id", "otherId");

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);
    // verify that bulk has been called by the mockClient once
    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation<Map<String, Object>> indexRequest = requests.get(0).index();

    // check that id has been removed and that other_id field remains
    assertEquals(Map.of("other_id", "otherId", "normalField", "normalValue"), indexRequest.document());
  }

  @Test
  public void testWhitelistWithOverride2() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/whitelistWithOverride2.conf");

    Document doc = Document.create("doc1");
    doc.setField("normalField", "normalValue");
    doc.setField("other_id", "otherId");

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);
    // verify that bulk has been called by the mockClient once
    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation<Map<String, Object>> indexRequest = requests.get(0).index();

    // check that id and normalField have been removed and that other_id field remains
    assertEquals(Map.of("other_id", "otherId"), indexRequest.document());
  }

  /**
   * Tests that the indexer correctly overrides the id if it exists in conf
   * @throws Exception
   */

  @Test
  public void testOverride() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/testOverride.conf");

    Document doc = Document.create("doc1");
    doc.setField("other_id", "otherId");

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);
    // verify that bulk has been called by the mockClient once
    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation<Map<String, Object>> indexRequest = requests.get(0).index();

    // check that id has been overwritten and that other_id field remains
    assertEquals(Map.of("id", "otherId", "other_id", "otherId"), indexRequest.document());
  }

  @Test
  public void testInvalidConfig() {
    // cannot use the indexOverrideField
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/invalidConfig.conf");

    assertThrows(IllegalArgumentException.class,
        () -> new ElasticsearchIndexer(config, messenger, "testing", mockClient)
    );
  }

  @Test
  public void testCloseConnection() throws IOException {
    ElasticsearchTransport mockTransport = Mockito.mock(ElasticsearchTransport.class);
    ElasticsearchClient mockTransportClient = Mockito.mock(ElasticsearchClient.class);
    when(mockTransportClient._transport()).thenReturn(mockTransport);

    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("ElasticsearchIndexerTest/testOverride.conf");

    ElasticsearchIndexer indexer = new ElasticsearchIndexer(config, messenger, "testing", null);
    // will print out to the logs but no errors despite null client.
    indexer.closeConnection();

    // only the call to check if it is null
    indexer = new ElasticsearchIndexer(config, messenger, "testing", mockClient);
    indexer.closeConnection();
    verify(mockClient, times(1))._transport();

    indexer = new ElasticsearchIndexer(config, messenger, "testing", mockTransportClient);
    indexer.closeConnection();
    // call to check if null (it isn't) and then a call to close.
    verify(mockTransportClient, times(2))._transport();
    verify(mockTransport, times(1)).close();

    // No error if the close goes wrong
    Mockito.doThrow(new RuntimeException("Mock Exception")).when(mockTransport).close();
    indexer.closeConnection();
  }

  public static class ErroringElasticsearchIndexer extends ElasticsearchIndexer {

    public static final Spec SPEC = ElasticsearchIndexer.SPEC;

    public ErroringElasticsearchIndexer(Config config, IndexerMessenger messenger,
        String metricsPrefix, ElasticsearchClient client) {
      super(config, messenger, "testing", client);
    }

    @Override
    public Set<Pair<Document, String>> sendToIndex(List<Document> docs) throws Exception {
      throw new Exception("Test that errors when sending to indexer are correctly handled");
    }
  }
}
