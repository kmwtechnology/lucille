package com.kmwllc.lucille.indexer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.core.IndexerRetryableException;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
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

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);
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

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);
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

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);
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

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient2);
    Set<Pair<Document, Exception>> failedDocs = indexer.sendToIndex(List.of(doc, doc2, doc3));
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
    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);
    assertTrue(indexer.validateConnection()); // should only work the first time with the mockClient
    assertFalse(indexer.validateConnection());
    assertFalse(indexer.validateConnection());

    OpenSearchIndexer bypassClientIndexer = new OpenSearchIndexer(config, messenger, true, "testing", null);
    Assert.assertTrue(bypassClientIndexer.validateConnection());

    OpenSearchIndexer failPingClientIndexer = new OpenSearchIndexer(config, messenger, "testing", mockFailPingClient);
    Assert.assertFalse(failPingClientIndexer.validateConnection());
  }

  @Test
  public void testMultipleBatches() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/batching.conf");
    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);

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
    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);

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
    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);

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
    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);

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
    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);

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
  public void testDeleteUsesIdOverride() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/batching.conf")
        .withValue("indexer.idOverrideField", ConfigValueFactory.fromAnyRef("other_id"));
    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);

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
    Config config = ConfigFactory.load("OpenSearchIndexerTest/batching.conf")
        .withValue("indexer.deletionMarkerField", ConfigValueFactory.fromAnyRef("file_expired"))
        .withValue("indexer.deletionMarkerFieldValue", ConfigValueFactory.fromAnyRef("true"));
    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);

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
  public void testOpenSearchIndexerNestedJson() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree("{\"a\": [{\"aa\":1}, {\"aa\": 2}] }");
    doc.setField("myJsonField", jsonNode);

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);
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

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);
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

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);
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

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);

    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation indexRequest = requests.get(0).index();
    Map<String, Object> map = (Map<String, Object>) indexRequest.document();

    assertEquals("doc1", indexRequest.id());

    // routing has been set appropriately even though routing field has been deleted by blacklist
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

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);
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

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);
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

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);
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

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient2);
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
   * Tests that the indexer correctly ignores fields stated in the blacklist portion of the config file
   * Note that indexer would even ignore the "id" field if configured, removing the id field in the Lucille document.
   * However, the id would still be passed to the OpenSearch index to id the documents.
   * @throws Exception
   */
  @Test
  public void testBlacklist() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/blacklist.conf");

    Document doc = Document.create("doc1");

    doc.setField("ignoreField1", "value1");
    doc.setField("ignoreField2", "value2");
    doc.setField("normalField", "normalValue");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);
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

  @Test
  public void testWhitelist() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/whitelist.conf");

    Document doc = Document.create("doc1");

    doc.setField("includeField1", "value1");
    doc.setField("includeField2", "value2");
    doc.setField("normalField", "normalValue");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);

    // verify that the bulk method has been called once by mockClient
    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation indexRequest = requests.get(0).index();
    Map<String, Object> map = (Map<String, Object>) indexRequest.document();

    // check that normalField and id have been removed
    assertFalse(map.containsKey("normalField"));
    assertFalse(map.containsValue("normalValue"));
    assertFalse(map.containsKey("id"));
  }

  @Test
  public void testBlacklistAndWhitelist() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/whitelist.conf");

    Document doc = Document.create("doc1");

    doc.setField("ignoreField1", "value1");
    doc.setField("includeField2", "value2");
    doc.setField("normalField", "normalValue");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<BulkRequest> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(BulkRequest.class);

    // verify that the bulk method has been called once by mockClient
    verify(mockClient, times(1)).bulk(bulkRequestArgumentCaptor.capture());

    BulkRequest br = bulkRequestArgumentCaptor.getValue();
    List<BulkOperation> requests = br.operations();
    IndexOperation indexRequest = requests.get(0).index();
    Map<String, Object> map = (Map<String, Object>) indexRequest.document();

    // check that ignoreField1, normalField, and id have been removed
    assertFalse(map.containsKey("ignoreField1"));
    assertFalse(map.containsKey("normalField1"));
    assertTrue(map.containsKey("includeField2"));
    assertTrue(map.containsValue("value2"));
    assertFalse(map.containsKey("id"));
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
    Config config = ConfigFactory.load("OpenSearchIndexerTest/blacklistWithOverride.conf");

    Document doc = Document.create("doc1");

    doc.setField("normalField", "normalValue");
    doc.setField("other_id", "otherId");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);
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

  @Test
  public void testWhitelistWithOverride() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/whitelistWithOverride.conf");

    Document doc = Document.create("doc1");

    doc.setField("whitelistField", "whitelistValue");
    doc.setField("other_id", "otherId");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);
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
    assertEquals(Map.of("whitelistField", "whitelistValue"), map);
  }

  /**
   * Tests that the indexer correctly ignores fields stated in the blacklist portion of the config file
   * Even if idOverride exists, id is still removed and Indexer will use the idOverride as document id
   * @throws Exception
   */
  @Test
  public void testBlacklistWithOverride2() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/blacklistWithOverride2.conf");

    Document doc = Document.create("doc1");

    doc.setField("normalField", "normalValue");
    doc.setField("other_id", "otherId");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);
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

  @Test
  public void testWhitelistWithOverride2() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/whitelistWithOverride2.conf");

    Document doc = Document.create("doc1");

    doc.setField("normalField", "normalValue");
    doc.setField("other_id", "otherId");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);
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
    assertEquals(Map.of("other_id", "otherId"), map);
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

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);
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

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", null);
    // will print out to the logs but no errors despite null client.
    indexer.closeConnection();

    // only the call to check if it is null
    indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);
    indexer.closeConnection();
    verify(mockClient, times(1))._transport();

    indexer = new OpenSearchIndexer(config, messenger, "testing", mockTransportClient);
    indexer.closeConnection();
    // call to check if null (it isn't) and then a call to close.
    verify(mockTransportClient, times(2))._transport();
    verify(mockTransport, times(1)).close();

    // No error if the close goes wrong
    Mockito.doThrow(new RuntimeException("Mock Exception")).when(mockTransport).close();
    indexer.closeConnection();
  }

  /**
   * Tests that the indexer retries a failed batch and succeeds on a subsequent attempt.
   * The mock client throws on the first bulk call and succeeds on the second.
   * With maxRetries=3 and retryWaitDurationMs=1, the indexer should retry and ultimately
   * send FINISH events for all documents.
   */
  @Test
  public void testRetrySucceedsOnSecondAttempt() throws Exception {
    OpenSearchClient retryClient = Mockito.mock(OpenSearchClient.class);

    BooleanResponse mockBooleanResponse = Mockito.mock(BooleanResponse.class);
    Mockito.when(retryClient.ping()).thenReturn(mockBooleanResponse);
    Mockito.when(mockBooleanResponse.value()).thenReturn(true);

    BulkResponse successResponse = Mockito.mock(BulkResponse.class);
    Mockito.when(successResponse.errors()).thenReturn(false);
    Mockito.when(successResponse.items()).thenReturn(new ArrayList<>());

    // Throw on the first call, succeed on the second
    Mockito.when(retryClient.bulk(any(BulkRequest.class)))
        .thenThrow(new IOException("Simulated transient failure"))
        .thenReturn(successResponse);

    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/retry.conf");

    Document doc1 = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", retryClient);
    messenger.sendForIndexing(doc1);
    messenger.sendForIndexing(doc2);
    indexer.run(2);

    // bulk() should have been called twice: once failing, once succeeding
    verify(retryClient, times(2)).bulk(any(BulkRequest.class));

    // Both documents should have received FINISH events
    List<Event> events = messenger.getSentEvents();
    assertEquals(2, events.size());
    assertTrue(events.stream().allMatch(e -> e.getType() == Event.Type.FINISH));
  }

  /**
   * Tests that the indexer exhausts all retries when every attempt fails, and that all
   * documents in the batch receive FAIL events after the final attempt.
   * With maxRetries=3, bulk() should be called 4 times total (1 initial + 3 retries).
   */
  @Test
  public void testRetryExhaustedAllDocumentsFail() throws Exception {
    OpenSearchClient retryClient = Mockito.mock(OpenSearchClient.class);

    BooleanResponse mockBooleanResponse = Mockito.mock(BooleanResponse.class);
    Mockito.when(retryClient.ping()).thenReturn(mockBooleanResponse);
    Mockito.when(mockBooleanResponse.value()).thenReturn(true);

    // Always throw — every attempt fails
    Mockito.when(retryClient.bulk(any(BulkRequest.class)))
        .thenThrow(new IOException("Persistent failure"));

    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/retry.conf");

    Document doc1 = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");
    Document doc3 = Document.create("doc3", "test_run");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", retryClient);
    messenger.sendForIndexing(doc1);
    messenger.sendForIndexing(doc2);
    messenger.sendForIndexing(doc3);
    indexer.run(3);

    // bulk() should have been called 4 times: 1 initial + 3 retries
    verify(retryClient, times(4)).bulk(any(BulkRequest.class));

    // All documents should have received FAIL events
    List<Event> events = messenger.getSentEvents();
    assertEquals(3, events.size());
    assertTrue(events.stream().allMatch(e -> e.getType() == Event.Type.FAIL));
  }

  /**
   * Tests that no retry is attempted when the server returns an HTTP status code that is not
   * in the configured retryableStatusCodes list.
   *
   * The indexer is configured with retryableStatusCodes: [429] and maxRetries: 3.
   * The mock throws an IndexerRetryableException with status 503 (not in the list).
   * bulk() should be called exactly once — no retries — and all documents should receive FAIL events.
   */
  @Test
  public void testNoRetryForNonRetryableStatusCode() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/retryWith429Only.conf");

    Document doc1 = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");

    // Use a subclass that throws IndexerRetryableException with status 503 (not in retryableStatusCodes)
    OpenSearchIndexer indexer = new Status503OpenSearchIndexer(config, messenger, mockClient, "testing");
    messenger.sendForIndexing(doc1);
    messenger.sendForIndexing(doc2);
    indexer.run(2);

    // bulk() on the mock client should never be called — the subclass overrides sendToIndex entirely
    verify(mockClient, times(0)).bulk(any(BulkRequest.class));

    // All documents should have received FAIL events with no retries
    List<Event> events = messenger.getSentEvents();
    assertEquals(2, events.size());
    assertTrue(events.stream().allMatch(e -> e.getType() == Event.Type.FAIL));
  }

  /**
   * Tests that the indexer retries when the bulk call returns without throwing but the response
   * contains an item-level error with a retryable status code (429). On the second attempt the
   * response is clean, so the document should receive a FINISH event and bulk() should be called
   * twice.
   */
  @Test
  public void testRetryOnRetryableItemLevelError() throws Exception {
    OpenSearchClient retryClient = Mockito.mock(OpenSearchClient.class);

    BooleanResponse mockBooleanResponse = Mockito.mock(BooleanResponse.class);
    Mockito.when(retryClient.ping()).thenReturn(mockBooleanResponse);
    Mockito.when(mockBooleanResponse.value()).thenReturn(true);

    // First response: item carries a 429 error — should trigger a retry
    BulkResponseItem errorItem = Mockito.mock(BulkResponseItem.class);
    ErrorCause errorCause = new ErrorCause.Builder()
        .reason("too many requests").type("es_rejected_execution_exception").build();
    Mockito.when(errorItem.error()).thenReturn(errorCause);
    Mockito.when(errorItem.status()).thenReturn(429);
    Mockito.when(errorItem.id()).thenReturn("doc1");

    BulkResponse errorResponse = Mockito.mock(BulkResponse.class);
    Mockito.when(errorResponse.items()).thenReturn(List.of(errorItem));

    // Second response: clean, no errors
    BulkResponseItem successItem = Mockito.mock(BulkResponseItem.class);
    Mockito.when(successItem.error()).thenReturn(null);
    Mockito.when(successItem.id()).thenReturn("doc1");

    BulkResponse successResponse = Mockito.mock(BulkResponse.class);
    Mockito.when(successResponse.items()).thenReturn(List.of(successItem));

    Mockito.when(retryClient.bulk(any(BulkRequest.class)))
        .thenReturn(errorResponse)
        .thenReturn(successResponse);

    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/retry.conf");

    Document doc1 = Document.create("doc1", "test_run");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", retryClient);
    messenger.sendForIndexing(doc1);
    indexer.run(1);

    // bulk() should be called twice: first returns a 429 item error, second succeeds
    verify(retryClient, times(2)).bulk(any(BulkRequest.class));

    // Document should have received a FINISH event after the successful retry
    List<Event> events = messenger.getSentEvents();
    assertEquals(1, events.size());
    assertEquals(Event.Type.FINISH, events.get(0).getType());
  }

  /**
   * Tests that no retry is attempted when the bulk call returns without throwing but the response
   * contains an item-level error with a non-retryable status code (400). bulk() should be called
   * exactly once and the document should receive a FAIL event carrying the item-level error reason.
   */
  @Test
  public void testNoRetryForNonRetryableItemLevelError() throws Exception {
    OpenSearchClient retryClient = Mockito.mock(OpenSearchClient.class);

    BooleanResponse mockBooleanResponse = Mockito.mock(BooleanResponse.class);
    Mockito.when(retryClient.ping()).thenReturn(mockBooleanResponse);
    Mockito.when(mockBooleanResponse.value()).thenReturn(true);

    // Response: item has a 400 error (e.g. mapping conflict) — not in the retryable list
    BulkResponseItem errorItem = Mockito.mock(BulkResponseItem.class);
    ErrorCause errorCause = new ErrorCause.Builder()
        .reason("mapper parsing exception").type("mapper_parsing_exception").build();
    Mockito.when(errorItem.error()).thenReturn(errorCause);
    Mockito.when(errorItem.status()).thenReturn(400);
    Mockito.when(errorItem.id()).thenReturn("doc1");

    BulkResponse errorResponse = Mockito.mock(BulkResponse.class);
    Mockito.when(errorResponse.items()).thenReturn(List.of(errorItem));

    Mockito.when(retryClient.bulk(any(BulkRequest.class))).thenReturn(errorResponse);

    TestMessenger messenger = new TestMessenger();
    // maxRetries: 3, retryableStatusCodes: [429] — 400 is not in the list
    Config config = ConfigFactory.load("OpenSearchIndexerTest/retryWith429Only.conf");

    Document doc1 = Document.create("doc1", "test_run");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", retryClient);
    messenger.sendForIndexing(doc1);
    indexer.run(1);

    // bulk() should be called exactly once — 400 is not retryable
    verify(retryClient, times(1)).bulk(any(BulkRequest.class));

    // Document should receive a FAIL event containing the item-level error reason
    List<Event> events = messenger.getSentEvents();
    assertEquals(1, events.size());
    assertEquals(Event.Type.FAIL, events.get(0).getType());
    assertTrue(events.get(0).getMessage().contains("mapper parsing exception"));
  }

  /**
   * Tests that when retries are exhausted due to persistent item-level retryable errors,
   * each document receives a per-document FAIL event with the specific item-level reason
   * rather than a generic batch-level message.
   * With maxRetries=3, bulk() should be called 4 times (1 initial + 3 retries), and every
   * document should receive a FAIL event containing the item-level error reason.
   */
  @Test
  public void testRetryExhaustedItemLevelErrorsProducePerDocFail() throws Exception {
    OpenSearchClient retryClient = Mockito.mock(OpenSearchClient.class);

    BooleanResponse mockBooleanResponse = Mockito.mock(BooleanResponse.class);
    Mockito.when(retryClient.ping()).thenReturn(mockBooleanResponse);
    Mockito.when(mockBooleanResponse.value()).thenReturn(true);

    // Every response: both items carry a 429 error — retries will be exhausted
    BulkResponseItem errorItem1 = Mockito.mock(BulkResponseItem.class);
    BulkResponseItem errorItem2 = Mockito.mock(BulkResponseItem.class);
    ErrorCause errorCause = new ErrorCause.Builder()
        .reason("too many requests").type("es_rejected_execution_exception").build();
    Mockito.when(errorItem1.error()).thenReturn(errorCause);
    Mockito.when(errorItem1.status()).thenReturn(429);
    Mockito.when(errorItem1.id()).thenReturn("doc1");
    Mockito.when(errorItem2.error()).thenReturn(errorCause);
    Mockito.when(errorItem2.status()).thenReturn(429);
    Mockito.when(errorItem2.id()).thenReturn("doc2");

    BulkResponse errorResponse = Mockito.mock(BulkResponse.class);
    Mockito.when(errorResponse.items()).thenReturn(List.of(errorItem1, errorItem2));

    Mockito.when(retryClient.bulk(any(BulkRequest.class))).thenReturn(errorResponse);

    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/retry.conf");

    Document doc1 = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", retryClient);
    messenger.sendForIndexing(doc1);
    messenger.sendForIndexing(doc2);
    indexer.run(2);

    // bulk() should be called 4 times: 1 initial + 3 retries
    verify(retryClient, times(4)).bulk(any(BulkRequest.class));

    // Each document should receive a per-document FAIL event with the item-level reason
    List<Event> events = messenger.getSentEvents();
    assertEquals(2, events.size());
    assertTrue(events.stream().allMatch(e -> e.getType() == Event.Type.FAIL));
    assertTrue(events.stream().allMatch(e -> e.getMessage().contains("too many requests")));
  }

  @Test
  public void testRetryAgainstLiveServer() throws Exception {
    AtomicInteger requestCount = new AtomicInteger(0);
    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);

    // Minimal valid OpenSearch bulk API response for a single successful index operation
    String bulkSuccessBody = "{\"took\":1,\"errors\":false,\"items\":"
        + "[{\"index\":{\"_index\":\"lucille-default\",\"_id\":\"doc1\","
        + "\"result\":\"created\",\"status\":201}}]}";
    byte[] bulkSuccessBytes = bulkSuccessBody.getBytes(StandardCharsets.UTF_8);

    server.createContext("/", exchange -> {
      int count = requestCount.incrementAndGet();
      if (count == 1) {
        // First request: return 429 to trigger a retry
        exchange.sendResponseHeaders(429, -1);
        exchange.close();
      } else {
        // Second request: return a valid bulk success response
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bulkSuccessBytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
          os.write(bulkSuccessBytes);
        }
        exchange.close();
      }
    });
    server.start();

    Config config = ConfigFactory.load("OpenSearchIndexerTest/retry.conf").
        withValue("opensearch.url", ConfigValueFactory.fromAnyRef("http://localhost:" + server.getAddress().getPort()));
    TestMessenger messenger = new TestMessenger();
    Document doc = Document.create("doc1");

    try {
      OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, false, "testing", "run1");
      messenger.sendForIndexing(doc);
      indexer.run(1);
    } finally {
      server.stop(0);
    }
    assertEquals(1, messenger.getDocsSentForIndexing().size()); // only 1 doc was sent for indexing
    assertEquals(2, requestCount.get()); // server should have received 2 requests: initial request and retry
    assertEquals(1, messenger.getSentEvents().size()); // only 1 accounting event should have been sent
    assertEquals(Event.Type.FINISH, messenger.getSentEvents().get(0).getType()); // document should have succeeded
  }

  /**
   * Tests that unknown-status-code failures (e.g. network timeouts) are NOT retried when the user
   * explicitly specifies retryableStatusCodes without including -1.
   * Config uses retryableStatusCodes: [429] — no -1, so IOException-based failures should not retry.
   */
  @Test
  public void testNoRetryForUnknownStatusCodeWhenNotInList() throws Exception {
    OpenSearchClient retryClient = Mockito.mock(OpenSearchClient.class);

    BooleanResponse mockBooleanResponse = Mockito.mock(BooleanResponse.class);
    Mockito.when(retryClient.ping()).thenReturn(mockBooleanResponse);
    Mockito.when(mockBooleanResponse.value()).thenReturn(true);

    // IOException becomes IndexerRetryableException with UNKNOWN_STATUS_CODE
    Mockito.when(retryClient.bulk(any(BulkRequest.class)))
        .thenThrow(new IOException("Connection refused"));

    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/retryWith429Only.conf");

    Document doc1 = Document.create("doc1", "test_run");

    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", retryClient);
    messenger.sendForIndexing(doc1);
    indexer.run(1);

    // bulk() should have been called only once — no retries for unknown status code
    verify(retryClient, times(1)).bulk(any(BulkRequest.class));

    // Document should have received a FAIL event
    List<Event> events = messenger.getSentEvents();
    assertEquals(1, events.size());
    assertEquals(Event.Type.FAIL, events.get(0).getType());
  }

  /**
   * Tests that an empty retryableStatusCodes list is rejected as invalid configuration.
   */
  @Test
  public void testEmptyRetryableStatusCodesRejected() {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/retry.conf")
        .withValue("indexer.retryableStatusCodes", ConfigValueFactory.fromIterable(List.of()));

    assertThrows(IllegalArgumentException.class, () ->
        new OpenSearchIndexer(config, messenger, "testing", mockClient));
  }

  /**
   * Tests that maxRetries of 0 is rejected as invalid.
   */
  @Test
  public void testMaxRetriesZeroRejected() {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/retry.conf")
        .withValue("indexer.maxRetries", ConfigValueFactory.fromAnyRef(0));

    assertThrows(IllegalArgumentException.class, () ->
        new OpenSearchIndexer(config, messenger, "testing", mockClient));
  }

  /**
   * Tests that retryWaitDurationMs without maxRetries is rejected.
   */
  @Test
  public void testRetryWaitDurationWithoutMaxRetriesRejected() {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/config.conf")
        .withValue("indexer.retryWaitDurationMs", ConfigValueFactory.fromAnyRef(500));

    assertThrows(IllegalArgumentException.class, () ->
        new OpenSearchIndexer(config, messenger, "testing", mockClient));
  }

  /**
   * Tests that retryableStatusCodes without maxRetries is rejected.
   */
  @Test
  public void testRetryableStatusCodesWithoutMaxRetriesRejected() {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/config.conf")
        .withValue("indexer.retryableStatusCodes", ConfigValueFactory.fromIterable(List.of(429)));

    assertThrows(IllegalArgumentException.class, () ->
        new OpenSearchIndexer(config, messenger, "testing", mockClient));
  }

  /**
   * Tests that retryMaxWaitDurationMs without maxRetries is rejected.
   */
  @Test
  public void testRetryMaxWaitDurationWithoutMaxRetriesRejected() {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/config.conf")
        .withValue("indexer.retryMaxWaitDurationMs", ConfigValueFactory.fromAnyRef(5000));

    assertThrows(IllegalArgumentException.class, () ->
        new OpenSearchIndexer(config, messenger, "testing", mockClient));
  }

  /**
   * Tests that retryRandomizationFactor without maxRetries is rejected.
   */
  @Test
  public void testRetryRandomizationFactorWithoutMaxRetriesRejected() {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/config.conf")
        .withValue("indexer.retryRandomizationFactor", ConfigValueFactory.fromAnyRef(0.3));

    assertThrows(IllegalArgumentException.class, () ->
        new OpenSearchIndexer(config, messenger, "testing", mockClient));
  }

  /**
   * Tests that the indexer accepts a retry configuration with all parameters set to their defaults.
   */
  @Test
  public void testRetryConfigWithAllParametersAccepted() {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/config.conf")
        .withValue("indexer.maxRetries", ConfigValueFactory.fromAnyRef(3))
        .withValue("indexer.retryWaitDurationMs", ConfigValueFactory.fromAnyRef(1000))
        .withValue("indexer.retryMaxWaitDurationMs", ConfigValueFactory.fromAnyRef(30000))
        .withValue("indexer.retryRandomizationFactor", ConfigValueFactory.fromAnyRef(0.5))
        .withValue("indexer.retryableStatusCodes", ConfigValueFactory.fromIterable(List.of(429, 503)));

    // Should not throw — all parameters are valid
    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);
    assertTrue(indexer.validateConnection());
  }

  /**
   * Tests that the indexer accepts a retry configuration with non-default custom values
   * for max wait duration and randomization factor.
   */
  @Test
  public void testRetryConfigWithCustomMaxWaitAndRandomization() {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("OpenSearchIndexerTest/config.conf")
        .withValue("indexer.maxRetries", ConfigValueFactory.fromAnyRef(5))
        .withValue("indexer.retryWaitDurationMs", ConfigValueFactory.fromAnyRef(500))
        .withValue("indexer.retryMaxWaitDurationMs", ConfigValueFactory.fromAnyRef(10000))
        .withValue("indexer.retryRandomizationFactor", ConfigValueFactory.fromAnyRef(0.0));

    // Should not throw — custom values are valid
    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);
    assertTrue(indexer.validateConnection());
  }


  public static class ErroringOpenSearchIndexer extends OpenSearchIndexer {

    public static final Spec SPEC = OpenSearchIndexer.SPEC;

    public ErroringOpenSearchIndexer(Config config, IndexerMessenger messenger,
        OpenSearchClient client, String metricsPrefix) {
      super(config, messenger, "testing", client);
    }

    @Override
    public Set<Pair<Document, Exception>> sendToIndex(List<Document> docs) throws Exception {
      throw new Exception("Test that errors when sending to indexer are correctly handled");
    }
  }

  /**
   * An OpenSearchIndexer subclass that always throws an {@link IndexerRetryableException}
   * with HTTP status 503, used to verify that status codes not in the configured
   * retryableStatusCodes list do not trigger a retry.
   */
  public static class Status503OpenSearchIndexer extends OpenSearchIndexer {

    public static final Spec SPEC = OpenSearchIndexer.SPEC;

    public Status503OpenSearchIndexer(Config config, IndexerMessenger messenger,
        OpenSearchClient client, String metricsPrefix) {
      super(config, messenger, metricsPrefix, client);
    }

    @Override
    public Set<Pair<Document, Exception>> sendToIndex(List<Document> docs) throws Exception {
      throw new IndexerRetryableException(
          503, "Simulated 503 Service Unavailable", new IOException("backend unavailable"));
    }
  }
}

