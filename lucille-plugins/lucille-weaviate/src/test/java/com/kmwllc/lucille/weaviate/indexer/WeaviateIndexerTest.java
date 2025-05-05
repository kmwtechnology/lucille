package com.kmwllc.lucille.weaviate.indexer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Result;
import io.weaviate.client.base.WeaviateError;
import io.weaviate.client.v1.batch.Batch;
import io.weaviate.client.v1.batch.api.ObjectsBatcher;
import io.weaviate.client.v1.batch.model.ObjectGetResponse;
import io.weaviate.client.v1.batch.model.ObjectsGetResponseAO2Result;
import io.weaviate.client.v1.batch.model.ObjectsGetResponseAO2Result.ErrorResponse;
import io.weaviate.client.v1.data.model.WeaviateObject;
import io.weaviate.client.v1.data.replication.model.ConsistencyLevel;
import io.weaviate.client.v1.misc.Misc;
import io.weaviate.client.v1.misc.api.MetaGetter;
import io.weaviate.client.v1.misc.model.Meta;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class WeaviateIndexerTest {

  private WeaviateClient mockClient;
  private ObjectsBatcher mockObjectsBatcher;

  @Before
  public void setup() {
    setupWeaviateClient();
  }

  private void setupWeaviateClient() {
    mockClient = Mockito.mock(WeaviateClient.class);

    Misc mockMisc = Mockito.mock(Misc.class);
    when(mockClient.misc()).thenReturn(mockMisc);

    MetaGetter mockMetaGetter = Mockito.mock(MetaGetter.class);
    when(mockMisc.metaGetter()).thenReturn(mockMetaGetter);

    Result<Meta> mockMetaResult = Mockito.mock(Result.class);
    when(mockMetaGetter.run()).thenReturn(mockMetaResult);
    // return null first and then 404 error
    when(mockMetaResult.getError()).thenReturn(null,
        new WeaviateError(404, new ArrayList<>()));

    Meta mockMeta = Mockito.mock(Meta.class);
    when(mockMeta.getHostname()).thenReturn("localhost");
    when(mockMeta.getVersion()).thenReturn("1.0.0");
    when(mockMeta.getModules()).thenReturn("modules");

    when(mockMetaResult.getResult()).thenReturn(mockMeta);

    Batch mockBatch = Mockito.mock(Batch.class);
    when(mockClient.batch()).thenReturn(mockBatch);

    mockObjectsBatcher = Mockito.mock(ObjectsBatcher.class);
    when(mockBatch.objectsBatcher()).thenReturn(mockObjectsBatcher);
    when(mockObjectsBatcher.withConsistencyLevel(ConsistencyLevel.ALL)).thenReturn(mockObjectsBatcher);

    Result<ObjectGetResponse[]> mockResponse = Mockito.mock(Result.class);
    when(mockObjectsBatcher.run()).thenReturn(mockResponse);
    when(mockResponse.getResult()).thenReturn(new ObjectGetResponse[]{});
  }

  // call this method at the beginning of a test to setup a weaviate client that returns 5 documents, two of which failed.
  // you'll have to mock the UUID generation in order to retrieve the Documents correctly.
  private void setupWeaviateClientWithSpecificFailures() {
    mockClient = Mockito.mock(WeaviateClient.class);

    Misc mockMisc = Mockito.mock(Misc.class);
    when(mockClient.misc()).thenReturn(mockMisc);

    MetaGetter mockMetaGetter = Mockito.mock(MetaGetter.class);
    when(mockMisc.metaGetter()).thenReturn(mockMetaGetter);

    Result<Meta> mockMetaResult = Mockito.mock(Result.class);
    when(mockMetaGetter.run()).thenReturn(mockMetaResult);
    // return null first and then 404 error
    when(mockMetaResult.getError()).thenReturn(null,
        new WeaviateError(404, new ArrayList<>()));

    Meta mockMeta = Mockito.mock(Meta.class);
    when(mockMeta.getHostname()).thenReturn("localhost");
    when(mockMeta.getVersion()).thenReturn("1.0.0");
    when(mockMeta.getModules()).thenReturn("modules");

    when(mockMetaResult.getResult()).thenReturn(mockMeta);

    Batch mockBatch = Mockito.mock(Batch.class);
    when(mockClient.batch()).thenReturn(mockBatch);

    mockObjectsBatcher = Mockito.mock(ObjectsBatcher.class);
    when(mockBatch.objectsBatcher()).thenReturn(mockObjectsBatcher);
    when(mockObjectsBatcher.withConsistencyLevel(ConsistencyLevel.ALL)).thenReturn(mockObjectsBatcher);

    Result<ObjectGetResponse[]> mockResponse = Mockito.mock(Result.class);
    when(mockObjectsBatcher.run()).thenReturn(mockResponse);

    ObjectGetResponse doc1Response = new ObjectGetResponse();
    doc1Response.setId(UUID.nameUUIDFromBytes("doc1".getBytes()).toString());
    doc1Response.setResult(new ObjectsGetResponseAO2Result());

    ObjectGetResponse doc2Response = new ObjectGetResponse();
    doc2Response.setId(UUID.nameUUIDFromBytes("doc2".getBytes()).toString());
    doc2Response.setResult(new ObjectsGetResponseAO2Result());

    ObjectGetResponse doc3Response = new ObjectGetResponse();
    doc3Response.setId(UUID.nameUUIDFromBytes("doc3".getBytes()).toString());
    doc3Response.setResult(new ObjectsGetResponseAO2Result());

    ObjectGetResponse failedDoc1Response = new ObjectGetResponse();
    failedDoc1Response.setId(UUID.nameUUIDFromBytes("failedDoc1".getBytes()).toString());
    failedDoc1Response.setResult(new ObjectsGetResponseAO2Result());
    failedDoc1Response.getResult().setErrors(new ErrorResponse("fake error 1"));

    ObjectGetResponse failedDoc2Response = new ObjectGetResponse();
    failedDoc2Response.setId(UUID.nameUUIDFromBytes("failedDoc2".getBytes()).toString());
    failedDoc2Response.setResult(new ObjectsGetResponseAO2Result());
    failedDoc2Response.getResult().setErrors(new ErrorResponse("fake error 2"));

    when(mockResponse.getResult()).thenReturn(new ObjectGetResponse[]{
        doc1Response, doc2Response, doc3Response, failedDoc1Response, failedDoc2Response
    });
  }

  @Test
  public void testWeaviateIndexer() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("WeaviateIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");

    WeaviateIndexer indexer = new WeaviateIndexer(config, messenger, mockClient, "testing");
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
  public void testWeaviateIndexerException() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("WeaviateIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");
    Document doc3 = Document.create("doc3", "test_run");
    Document doc4 = Document.create("doc4", "test_run");
    Document doc5 = Document.create("doc5", "test_run");

    WeaviateIndexer indexer = new CorruptedWeaviateIndexer(config, messenger, mockClient, "testing");
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
  public void testWeaviateSpecificFailures() throws Exception {
    setupWeaviateClientWithSpecificFailures();

    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("WeaviateIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");
    Document doc3 = Document.create("doc3", "test_run");
    Document failedDoc1 = Document.create("failedDoc1", "test_run");
    Document failedDoc2 = Document.create("failedDoc2", "test_run");

    WeaviateIndexer indexer = new WeaviateIndexer(config, messenger, mockClient, "testing");
    Set<Document> failedDocs = indexer.sendToIndex(List.of(doc, doc2, doc3, failedDoc1, failedDoc2));

    assertEquals(2, failedDocs.size());
    assertTrue(failedDocs.contains(failedDoc1));
    assertTrue(failedDocs.contains(failedDoc2));
  }

  @Test
  public void testValidateConnection() {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("WeaviateIndexerTest/config.conf");
    WeaviateIndexer indexer = new WeaviateIndexer(config, messenger, mockClient, "testing");
    Assert.assertTrue(indexer.validateConnection()); // should only work the first time with the mockClient
    Assert.assertFalse(indexer.validateConnection());
    Assert.assertFalse(indexer.validateConnection());
  }

  @Test
  public void testMultipleBatches() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("WeaviateIndexerTest/batching.conf");
    WeaviateIndexer indexer = new WeaviateIndexer(config, messenger, mockClient, "testing");

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

    // the batcher will be run 3 times
    verify(mockObjectsBatcher, times(3)).run();

    ArgumentCaptor<WeaviateObject> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(WeaviateObject.class);
    // five objects will be added to the batcher
    verify(mockObjectsBatcher, times(5)).withObject(bulkRequestArgumentCaptor.capture());

    List<WeaviateObject> bulkRequestValue = bulkRequestArgumentCaptor.getAllValues();
    assertEquals(5, bulkRequestValue.size());

    WeaviateObject object = bulkRequestArgumentCaptor.getValue();
    assertEquals(WeaviateIndexer.generateDocumentUUID(doc5), object.getId());

    Assert.assertEquals(5, messenger.getSentEvents().size());
    List<Event> events = messenger.getSentEvents();
    Assert.assertEquals("doc1", events.get(0).getDocumentId());
    Assert.assertEquals(Event.Type.FINISH, events.get(0).getType());
  }

  private static class CorruptedWeaviateIndexer extends WeaviateIndexer {

    public CorruptedWeaviateIndexer(Config config, IndexerMessenger messenger,
        WeaviateClient client, String metricsPrefix) {
      super(config, messenger, client, "testing");
    }

    @Override
    public Set<Document> sendToIndex(List<Document> docs) throws Exception {
      throw new Exception("Test that errors when sending to indexer are correctly handled");
    }
  }
}